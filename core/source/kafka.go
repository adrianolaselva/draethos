package source

import (
	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type kafkaSource struct {
	sourceSpec specs.Source
	target     interfaces.TargetInterface
	dlq        interfaces.TargetInterface
	codec      interfaces.CodecInterface
	configMap  kafka.ConfigMap
}

func NewKafkaSource(sourceSpec specs.Source,
	target interfaces.TargetInterface,
	dlq interfaces.TargetInterface,
	codec interfaces.CodecInterface) (interfaces.SourceInterface, error) {
	return kafkaSource{sourceSpec: sourceSpec, target: target, dlq: dlq, codec: codec, configMap: kafka.ConfigMap{
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"enable.auto.commit":              false,
		"session.timeout.ms":              6000,
	}}, nil
}

func (k kafkaSource) Worker() error {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	if err := k.target.Initialize(); err != nil {
		return err
	}

	if k.dlq != nil {
		if err := k.dlq.Initialize(); err != nil {
			return err
		}
	}

	for key, value := range k.sourceSpec.SourceSpecs.Configurations {
		zap.S().Debugf("source set configuration [key: %v, value: %v]", key, value)
		_ = k.configMap.SetKey(key, value)
	}

	consumer, err := kafka.NewConsumer(&k.configMap)
	if err != nil {
		return err
	}

	topics := append(strings.Split(k.sourceSpec.SourceSpecs.Topic, ","), "^aRegex.*[Tt]opic")
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}

	defer consumer.Close()

	zap.S().Infof("topic successfully subscribed [%s], waiting messages", topics)

	run := true
	for run {
		select {
		case sig := <-sigChan:
			run = false
			zap.S().Infof("caught signal %v: terminating", sig)
			if err = k.target.Flush(); err != nil {
				return errors.Errorf("failed to flush messages [error: %v]", err.Error())
			}

			if _, err = consumer.Commit(); err == nil {
				zap.S().Infof("events successfully committed")
			}
		default:
			ev := consumer.Poll(k.sourceSpec.SourceSpecs.TimeoutMs)
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				zap.S().Debugf("assigned partitions [%v]", e.Partitions)
				_ = consumer.Assign(e.Partitions)
			case kafka.RevokedPartitions:
				zap.S().Debugf("revoked partitions [%v]", e.Partitions)
				_ = consumer.Unassign()
			case *kafka.Message:
				err = k.handleEvent(e)
				if err != nil {
					zap.S().Debugf(err.Error())
					continue
				}

				if !k.target.CanFlush() {
					continue
				}

				if err = k.target.Flush(); err != nil {
					return errors.Errorf("failed to flush messages [error: %v]", err.Error())
				}

				if _, err = consumer.Commit(); err == nil {
					zap.S().Infof("events successfully committed")
				}
			case kafka.PartitionEOF:
				if err = k.target.Flush(); err != nil {
					return errors.Errorf("failed to flush messages [error: %v]", err.Error())
				}

				if _, err = consumer.Commit(); err == nil {
					zap.S().Infof("events successfully committed")
				}
			case kafka.Error:
				zap.S().Debugf(e.Error())
			}
		}
	}

	return nil
}

func (k *kafkaSource) handleEvent(msg *kafka.Message) error {
	zap.S().Infof("processing event [key: %s, value %s]", msg.Key, msg.Value)

	payload, err := k.codec.Deserialize(msg.Value)
	if err != nil {
		return err
	}

	return k.target.Attach(string(msg.Key), payload)
}
