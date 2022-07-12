package target

import (
	"container/list"
	"draethos.io.com/internal/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.uber.org/zap"
	"sync"
)

type kafkaTarget struct {
	sync.Mutex
	targetSpec specs.Target
	codec      interfaces.CodecInterface
	configMap  kafka.ConfigMap
	producer   *kafka.Producer
	queue      *list.List
}

func NewKafkaTarget(targetSpec specs.Target, codec interfaces.CodecInterface) (*kafkaTarget, error) {
	return &kafkaTarget{targetSpec: targetSpec, codec: codec, configMap: kafka.ConfigMap{
		"message.send.max.retries": 10000000,
		"enable.idempotence":       true,
	}, queue: list.New()}, nil
}

func (k *kafkaTarget) Initialize() error {
	for key, value := range k.targetSpec.TargetSpecs.Configurations {
		zap.S().Debugf("target set configuration [key: %v, value: %v]", key, value)
		_ = k.configMap.SetKey(key, value)
	}

	producer, err := kafka.NewProducer(&k.configMap)
	if err != nil {
		return err
	}

	k.producer = producer
	return nil
}

func (k *kafkaTarget) Attach(_ string, data map[string]interface{}) error {
	k.Lock()
	defer k.Unlock()

	k.queue.PushBack(data)
	return nil
}

func (k *kafkaTarget) CanFlush() bool {
	return k.queue.Len() >= k.targetSpec.TargetSpecs.BatchSize
}

func (k *kafkaTarget) Flush() error {
	k.Lock()
	defer k.Unlock()

	if k.queue.Len() == 0 {
		return nil
	}

	zap.S().Debugf(fmt.Sprintf("flush %v events", k.queue.Len()))

	for k.queue.Len() > 0 {
		e := k.queue.Front()

		value, ok := e.Value.(map[string]interface{})
		if !ok {
			k.queue.Remove(e)
			zap.S().Warnf("failed to deserialize event [%x], waiting messages", e.Value)
			return nil
		}

		content, err := k.codec.Serialize(value)
		if err != nil {
			return err
		}

		err = k.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &k.targetSpec.TargetSpecs.Topic,
				Partition: kafka.PartitionAny,
			},
			Value: content,
		}, nil)

		if err != nil {
			return err
		}

		k.queue.Remove(e)
		k.producer.Flush(-1)
	}

	return nil
}

func (k *kafkaTarget) Close() error {
	k.producer.Close()
	return nil
}
