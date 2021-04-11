package context

import (
	"draethos.io.com/core/interfaces"
	"draethos.io.com/core/source"
	"draethos.io.com/pkg/streams/specs"
	"errors"
	"fmt"
)

const (
	KafkaSource = "kafka"
)

func NewSourceContext(stream specs.Stream, target interfaces.TargetInterface, dlq interfaces.TargetInterface) (interfaces.SourceInterface, error) {
	switch stream.Stream.Instance.Source.Type {
	case KafkaSource:
		return source.NewKafkaSource(stream.Stream.Instance.Source, target, dlq, NewCodecContext(stream.Stream.Instance.Source.Codec))
	default:
		return nil, errors.New(fmt.Sprintf("source %s is invalid", stream.Stream.Instance.Target.Type))
	}
}
