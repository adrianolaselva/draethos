package context

import (
	"errors"
	"fmt"

	"draethos.io.com/core/interfaces"
	"draethos.io.com/core/source"
	"draethos.io.com/pkg/streams/specs"
	"github.com/gorilla/mux"
)

const (
	KafkaSource = "kafka"
	HttpSource  = "http"
)

func NewSourceContext(stream specs.Stream,
	target interfaces.TargetInterface,
	dlq interfaces.TargetInterface,
	router *mux.Router,
	port string) (interfaces.SourceInterface, error) {
	switch stream.Stream.Instance.Source.Type {
	case KafkaSource:
		return source.NewKafkaSource(stream.Stream.Instance.Source,
			target,
			dlq,
			NewCodecContext(stream.Stream.Instance.Source.Codec))
	case HttpSource:
		return source.NewHttpSource(stream.Stream.Instance.Source,
			target,
			dlq,
			NewCodecContext(stream.Stream.Instance.Source.Codec),
			router,
			port)
	default:
		return nil, errors.New(fmt.Sprintf("source %s is invalid", stream.Stream.Instance.Target.Type))
	}
}
