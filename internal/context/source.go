package context

import (
	interfaces2 "draethos.io.com/internal/interfaces"
	source2 "draethos.io.com/internal/source"
	"errors"
	"fmt"

	"draethos.io.com/pkg/streams/specs"
	"github.com/gorilla/mux"
)

const (
	KafkaSource = "kafka"
	HttpSource  = "http"
	CsvSource   = "csv"
	JsonLSource = "jsonl"
)

func NewSourceContext(stream specs.Stream,
	target interfaces2.TargetInterface,
	dlq interfaces2.TargetInterface,
	router *mux.Router,
	port string) (interfaces2.SourceInterface, error) {
	switch stream.Stream.Instance.Source.Type {
	case KafkaSource:
		return source2.NewKafkaSource(stream.Stream.Instance.Source,
			target,
			dlq,
			NewCodecContext(stream.Stream.Instance.Source.Codec))
	case HttpSource:
		return source2.NewHttpSource(stream.Stream.Instance.Source,
			target,
			dlq,
			NewCodecContext(stream.Stream.Instance.Source.Codec),
			router,
			port)
	case CsvSource:
		return source2.NewCsvSource(stream.Stream.Instance.Source,
			target,
			dlq,
			NewCodecContext(stream.Stream.Instance.Source.Codec))
	case JsonLSource:
		return source2.NewJsonLSource(stream.Stream.Instance.Source,
			target,
			dlq,
			NewCodecContext(stream.Stream.Instance.Source.Codec))
	default:
		return nil, errors.New(fmt.Sprintf("source %s is invalid", stream.Stream.Instance.Target.Type))
	}
}
