package streams

const (
	KafkaSource = "kafka"
)

type Source interface {
	Process() error
}

func GetSourceContext(ctx string) Source {
	switch ctx {
	case KafkaSource:
		return nil
	default:
		return nil
	}
}
