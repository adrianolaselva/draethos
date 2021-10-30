package streams

const (
	KafkaTarget = "kafka"
	S3Target    = "s3"
)

type Target interface {
	Attach(key string, data map[string]interface{}) error
	Flush() error
}

func GetTargetContext(ctx string) Source {
	switch ctx {
	case KafkaTarget:
		return nil
	case S3Target:
		return nil
	default:
		return nil
	}
}
