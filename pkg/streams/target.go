package streams

const (
	KafkaTarget         = "kafka"
	GCloudStorageTarget = "gcloudstorage"
)

type Target interface {
	Attach(key string, data map[string]interface{}) error
	Flush() error
}

func GetTargetContext(ctx string) Source {
	switch ctx {
	case KafkaTarget:
		return nil
	case GCloudStorageTarget:
		return nil
	default:
		return nil
	}
}
