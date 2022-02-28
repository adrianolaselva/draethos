package specs

type Stream struct {
	Stream Base `yaml:"stream,omitempty"`
}

type Base struct {
	Port        string      `yaml:"port"`
	HealthCheck HealthCheck `yaml:"healthCheck"`
	Metrics     Metrics     `yaml:"metrics"`
	Instance    Instance    `yaml:"instance"`
}

type Instance struct {
	Source Source `yaml:"source,omitempty"`
	Target Target `yaml:"target,omitempty"`
	Dlq    Target `yaml:"dlq,omitempty"`
}

type Source struct {
	Type        string      `yaml:"type,omitempty"`
	Codec       string      `yaml:"codec,omitempty"`
	SourceSpecs SourceSpecs `yaml:"specs,omitempty"`
}

type Target struct {
	Type        string      `yaml:"type,omitempty"`
	TargetSpecs TargetSpecs `yaml:"specs,omitempty"`
}

type SourceSpecs struct {
	Topic          string                 `yaml:"topic,omitempty"`
	TimeoutMs      int                    `yaml:"timeoutMs,omitempty"`
	Configurations map[string]interface{} `yaml:"configurations,omitempty"`
}

type TargetSpecs struct {
	Database            string                 `yaml:"database,omitempty"`
	Table               string                 `yaml:"table,omitempty"`
	KeyColumnName       string                 `yaml:"keyColumnName,omitempty"`
	Topic               string                 `yaml:"topic,omitempty"`
	Queue               string                 `yaml:"queue,omitempty"`
	QueueUrl            string                 `yaml:"queueUrl,omitempty"`
	TopicArn            string                 `yaml:"topicArn,omitempty"`
	Bucket              string                 `yaml:"bucket,omitempty"`
	Prefix              string                 `yaml:"prefix,omitempty"`
	Codec               string                 `yaml:"codec,omitempty"`
	BatchSize           int                    `yaml:"batchSize,omitempty"`
	BufferSize          uint64                 `yaml:"bufferSize,omitempty"`
	LineBreak           string                 `yaml:"lineBreak,omitempty"`
	FlushInMilliseconds int                    `yaml:"flushInMilliseconds,omitempty"`
	DelaySeconds        int64                  `yaml:"delaySeconds,omitempty"`
	Configurations      map[string]interface{} `yaml:"configurations,omitempty"`
}

type HealthCheck struct {
	Endpoint string `yaml:"endpoint,omitempty"`
}

type Metrics struct {
	Endpoint string `yaml:"endpoint,omitempty"`
}
