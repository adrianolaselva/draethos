package specs

type Stream struct {
	Stream Base `yaml:"stream"`
}

type Base struct {
	Port        string      `yaml:"port"`
	HealthCheck HealthCheck `yaml:"healthCheck"`
	Metrics     Metrics     `yaml:"metrics"`
	Instance    Instance    `yaml:"instance"`
}

type Instance struct {
	Source Source `yaml:"source"`
	Target Target `yaml:"target"`
	Dlq    Target `yaml:"dlq"`
}

type Source struct {
	Type        string      `yaml:"type"`
	Codec       string      `yaml:"codec"`
	SourceSpecs SourceSpecs `yaml:"specs"`
}

type Target struct {
	Type        string      `yaml:"type"`
	TargetSpecs TargetSpecs `yaml:"specs"`
}

type SourceSpecs struct {
	Topic          string                 `yaml:"topic"`
	TimeoutMs      int                    `yaml:"timeoutMs"`
	Configurations map[string]interface{} `yaml:"configurations"`
}

type TargetSpecs struct {
	Database            string                 `yaml:"database"`
	Table               string                 `yaml:"table"`
	KeyColumnName       string                 `yaml:"keyColumnName"`
	Topic               string                 `yaml:"topic"`
	Bucket              string                 `yaml:"bucket"`
	Prefix              string                 `yaml:"prefix"`
	Codec               string                 `yaml:"codec"`
	BatchSize           int                    `yaml:"batchSize"`
	BufferSize          uint64                 `yaml:"bufferSize"`
	LineBreak           string                 `yaml:"lineBreak"`
	FlushInMilliseconds int                    `yaml:"flushInMilliseconds"`
	Configurations      map[string]interface{} `yaml:"configurations"`
}

type HealthCheck struct {
	Endpoint string `yaml:"endpoint"`
}

type Metrics struct {
	Endpoint string `yaml:"endpoint"`
}
