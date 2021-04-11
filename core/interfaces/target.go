package interfaces

type TargetInterface interface {
	Initialize() error
	Attach(key string, data map[string]interface{}) error
	Flush() error
	CanFlush() bool
	Close() error
}
