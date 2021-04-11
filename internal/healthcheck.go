package internal

type HealthCheck interface {
	Start() error
}

type healthCheck struct {
}

func NewHealthCheck() HealthCheck {
	return &healthCheck{}
}

func (healthCheck) Start() error {
	return nil
}
