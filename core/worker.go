package core

import (
	"draethos.io.com/core/context"
	"draethos.io.com/pkg/streams/specs"
	"go.uber.org/zap"
)

type Worker interface {
	Start() error
}

type worker struct {
	Err        error
	configSpec specs.Stream
}

func NewWorker(configSpec specs.Stream) Worker {
	return &worker{configSpec: configSpec}
}

func (s *worker) Start() error {
	zap.S().Infof("initializing target: %v", s.configSpec.Stream.Instance.Target.Type)
	target, err := context.NewTargetContext(s.configSpec.Stream.Instance.Target)
	if err != nil {
		return err
	}

	zap.S().Infof("initializing dlq context: %v", s.configSpec.Stream.Instance.Dlq.Type)
	dlq, err := context.NewTargetContext(s.configSpec.Stream.Instance.Dlq)
	if err != nil {
		return err
	}

	zap.S().Infof("initializing source context: %v", s.configSpec.Stream.Instance.Source.Type)
	source, err := context.NewSourceContext(s.configSpec, target, dlq)
	if err != nil {
		return err
	}

	zap.S().Debug("initializing worker")
	return source.Worker()
}
