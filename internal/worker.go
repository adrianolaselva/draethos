package internal

import (
	context2 "draethos.io.com/internal/context"
	"fmt"
	"net/http"
	"time"

	"draethos.io.com/pkg/streams/specs"
	"github.com/gorilla/mux"
	"github.com/heptiolabs/healthcheck"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type Worker interface {
	Start() error
}

type worker struct {
	Err           error
	configBuilder ConfigBuilder
	router        *mux.Router
	configSpec    specs.Stream
}

func NewWorker(configSpec specs.Stream,
	configBuilder ConfigBuilder) Worker {
	return &worker{
		configSpec:    configSpec,
		configBuilder: configBuilder,
		router:        &mux.Router{},
	}
}

func (s *worker) Start() error {
	zap.S().Infof("initializing target: %v", s.configSpec.Stream.Instance.Target.Type)
	target, err := context2.NewTargetContext(s.configSpec.Stream.Instance.Target)
	if err != nil {
		return err
	}

	zap.S().Infof("initializing dlq context: %v", s.configSpec.Stream.Instance.Dlq.Type)
	dlq, err := context2.NewTargetContext(s.configSpec.Stream.Instance.Dlq)
	if err != nil {
		zap.S().Infof("dlq not defined: %v", err.Error())
	}

	zap.S().Infof("initializing source context: %v", s.configSpec.Stream.Instance.Source.Type)
	source, err := context2.NewSourceContext(s.configSpec, target, dlq, s.router, s.configSpec.Stream.Port)
	if err != nil {
		return err
	}

	s.Setup()

	zap.S().Debug("initializing worker")

	return source.Worker()
}

func (s *worker) Setup() {
	if s.configBuilder.GetHttpPort() != "0" {
		s.configSpec.Stream.Port = s.configBuilder.GetHttpPort()
	}

	if s.configBuilder.IsEnabledLiveness() {
		s.initializeHealthCheck(s.configSpec.Stream.HealthCheck.Endpoint)
		zap.S().Debugf("initialize endpoint liveness: http://localhost:%s%s",
			s.configSpec.Stream.Port,
			s.configSpec.Stream.HealthCheck.Endpoint)
	}

	if s.configBuilder.IsEnabledMetrics() {
		s.initializePrometheus(s.configSpec.Stream.Metrics.Endpoint)
		zap.S().Debugf("initialize endpoint prometheus: http://localhost:%s%s",
			s.configSpec.Stream.Port,
			s.configSpec.Stream.Metrics.Endpoint)
	}

	if s.configSpec.Stream.Instance.Source.Type != context2.HttpSource {
		srv := &http.Server{
			Handler:      s.router,
			Addr:         fmt.Sprintf("0.0.0.0:%s", s.configSpec.Stream.Port),
			WriteTimeout: 15 * time.Second,
			ReadTimeout:  15 * time.Second,
		}

		go srv.ListenAndServe()
	}
}

func (s *worker) initializeHealthCheck(checkEndpoint string) {
	var health = healthcheck.
		NewHandler()

	health.AddLivenessCheck("goroutine-threshold", healthcheck.HTTPGetCheck("", 100))
	health.AddLivenessCheck("goroutine-threshold", healthcheck.GoroutineCountCheck(100))
	health.AddReadinessCheck("health-name", func() error {
		return nil
	})

	s.router.HandleFunc(checkEndpoint, health.LiveEndpoint)
}

func (s *worker) initializePrometheus(endpoint string) {
	s.router.Handle(endpoint, promhttp.Handler())
}
