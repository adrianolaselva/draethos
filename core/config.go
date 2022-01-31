package core

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"draethos.io.com/pkg/streams/specs"
	"github.com/heptiolabs/healthcheck"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type ConfigBuilder interface {
	readFile() error
	validateExtension() error
	SetFile(filePath string) ConfigBuilder
	EnableLiveness() ConfigBuilder
	EnableMetrics() ConfigBuilder
	Build() (*specs.Stream, error)
}

type configBuilder struct {
	filePath       string
	enableLiveness bool
	enableMetrics  bool
	file           []byte
}

func NewConfigBuilder() ConfigBuilder {
	return &configBuilder{}
}

func (c *configBuilder) Build() (*specs.Stream, error) {
	if err := c.readFile(); err != nil {
		return nil, err
	}

	if err := c.validateExtension(); err != nil {
		return nil, err
	}

	stream, err := c.deserializeYaml()
	if err != nil {
		return nil, err
	}

	if c.enableLiveness {
		initializeHealthCheck(stream.Stream.HealthCheck.Endpoint, stream.Stream.Port)
		zap.S().Debugf("initialize entpoint health-check: [localhost:%s%s]",
			stream.Stream.Port,
			stream.Stream.HealthCheck.Endpoint)
	}

	if c.enableMetrics {
		initializePrometheus(stream.Stream.Metrics.Endpoint, stream.Stream.Port)
		zap.S().Debugf("initialize endpoint prometheus metrics: [localhost:%s%s]",
			stream.Stream.Port,
			stream.Stream.Metrics.Endpoint)
	}

	return stream, nil
}

func (c *configBuilder) EnableLiveness() ConfigBuilder {
	c.enableLiveness = true
	return c
}

func (c *configBuilder) EnableMetrics() ConfigBuilder {
	c.enableMetrics = true
	return c
}

func (c *configBuilder) SetFile(filePath string) ConfigBuilder {
	c.filePath = filePath
	return c
}

func (c *configBuilder) validateExtension() error {
	if strings.LastIndex(c.filePath, ".yml") > 0 {
		return nil
	}

	if strings.LastIndex(c.filePath, ".yaml") > 0 {
		return nil
	}

	return errors.New(fmt.Sprintf("%s file is invalid, I only accept files with extension yml or yaml", c.filePath))
}

func (c *configBuilder) readFile() error {
	file, err := ioutil.ReadFile(c.filePath)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to load %s file, make sure the path was passed correctly", c.filePath))
	}

	c.file = file
	return nil
}

func (c *configBuilder) deserializeYaml() (*specs.Stream, error) {
	var stream specs.Stream
	if err := yaml.Unmarshal(c.file, &stream); err != nil {
		return nil, errors.New(fmt.Sprintf("failed to deserialize %s file, make sure the path was passed correctly", c.filePath))
	}

	return &stream, nil
}

func initializeHealthCheck(checkEndpoint string, port string) {
	var health = healthcheck.
		NewHandler()

	health.AddLivenessCheck("goroutine-threshold", healthcheck.HTTPGetCheck("", 100))
	health.AddLivenessCheck("goroutine-threshold", healthcheck.GoroutineCountCheck(100))
	health.AddReadinessCheck("health-name", func() error {
		return nil
	})

	mux := http.NewServeMux()
	mux.HandleFunc(checkEndpoint, health.LiveEndpoint)
	mux.HandleFunc("health2", health.ReadyEndpoint)

	go http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", port), mux)
}

func initializePrometheus(endpoint string, port string) {
	http.Handle(endpoint, promhttp.Handler())
	go http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", port), nil)
}
