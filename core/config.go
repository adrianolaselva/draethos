package core

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"draethos.io.com/pkg/streams/specs"
	"github.com/gorilla/mux"
	"github.com/heptiolabs/healthcheck"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

type ConfigBuilder interface {
	readFile() error
	validateExtension() error
	SetPort(port string) ConfigBuilder
	SetFile(filePath string) ConfigBuilder
	EnableLiveness() ConfigBuilder
	EnableMetrics() ConfigBuilder
	Build() (*specs.Stream, error)

	initializeHealthCheck(checkEndpoint string)
	initializePrometheus(endpoint string)
}

type configBuilder struct {
	filePath       string
	enableLiveness bool
	enableMetrics  bool
	httpPort       string
	router         *mux.Router
	file           []byte
}

func NewConfigBuilder() ConfigBuilder {
	return &configBuilder{
		router: &mux.Router{}}
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

	if c.httpPort != "0" {
		stream.Stream.Port = c.httpPort
	}

	if c.enableLiveness {
		c.initializeHealthCheck(stream.Stream.HealthCheck.Endpoint)
		zap.S().Debugf("initialize endpoint liveness: http://localhost:%s%s",
			stream.Stream.Port,
			stream.Stream.HealthCheck.Endpoint)
	}

	if c.enableMetrics {
		c.initializePrometheus(stream.Stream.Metrics.Endpoint)
		zap.S().Debugf("initialize endpoint prometheus: http://localhost:%s%s",
			stream.Stream.Port,
			stream.Stream.Metrics.Endpoint)
	}

	if c.enableMetrics || c.enableLiveness {
		srv := &http.Server{
			Handler:      c.router,
			Addr:         fmt.Sprintf("0.0.0.0:%s", stream.Stream.Port),
			WriteTimeout: 15 * time.Second,
			ReadTimeout:  15 * time.Second,
		}

		go srv.ListenAndServe()
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

func (c *configBuilder) SetPort(port string) ConfigBuilder {
	c.httpPort = port
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

func (c *configBuilder) initializeHealthCheck(checkEndpoint string) {
	var health = healthcheck.
		NewHandler()

	health.AddLivenessCheck("goroutine-threshold", healthcheck.HTTPGetCheck("", 100))
	health.AddLivenessCheck("goroutine-threshold", healthcheck.GoroutineCountCheck(100))
	health.AddReadinessCheck("health-name", func() error {
		return nil
	})

	c.router.HandleFunc(checkEndpoint, health.LiveEndpoint)
}

func (c *configBuilder) initializePrometheus(endpoint string) {
	c.router.Handle(endpoint, promhttp.Handler())
}
