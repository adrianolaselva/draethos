package core

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"draethos.io.com/pkg/streams/specs"
	"gopkg.in/yaml.v2"
)

type ConfigBuilder interface {
	readFile() error
	validateExtension() error
	SetPort(port string) ConfigBuilder
	SetFile(filePath string) ConfigBuilder
	IsEnabledLiveness() bool
	IsEnabledMetrics() bool
	EnableLiveness() ConfigBuilder
	EnableMetrics() ConfigBuilder
	GetHttpPort() string
	Build() (*specs.Stream, error)
}

type configBuilder struct {
	filePath       string
	enableLiveness bool
	enableMetrics  bool
	httpPort       string
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

	return stream, nil
}

func (c *configBuilder) GetHttpPort() string {
	return c.httpPort
}

func (c *configBuilder) IsEnabledLiveness() bool {
	return c.enableLiveness
}

func (c *configBuilder) IsEnabledMetrics() bool {
	return c.enableMetrics
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
