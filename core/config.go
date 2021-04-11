package core

import (
	"draethos.io.com/pkg/streams/specs"
	"errors"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"strings"
)

type ConfigBuilder interface {
	readFile() error
	validateExtension() error
	SetFile(filePath string) ConfigBuilder
	Build() (*specs.Stream, error)
}

type configBuilder struct {
	filePath string
	file     []byte
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
