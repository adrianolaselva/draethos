package source

import (
	"bufio"
	"crypto/md5"
	interfaces2 "draethos.io.com/internal/interfaces"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"draethos.io.com/pkg/streams/specs"
	"go.uber.org/zap"
)

type jsonLSource struct {
	sync.Mutex
	sourceSpec specs.Source
	target     interfaces2.TargetInterface
	dlq        interfaces2.TargetInterface
	codec      interfaces2.CodecInterface
}

func NewJsonLSource(sourceSpec specs.Source,
	target interfaces2.TargetInterface,
	dlq interfaces2.TargetInterface,
	codec interfaces2.CodecInterface,
) (interfaces2.SourceInterface, error) {
	return &jsonLSource{
		sourceSpec: sourceSpec,
		target:     target,
		dlq:        dlq,
		codec:      codec,
	}, nil
}

func (c *jsonLSource) Worker() error {
	if err := c.target.Initialize(); err != nil {
		return err
	}

	if c.dlq != nil {
		if err := c.dlq.Initialize(); err != nil {
			return err
		}
	}

	path, err := os.Stat(c.sourceSpec.SourceSpecs.Path)
	if err != nil {
		zap.S().Errorf("jsonl file/directory %s not found: %s", c.sourceSpec.SourceSpecs.Path, err.Error())
		return err
	}

	if path.IsDir() {
		return c.processDir(c.sourceSpec.SourceSpecs.Path)
	}

	return c.processFile(c.sourceSpec.SourceSpecs.Path)
}

func (c *jsonLSource) processDir(path string) error {
	c.Lock()
	defer c.Unlock()

	files, err := c.filePathWalkDir(path)
	if err != nil {
		panic(err)
	}

	for _, v := range files {
		if !strings.HasSuffix(v, ".jsonl") {
			zap.S().Warnf("invalid file %s", v)
			continue
		}

		if err = c.processFile(v); err != nil {
			return err
		}
	}

	return nil
}

func (c *jsonLSource) processFile(filename string) error {
	if _, err := os.Stat(filename); err != nil {
		zap.S().Errorf("jsonl file %s not found: %s", filename, err.Error())
		return err
	}

	file, err := os.Open(filename)
	if err != nil {
		zap.S().Errorf("failed to load jsonl file %s: %s", filename, err.Error())
		return err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		var payload = make(map[string]interface{}, 0)
		if err = json.Unmarshal(scanner.Bytes(), &payload); err != nil {
			return err
		}

		key := fmt.Sprintf("%x", md5.Sum(scanner.Bytes()))
		if err = c.target.Attach(key, payload); err != nil {
			zap.S().Errorf("failed to attach content: %s", err.Error())
			return err
		}

		if !c.target.CanFlush() {
			continue
		}

		if err = c.target.Flush(); err != nil {
			return errors.New(fmt.Sprintf("failed to flush event: %s", err.Error()))
		}
	}

	if err = c.target.Flush(); err != nil {
		return errors.New(fmt.Sprintf("failed to flush event: %s", err.Error()))
	}

	return nil
}

func (c *jsonLSource) filePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}
