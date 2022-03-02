package source

import (
	"crypto/md5"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"go.uber.org/zap"
)

type csvSource struct {
	sync.Mutex
	sourceSpec specs.Source
	target     interfaces.TargetInterface
	dlq        interfaces.TargetInterface
	codec      interfaces.CodecInterface
}

func NewCsvSource(sourceSpec specs.Source,
	target interfaces.TargetInterface,
	dlq interfaces.TargetInterface,
	codec interfaces.CodecInterface,
) (interfaces.SourceInterface, error) {
	return &csvSource{
		sourceSpec: sourceSpec,
		target:     target,
		dlq:        dlq,
		codec:      codec,
	}, nil
}

func (c *csvSource) Worker() error {
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
		zap.S().Errorf("csv file/dorectory %s not found: %s", c.sourceSpec.SourceSpecs.Path, err.Error())
		return err
	}

	if path.IsDir() {
		return c.processDir(c.sourceSpec.SourceSpecs.Path)
	}

	return c.processFile(c.sourceSpec.SourceSpecs.Path)
}

func (c *csvSource) processDir(path string) error {
	c.Lock()
	defer c.Unlock()

	files, err := c.filePathWalkDir(path)
	if err != nil {
		panic(err)
	}

	for _, v := range files {
		if !strings.HasSuffix(v, ".csv") {
			zap.S().Warnf("invalid file %s", v)
			continue
		}

		if err := c.processFile(v); err != nil {
			return err
		}
	}

	return nil
}

func (c *csvSource) processFile(filename string) error {
	if _, err := os.Stat(filename); err != nil {
		zap.S().Errorf("csv file %s not found: %s", filename, err.Error())
		return err
	}

	file, err := os.Open(filename)
	if err != nil {
		zap.S().Errorf("failed to load csv file %s: %s", filename, err.Error())
		return err
	}

	defer file.Close()

	reader := csv.NewReader(file)

	lines := 0
	columns := []string{}
	for {
		records, err := reader.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			zap.S().Errorf("failed to read columns csv file %s: %s", filename, err.Error())
			return err
		}

		if records == nil {
			break
		}

		if lines == 0 {
			for _, v := range records {
				columns = append(columns, strings.ToLower(strings.ReplaceAll(v, " ", "_")))
			}

			lines++

			zap.S().Debugf("columns %s", records)

			continue
		}

		payload := make(map[string]interface{}, 0)
		for k, v := range records {
			payload[columns[k]] = v
		}

		// zap.S().Debugf("send event %s", payload)

		key := fmt.Sprintf("'%x'", md5.Sum([]byte(strings.Join(records, ""))))
		if err := c.target.Attach(key, payload); err != nil {
			zap.S().Errorf("failed to attach content: %s", err.Error())
			return err
		}

		if !c.target.CanFlush() {
			continue
		}

		if err := c.target.Flush(); err != nil {
			return errors.New(fmt.Sprintf("failed to flush event: %s", err.Error()))
		}
	}

	if err := c.target.Flush(); err != nil {
		return errors.New(fmt.Sprintf("failed to flush event: %s", err.Error()))
	}

	return nil
}

func (c *csvSource) filePathWalkDir(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})

	return files, err
}
