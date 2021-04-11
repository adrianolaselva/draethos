package target

import (
	"crypto/md5"
	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"fmt"
	"go.uber.org/zap"
	"os"
	"regexp"
	"strings"
	"time"
)

type gCloudStorageTarget struct {
	targetSpec specs.Target
	codec      interfaces.CodecInterface
	fileName   string
}

func NewGCloudStorageTarget(targetSpec specs.Target, codec interfaces.CodecInterface) (interfaces.TargetInterface, error) {
	return &gCloudStorageTarget{targetSpec: targetSpec, codec: codec}, nil
}

func (g *gCloudStorageTarget) Initialize() error {
	return nil
}

func (g *gCloudStorageTarget) Attach(key string, content map[string]interface{}) error {
	if g.fileName == "" {
		g.fileName = fmt.Sprintf("/tmp/%x", md5.Sum([]byte(time.Now().String())))
	}

	file, err := os.OpenFile(g.fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	zap.S().Debugf("append content to temp file: %v", g.fileName)
	payload, err := g.codec.Serialize(content)
	if err != nil {
		return err
	}

	zap.S().Debugf("append payload: %s", payload)
	if _, err = file.WriteString(fmt.Sprintf("%s\n", string(payload))); err != nil {
		return err
	}

	return nil
}

func (gCloudStorageTarget) CanFlush() bool {
	return true
}

func (g *gCloudStorageTarget) Flush() error {
	if g.fileName == "" {
		return nil
	}

	//defer g.Close()

	zap.S().Debugf("upload file: %s%x.jsonl", g.prefixFormatter(), md5.Sum([]byte(time.Now().String())))

	return nil
}

func (g *gCloudStorageTarget) Close() error {
	g.fileName = ""
	_ = os.Remove(g.fileName)
	return nil
}

func (g *gCloudStorageTarget) prefixFormatter() string {
	var formatterPrefix = regexp.MustCompile(`^\%{\S+\}$`)

	prefixFormatter := g.targetSpec.TargetSpecs.Prefix

	if formatterPrefix.MatchString("%{YEAR}") {
		prefixFormatter = strings.Replace(prefixFormatter, "%{YEAR}", time.Now().Format("2006"), 5)
	}

	if formatterPrefix.MatchString("%{MONTH}") {
		prefixFormatter = strings.Replace(prefixFormatter, "%{MONTH}", time.Now().Format("01"), 5)
	}

	if formatterPrefix.MatchString("%{DAY}") {
		prefixFormatter = strings.Replace(prefixFormatter, "%{DAY}", time.Now().Format("02"), 5)
	}

	if formatterPrefix.MatchString("%{HOUR}") {
		prefixFormatter = strings.Replace(prefixFormatter, "%{HOUR}", time.Now().Format("15"), 5)
	}

	if formatterPrefix.MatchString("%{MINUTE}") {
		prefixFormatter = strings.Replace(prefixFormatter, "%{MINUTE}", time.Now().Format("04"), 5)
	}

	if formatterPrefix.MatchString("%{SECOND}") {
		prefixFormatter = strings.Replace(prefixFormatter, "%{SECOND}", time.Now().Format("05"), 5)
	}

	return prefixFormatter
}
