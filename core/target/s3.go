package target

import (
	"bytes"
	"container/list"
	"context"
	"crypto/md5"
	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultAWSRegion      = "us-east-1"
	DefaultProfile        = "default"
	bufferSizeDefault int = 1048576
	LineBreakDefault 	  = "\n"
)

type s3Target struct {
	sync.Mutex
	session    *session.Session
	targetSpec specs.Target
	codec      interfaces.CodecInterface
	fileName   string
	queue      *list.List
	bufferLen  uint64
}

func NewS3Target(targetSpec specs.Target, codec interfaces.CodecInterface) (interfaces.TargetInterface, error) {
	return &s3Target{targetSpec: targetSpec, codec: codec, queue: list.New()}, nil
}

func (g *s3Target) Initialize() error {
	var cred *credentials.Credentials
	var region = DefaultAWSRegion
	var profile = DefaultProfile
	var accessKey = ""
	var secretKey = ""
	var token = ""

	if g.targetSpec.TargetSpecs.Bucket == "" {
		return errors.Errorf("bucket not defined")
	}

	if g.targetSpec.TargetSpecs.LineBreak == "" {
		g.targetSpec.TargetSpecs.LineBreak = LineBreakDefault
	}

	if value, ok := g.targetSpec.TargetSpecs.Configurations["aws.region"].(string); ok {
		region = value
	}

	if value, ok := g.targetSpec.TargetSpecs.Configurations["aws.profile"].(string); ok {
		profile = value
	}

	if value, ok := g.targetSpec.TargetSpecs.Configurations["aws.access.key"].(string); ok {
		accessKey = value
	}

	if value, ok := g.targetSpec.TargetSpecs.Configurations["aws.secret.key"].(string); ok {
		secretKey = value
	}

	if value, ok := g.targetSpec.TargetSpecs.Configurations["aws.credential.file"].(string); ok {
		_, err := os.Stat(value)
		if err != nil {
			zap.S().Debugf("aws_credentials_file %s not found", value)
		}

		if err == nil {
			cred = credentials.NewSharedCredentials(value, profile)
		}
	}

	if accessKey != "" && secretKey != "" {
		cred = credentials.NewStaticCredentials(accessKey, secretKey, token)
	}

	sess := session.Must(session.NewSession(
		&aws.Config{
			Region:      aws.String(region),
			Credentials: cred,
		}),
	)

	g.session = sess

	uploader := s3manager.NewUploader(g.session)

	if _, err := uploader.S3.ListBuckets(&s3.ListBucketsInput{}); err != nil {
		return errors.Errorf("failed to access s3: %s", err.Error())
	}

	if _, err := s3manager.GetBucketRegion(context.TODO(), sess, g.targetSpec.TargetSpecs.Bucket, region); err != nil {
		return errors.Errorf("bucket %s not found: %s", g.targetSpec.TargetSpecs.Bucket, err.Error())
	}

	return nil
}

func (g *s3Target) Attach(key string, content map[string]interface{}) error {
	g.Lock()
	defer g.Unlock()

	payload, err := g.codec.Serialize(content)
	if err != nil {
		return errors.Errorf("failed to serialize payload: %s", err.Error())
	}

	buffer := new(bytes.Buffer)
	if err := json.Compact(buffer, payload); err != nil {
		zap.S().Warnf("failed to compact json: %s", err.Error())
	}

	payload = buffer.Bytes()
	g.bufferLen += uint64(buffer.Len())
	g.bufferLen += uint64(len([]byte(g.targetSpec.TargetSpecs.LineBreak)))
	g.queue.PushBack(payload)

	zap.S().Debugf("buffer length: %s", lenReadable(g.bufferLen, 2))

	return nil
}

func (g *s3Target) CanFlush() bool {
	g.Lock()
	defer g.Unlock()

	if g.targetSpec.TargetSpecs.BufferSize > 0 {
		return g.bufferLen >= g.targetSpec.TargetSpecs.BufferSize
	}

	if g.targetSpec.TargetSpecs.BatchSize > 0 {
		return g.queue.Len() >= g.targetSpec.TargetSpecs.BatchSize
	}

	return true
}

func (g *s3Target) Flush() error {
	g.Lock()
	defer g.Unlock()

	var fileName = fmt.Sprintf("%s%x.jsonl", g.prefixFormatter(), md5.Sum([]byte(time.Now().String())))
	uploader := s3manager.NewUploader(g.session)

	if g.bufferLen == 0 {
		return nil
	}

	var bufferRx strings.Builder
	elementLen := g.queue.Len()
	for i := 0; i <= elementLen; i++ {
		if element := g.queue.Front(); element != nil {
			if content, ok := element.Value.([]byte); ok {
				bufferRx.WriteString(fmt.Sprintf("%s%s", content, g.targetSpec.TargetSpecs.LineBreak))
			}
			g.queue.Remove(element)
		}
	}

	zap.S().Debugf("upload file: %s.jsonl, bytes length: %s", fileName, lenReadable(g.bufferLen, 2))

	g.bufferLen = 0

	start := time.Now()
	result, err := uploader.Upload(&s3manager.UploadInput{
		Bucket:          &g.targetSpec.TargetSpecs.Bucket,
		Key:             &fileName,
		Body:            strings.NewReader(bufferRx.String()),
		ContentEncoding: aws.String("application/json"),
	})

	elapsed := time.Since(start)
	if err != nil {
		return errors.Errorf("failed to upload file %s, elapsed time: %s, error: %s", fileName, elapsed, err.Error())
	}

	zap.S().Infof("uploaded file %s, elapsed time: %s", result.Location, elapsed)

	return nil
}

func (g *s3Target) Close() error {
	return nil
}

func (g *s3Target) prefixFormatter() string {
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

const (
	TB = 1000000000000
	GB = 1000000000
	MB = 1000000
	KB = 1000
)

func lenReadable(length uint64, decimals int) (out string) {
	var unit string
	var i int
	var remainder int

	if length > TB {
		unit = "TB"
		i = int(length) / TB
		remainder = int(length) - (i * TB)
	} else if length > GB {
		unit = "GB"
		i = int(length) / GB
		remainder = int(length) - (i * GB)
	} else if length > MB {
		unit = "MB"
		i = int(length) / MB
		remainder = int(length) - (i * MB)
	} else if length > KB {
		unit = "KB"
		i = int(length) / KB
		remainder = int(length) - (i * KB)
	} else {
		return strconv.Itoa(int(length)) + " B"
	}

	if decimals == 0 {
		return strconv.Itoa(i) + " " + unit
	}

	width := 0
	if remainder > GB {
		width = 12
	} else if remainder > MB {
		width = 9
	} else if remainder > KB {
		width = 6
	} else {
		width = 3
	}

	remainderString := strconv.Itoa(remainder)
	for iter := len(remainderString); iter < width; iter++ {
		remainderString = "0" + remainderString
	}
	if decimals > len(remainderString) {
		decimals = len(remainderString)
	}

	return fmt.Sprintf("%d.%s %s", i, remainderString[:decimals], unit)
}

