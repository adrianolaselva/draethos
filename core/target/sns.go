package target

import (
	"bytes"
	"container/list"
	"encoding/json"
	"os"
	"sync"
	"time"

	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	SnsDefaultAWSRegion = "us-east-1"
	SnsDefaultProfile   = "default"
)

type snsTarget struct {
	sync.Mutex
	session    *session.Session
	targetSpec specs.Target
	codec      interfaces.CodecInterface
	fileName   string
	queue      *list.List
	bufferLen  uint64
}

func NewSnsTarget(targetSpec specs.Target, codec interfaces.CodecInterface) (interfaces.TargetInterface, error) {
	return &snsTarget{targetSpec: targetSpec, codec: codec, queue: list.New()}, nil
}

func (g *snsTarget) Initialize() error {
	var cred *credentials.Credentials
	var region = SnsDefaultAWSRegion
	var profile = SnsDefaultProfile
	var accessKey = ""
	var secretKey = ""
	var token = ""

	if g.targetSpec.TargetSpecs.TopicArn == "" {
		return errors.Errorf("topicArn not defined")
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

	topic := sns.New(g.session)

	if _, err := topic.ListTopics(&sns.ListTopicsInput{}); err != nil {
		return errors.Errorf("failed to access sns: %s", err.Error())
	}

	if g.targetSpec.TargetSpecs.QueueUrl != "" {
		if _, err := topic.GetTopicAttributes(&sns.GetTopicAttributesInput{
			TopicArn: &g.targetSpec.TargetSpecs.TopicArn,
		}); err != nil {
			return errors.Errorf("topic %s not found: %s", g.targetSpec.TargetSpecs.TopicArn, err.Error())
		}

		return nil
	}

	return nil
}

func (g *snsTarget) Attach(_ string, data map[string]interface{}) error {
	g.Lock()
	defer g.Unlock()

	payload, err := g.codec.Serialize(data)
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

func (g *snsTarget) CanFlush() bool {
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

func (g *snsTarget) Flush() error {
	g.Lock()
	defer g.Unlock()

	start := time.Now()

	topic := sns.New(g.session)

	elementLen := g.queue.Len()
	for i := 0; i <= elementLen; i++ {
		if element := g.queue.Front(); element != nil {
			if content, ok := element.Value.(string); ok {
				if _, err := topic.Publish(&sns.PublishInput{
					TopicArn: &g.targetSpec.TargetSpecs.TopicArn,
					Message:  &content,
				}); err != nil {
					return errors.Errorf("failed to send event, error: %s", err.Error())
				}
			}
			g.queue.Remove(element)
		}
	}

	elapsed := time.Since(start)

	zap.S().Infof("events sent successfully, elapsed time: %s", elapsed)

	return nil
}

func (g *snsTarget) Close() error {
	return nil
}
