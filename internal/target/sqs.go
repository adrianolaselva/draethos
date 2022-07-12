package target

import (
	"bytes"
	"container/list"
	interfaces2 "draethos.io.com/internal/interfaces"
	"encoding/json"
	"os"
	"sync"
	"time"

	"draethos.io.com/pkg/streams/specs"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	SqsDefaultAWSRegion      = "us-east-1"
	SqsDefaultProfile        = "default"
	SqsbufferSizeDefault int = 1048576
	SqsLineBreakDefault      = "\n"
)

type sqsTarget struct {
	sync.Mutex
	session    *session.Session
	targetSpec specs.Target
	codec      interfaces2.CodecInterface
	fileName   string
	queue      *list.List
	bufferLen  uint64
}

func NewSqsTarget(targetSpec specs.Target, codec interfaces2.CodecInterface) (interfaces2.TargetInterface, error) {
	return &sqsTarget{targetSpec: targetSpec, codec: codec, queue: list.New()}, nil
}

func (g *sqsTarget) Initialize() error {
	var cred *credentials.Credentials
	var region = SqsDefaultAWSRegion
	var profile = SqsDefaultProfile
	var accessKey = ""
	var secretKey = ""
	var token = ""

	if g.targetSpec.TargetSpecs.QueueUrl == "" {
		return errors.Errorf("queueUrl not defined")
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

	queue := sqs.New(g.session)

	maxResults := int64(1)
	if _, err := queue.ListQueues(&sqs.ListQueuesInput{
		MaxResults: &maxResults,
	}); err != nil {
		return errors.Errorf("failed to access sqs: %s", err.Error())
	}

	if g.targetSpec.TargetSpecs.QueueUrl != "" {
		if _, err := queue.GetQueueAttributes(&sqs.GetQueueAttributesInput{
			QueueUrl: &g.targetSpec.TargetSpecs.QueueUrl,
		}); err != nil {
			return errors.Errorf("queue %s not found: %s", g.targetSpec.TargetSpecs.Queue, err.Error())
		}

		return nil
	}

	if g.targetSpec.TargetSpecs.Queue != "" {
		queueOutPut, err := queue.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: &g.targetSpec.TargetSpecs.Queue,
		})

		if err != nil {
			return errors.Errorf("queue %s not found: %s", g.targetSpec.TargetSpecs.Queue, err.Error())
		}

		g.targetSpec.TargetSpecs.QueueUrl = *queueOutPut.QueueUrl
	}

	return nil
}

func (g *sqsTarget) Attach(_ string, data map[string]interface{}) error {
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

func (g *sqsTarget) CanFlush() bool {
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

func (g *sqsTarget) Flush() error {
	g.Lock()
	defer g.Unlock()

	start := time.Now()

	queue := sqs.New(g.session)

	entries := make([]*sqs.SendMessageBatchRequestEntry, 0)
	elementLen := g.queue.Len()
	for i := 0; i <= elementLen; i++ {
		if element := g.queue.Front(); element != nil {
			if content, ok := element.Value.(string); ok {
				entries = append(entries, &sqs.SendMessageBatchRequestEntry{
					DelaySeconds: &g.targetSpec.TargetSpecs.DelaySeconds,
					MessageBody:  &content,
				})
			}
			g.queue.Remove(element)
		}
	}

	elapsed := time.Since(start)
	if _, err := queue.SendMessageBatch(&sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: &g.targetSpec.TargetSpecs.QueueUrl,
	}); err != nil {
		return errors.Errorf("failed to send events, elapsed time: %s, error: %s", elapsed, err.Error())
	}

	zap.S().Infof("events sent successfully, elapsed time: %s", elapsed)

	return nil
}

func (g *sqsTarget) Close() error {
	return nil
}
