package context

import (
	"draethos.io.com/core/interfaces"
	"draethos.io.com/core/target"
	"draethos.io.com/pkg/streams/specs"
	"errors"
	"fmt"
)

const (
	KafkaTarget         = "kafka"
	GCloudStorageTarget = "gcloudstorage"
)

func NewTargetContext(targetSpec specs.Target) (interfaces.TargetInterface, error) {
	switch targetSpec.Type {
	case KafkaTarget:
		return target.NewKafkaTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case GCloudStorageTarget:
		return target.NewGCloudStorageTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	default:
		return nil, errors.New(fmt.Sprintf("target %s is invalid", targetSpec.Type))
	}
}
