package context

import (
	"errors"
	"fmt"

	"draethos.io.com/core/interfaces"
	"draethos.io.com/core/target"
	"draethos.io.com/pkg/streams/specs"
)

const (
	KafkaTarget = "kafka"
	S3Target    = "s3"
	SqsTarget   = "sqs"
	SnsTarget   = "sns"
	PgSqlTarget = "pgsql"
	MySqlTarget = "mysql"
)

func NewTargetContext(targetSpec specs.Target) (interfaces.TargetInterface, error) {
	switch targetSpec.Type {
	case KafkaTarget:
		return target.NewKafkaTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case S3Target:
		return target.NewS3Target(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case PgSqlTarget:
		return target.NewPgsqlTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case MySqlTarget:
		return target.NewMysqlTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case SqsTarget:
		return target.NewSqsTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case SnsTarget:
		return target.NewSnsTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	default:
		return nil, errors.New(fmt.Sprintf("target %s is invalid", targetSpec.Type))
	}
}
