package context

import (
	"draethos.io.com/internal/interfaces"
	target2 "draethos.io.com/internal/target"
	"errors"
	"fmt"

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
		return target2.NewKafkaTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case S3Target:
		return target2.NewS3Target(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case PgSqlTarget:
		return target2.NewPgsqlTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case MySqlTarget:
		return target2.NewMysqlTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case SqsTarget:
		return target2.NewSqsTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	case SnsTarget:
		return target2.NewSnsTarget(targetSpec, NewCodecContext(targetSpec.TargetSpecs.Codec))
	default:
		return nil, errors.New(fmt.Sprintf("target %s is invalid", targetSpec.Type))
	}
}
