package scaffold

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/color"
	"draethos.io.com/pkg/streams/specs"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

type scaffoldCommand struct {
}

func NewScaffoldCommand() interfaces.BuildCommand {
	return scaffoldCommand{}
}

func (s scaffoldCommand) Build() *cobra.Command {
	var scaffoldCommand = &cobra.Command{
		Use:   "generate",
		Short: "Generage scaffold",
		Long:  `Generate scaffolding from parameters`,
		Example: `./draethos generate \
	--port 8000 \
	--export-path ./share/pipeline.yaml \
	--instance.source.type kafka \
	--instance.source.specs.topic topic.source \
	--instance.source.specs.configurations "group.id=draethos" \
	--instance.source.specs.configurations "bootstrap.servers=localhost:9093" \
	--instance.source.specs.configurations "auto.offset.reset=beginning" \
	--instance.target.type topic.target \
	--instance.target.specs.batchSize 100 \
	--instance.target.specs.configurations "bootstrap.servers=localhost:9093"`,
		RunE: s.runE,
	}

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"export-path",
			"",
			"pipeline.yaml",
			"export path")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"port",
			"",
			"",
			"http server port")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.source.type",
			"",
			"",
			"instance source type")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.source.specs.topic",
			"",
			"",
			"instance source topic name")

	scaffoldCommand.
		PersistentFlags().
		IntP(
			"instance.source.specs.timeoutMs",
			"",
			1000,
			"instance source timeout (default: 1000)")

	scaffoldCommand.
		PersistentFlags().
		StringArrayP(
			"instance.source.specs.configurations",
			"",
			nil,
			"instance source configurations")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.type",
			"",
			"",
			"target type")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.database",
			"",
			"",
			"database name")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.table",
			"",
			"",
			"table name")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.bucket",
			"",
			"",
			"bucket name")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.batchSize",
			"",
			"",
			"batch size")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.topic",
			"",
			"",
			"topic name")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.topicArn",
			"",
			"",
			"topic arn")

	scaffoldCommand.
		PersistentFlags().
		IntP(
			"instance.target.specs.bufferSize",
			"",
			1048576,
			"buffer size")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.codec",
			"",
			"",
			"serialize event before send to target (default: json, yaml)")

	scaffoldCommand.
		PersistentFlags().
		IntP(
			"instance.target.specs.delaySeconds",
			"",
			1,
			"flush events interval")

	scaffoldCommand.
		PersistentFlags().
		IntP(
			"instance.target.specs.flushInMilliseconds",
			"",
			10000,
			"milliseconds to flush events")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.keyColumnName",
			"",
			"",
			"unique column name")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.lineBreak",
			"",
			"",
			"line break")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.prefix",
			"",
			"",
			"storage prefix ex: /directory/%{YEAR}/%{MONTH}/%{DAY}/%{HOUR}/")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.queue",
			"",
			"",
			"queue name")

	scaffoldCommand.
		PersistentFlags().
		StringP(
			"instance.target.specs.queueUrl",
			"",
			"",
			"queue url")

	scaffoldCommand.
		PersistentFlags().
		StringArrayP(
			"instance.target.specs.configurations",
			"",
			nil,
			"instance target configurations")

	return scaffoldCommand
}

func (s scaffoldCommand) runE(cmd *cobra.Command, args []string) error {
	stream := specs.Stream{
		Stream: specs.Base{
			Port: "9000",
			HealthCheck: specs.HealthCheck{
				Endpoint: "/health",
			},
			Metrics: specs.Metrics{
				Endpoint: "/metrics",
			},
			Instance: specs.Instance{
				Source: specs.Source{
					SourceSpecs: specs.SourceSpecs{
						Configurations: make(map[string]interface{}),
					},
				},
				Target: specs.Target{
					TargetSpecs: specs.TargetSpecs{
						Configurations: make(map[string]interface{}),
					},
				},
				Dlq: specs.Target{
					TargetSpecs: specs.TargetSpecs{
						Configurations: make(map[string]interface{}),
					},
				},
			},
		},
	}

	path := "pipeline.yaml"
	if value, err := cmd.Flags().GetString("export-path"); err == nil {
		path = value
	}

	if value, err := cmd.Flags().GetString("port"); err == nil {
		stream.Stream.Port = value
	}

	if value, err := cmd.Flags().GetString("instance.source.type"); err == nil {
		stream.Stream.Instance.Source.Type = value
	}

	if value, err := cmd.Flags().GetString("instance.source.specs.topic"); err == nil {
		stream.Stream.Instance.Source.SourceSpecs.Topic = value
	}

	if value, err := cmd.Flags().GetInt("instance.source.specs.timeoutMs"); err == nil {
		stream.Stream.Instance.Source.SourceSpecs.TimeoutMs = value
	}

	if values, err := cmd.Flags().GetStringArray("instance.source.specs.configurations"); err == nil {
		for _, v := range values {
			data := strings.Split(v, "=")
			if len(data) < 1 {
				fmt.Println(fmt.Sprintf("%sfailed to set source configuration: %v%s", color.Yellow, data, color.Reset))
				continue
			}

			stream.Stream.Instance.Source.SourceSpecs.Configurations[data[0]] = data[1]
		}
	}

	if value, err := cmd.Flags().GetString("instance.target.type"); err == nil {
		stream.Stream.Instance.Target.Type = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.database"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.Database = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.bucket"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.Bucket = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.table"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.Table = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.topic"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.Topic = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.topicArn"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.TopicArn = value
	}

	if value, err := cmd.Flags().GetUint64("instance.target.specs.bufferSize"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.BufferSize = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.codec"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.Codec = value
	}

	if value, err := cmd.Flags().GetInt64("instance.target.specs.delaySeconds"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.DelaySeconds = value
	}

	if value, err := cmd.Flags().GetInt("instance.target.specs.flushInMilliseconds"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.FlushInMilliseconds = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.keyColumnName"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.KeyColumnName = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.lineBreak"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.LineBreak = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.prefix"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.Prefix = value
	}

	if value, err := cmd.Flags().GetInt("instance.target.specs.batchSize"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.BatchSize = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.queue"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.Queue = value
	}

	if value, err := cmd.Flags().GetString("instance.target.specs.queueUrl"); err == nil {
		stream.Stream.Instance.Target.TargetSpecs.QueueUrl = value
	}

	if values, err := cmd.Flags().GetStringArray("instance.target.specs.configurations"); err == nil {
		for _, v := range values {
			data := strings.Split(v, "=")
			if len(data) < 1 {
				fmt.Println(fmt.Sprintf("%sfailed to set target configuration: %v%s", color.Yellow, data, color.Reset))
				continue
			}

			stream.Stream.Instance.Target.TargetSpecs.Configurations[data[0]] = data[1]
		}
	}

	scaffold, err := yaml.Marshal(stream)
	if err != nil {
		return errors.New(fmt.Sprintf("failed to serialize yaml: %s", err.Error()))
	}

	if err := ioutil.WriteFile(path, scaffold, 0644); err != nil {
		return errors.New(fmt.Sprintf("failed to create file %s: %s", path, err.Error()))
	}

	fmt.Println(fmt.Sprintf("%sgenerated scaffold \npath: %s\n-------\n%s-------%s", color.Green, path, scaffold, color.Reset))

	return nil
}
