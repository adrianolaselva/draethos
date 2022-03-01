package start

import (
	"errors"
	"fmt"

	"draethos.io.com/core"
	"draethos.io.com/core/interfaces"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

type startCommand struct {
}

func NewStartCommand() interfaces.BuildCommand {
	return startCommand{}
}

func (s startCommand) Build() *cobra.Command {
	var startCommand = &cobra.Command{
		Use:     "start",
		Short:   "Start application",
		Long:    "Initialize stream from file informed by parameter",
		Example: "./draethos start -f pipeline.yaml",
		RunE:    s.runE,
	}

	startCommand.
		PersistentFlags().
		StringP(
			"file",
			"f",
			"",
			"file pipelines to be initialized")

	startCommand.
		PersistentFlags().
		BoolP(
			"liveness",
			"l",
			false,
			"initialize with health check")

	startCommand.
		PersistentFlags().
		BoolP(
			"metrics",
			"m",
			false,
			"initialize with metrics")

	startCommand.
		PersistentFlags().
		BoolP(
			"verbose",
			"v",
			false,
			"display verbose mode")

	startCommand.
		PersistentFlags().
		StringP(
			"port",
			"p",
			"",
			"http server port")

	return startCommand
}

func (startCommand) runE(cmd *cobra.Command, args []string) error {
	configBuilder := core.NewConfigBuilder()

	if value, err := cmd.Flags().GetString("file"); err == nil {
		configBuilder.SetFile(value)
	}

	if value, err := cmd.Flags().GetString("port"); err == nil {
		configBuilder.SetPort(value)
	}

	if value, err := cmd.Flags().GetBool("liveness"); err == nil && value {
		configBuilder.EnableLiveness()
	}

	if value, err := cmd.Flags().GetBool("metrics"); err == nil && value {
		configBuilder.EnableMetrics()
	}

	config, err := configBuilder.Build()
	if err != nil {
		zap.S().Error(err.Error())

		return errors.New(fmt.Sprintf("failed to initialize stream: %s", err.Error()))
	}

	if err = core.NewWorker(*config, configBuilder).Start(); err != nil {
		zap.S().Error(err.Error())

		return errors.New(fmt.Sprintf("failed to initialize stream: %s", err.Error()))
	}

	return nil
}
