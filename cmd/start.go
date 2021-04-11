package cmd

import (
	"draethos.io.com/core"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var startCommand = &cobra.Command{
	Use:   "start",
	Short: "Start application",
	Args: func(cmd *cobra.Command, args []string) error {

		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		configBuilder := core.NewConfigBuilder()
		if value, err := cmd.Flags().GetString("file"); err == nil {
			configBuilder.SetFile(value)
		}

		config, err := configBuilder.Build()
		if err != nil {
			zap.S().Error(err.Error())
			return
		}

		if err = core.NewWorker(*config).Start(); err != nil {
			zap.S().Error(err.Error())
			return
		}
	},
}
