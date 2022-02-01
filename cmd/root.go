package cmd

import (
	"fmt"
	"os"
	"syscall"

	"draethos.io.com/pkg/color"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	TerminalPrint = ` ____                 _   _               
|  _ \ _ __ __ _  ___| |_| |__   ___  ___ 
| | | | '__/ _' |/ _ \ __| '_ \ / _ \/ __|
| |_| | | | (_| |  __/ |_| | | | (_) \__ \
|____/|_|  \__,_|\___|\__|_| |_|\___/|___/
			   release: %s`
)

func Execute() {
	initializeLogger()

	var release = "latest"
	if value, ok := syscall.Getenv("VERSION"); ok {
		release = value
	}

	var rootCmd = &cobra.Command{
		Use:     "draethos",
		Version: release,
		Long:    fmt.Sprintf("%s%s%s", color.Green, fmt.Sprintf(TerminalPrint, release), color.Reset),
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
		StringP(
			"liveness",
			"l",
			"",
			"initialize with health check")

	startCommand.
		PersistentFlags().
		StringP(
			"metrics",
			"m",
			"",
			"initialize with metrics")

	rootCmd.AddCommand(startCommand)

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func initializeLogger() {
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
}
