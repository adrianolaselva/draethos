package cmd

import (
	"draethos.io.com/cmd/scaffold"
	"draethos.io.com/cmd/start"
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
	logger, _ := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)

	var release = "latest"
	if value, ok := syscall.Getenv("VERSION"); ok {
		release = value
	}

	var rootCmd = &cobra.Command{
		Use:     "draethos",
		Version: release,
		Long:    fmt.Sprintf("%s%s%s", color.Green, fmt.Sprintf(TerminalPrint, release), color.Reset),
	}

	rootCmd.AddCommand(start.NewStartCommand().Build())
	rootCmd.AddCommand(scaffold.NewScaffoldCommand().Build())

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(fmt.Sprintf("%s%s%s", color.Red, err.Error(), color.Reset))
		os.Exit(1)
	}
}
