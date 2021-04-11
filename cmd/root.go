package cmd

import (
	"fmt"
	"github.com/heptiolabs/healthcheck"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"net/http"
	"os"
	"syscall"
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
		Long:    fmt.Sprintf(TerminalPrint, release),
	}

	startCommand.
		PersistentFlags().
		StringP(
			"file",
			"f",
			"",
			"file pipelines to be initialized")

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

func initializeHealthCheck(checkEndpoint string, liveEndpoint string, port string) {
	var health = healthcheck.
		NewHandler()

	//health.AddLivenessCheck("goroutine-threshold", healthcheck.HTTPGetCheck("", 100))
	//health.AddLivenessCheck("goroutine-threshold", healthcheck.GoroutineCountCheck(100))
	health.AddReadinessCheck("health-name", func() error {
		return nil
	})

	mux := http.NewServeMux()
	mux.HandleFunc(checkEndpoint, health.LiveEndpoint)
	mux.HandleFunc(liveEndpoint, health.ReadyEndpoint)

	go http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", port), mux)
}

func initializePrometheus(endpoint string, port string) {
	http.Handle(endpoint, promhttp.Handler())
	go http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", port), nil)
}
