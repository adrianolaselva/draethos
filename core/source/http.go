package source

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"draethos.io.com/core/interfaces"
	"draethos.io.com/pkg/streams/specs"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type httpSource struct {
	sourceSpec   specs.Source
	target       interfaces.TargetInterface
	dlq          interfaces.TargetInterface
	codec        interfaces.CodecInterface
	router       *mux.Router
	port         string
	writeTimeout int64
	readTimeout  int64
	idleTimeout  int64
	serverName   string
}

const MethodsAllowedDefault = "GET,POST"

func NewHttpSource(sourceSpec specs.Source,
	target interfaces.TargetInterface,
	dlq interfaces.TargetInterface,
	codec interfaces.CodecInterface,
	router *mux.Router,
	port string) (interfaces.SourceInterface, error) {
	return httpSource{
		sourceSpec: sourceSpec,
		target:     target,
		dlq:        dlq,
		codec:      codec,
		router:     router,
		port:       port,
	}, nil
}

func (k httpSource) Worker() error {
	if err := k.target.Initialize(); err != nil {
		return err
	}

	if k.dlq != nil {
		if err := k.dlq.Initialize(); err != nil {
			return err
		}
	}

	if v, ok := k.sourceSpec.SourceSpecs.Configurations["writeTimeout"].(int64); ok {
		k.writeTimeout = v
	}

	if v, ok := k.sourceSpec.SourceSpecs.Configurations["readTimeout"].(int64); ok {
		k.readTimeout = v
	}

	if v, ok := k.sourceSpec.SourceSpecs.Configurations["idleTimeout"].(int64); ok {
		k.idleTimeout = v
	}

	if v, ok := k.sourceSpec.SourceSpecs.Configurations["serverName"].(string); ok {
		k.serverName = v
	}

	if k.sourceSpec.SourceSpecs.Method == "" {
		k.sourceSpec.SourceSpecs.Method = MethodsAllowedDefault
	}

	k.router.
		HandleFunc(k.sourceSpec.SourceSpecs.Endpoint, k.Handle).
		Methods(strings.Split(k.sourceSpec.SourceSpecs.Method, ",")...)

	zap.S().Infof("endpoint initialize [endpoint: %s, method(s): %s]",
		fmt.Sprintf("0.0.0.0:%s%s", k.port, k.sourceSpec.SourceSpecs.Endpoint),
		k.sourceSpec.SourceSpecs.Method)

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	srv := &http.Server{
		Handler:      k.router,
		Addr:         fmt.Sprintf("0.0.0.0:%s", k.port),
		WriteTimeout: time.Duration(k.writeTimeout) * time.Second,
		ReadTimeout:  time.Duration(k.readTimeout) * time.Second,
		IdleTimeout:  time.Duration(k.idleTimeout) * time.Second,
		TLSConfig: &tls.Config{
			ServerName: k.serverName,
		},
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			zap.S().Warnf(err.Error())
			sigchan <- syscall.SIGTERM
		}
	}()

	<-sigchan
	if err := k.target.Flush(); err != nil {
		return errors.New(fmt.Sprintf("failed to flush event: %s", err.Error()))
	}

	return nil
}

func (k httpSource) Handle(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()
	key := fmt.Sprintf("'%x'", md5.Sum([]byte(time.Now().String())))

	w.Header().Set("x-stream-application", "draethos")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("x-request-key", key)

	payload := make(map[string]interface{})
	if body, err := ioutil.ReadAll(r.Body); err == nil && len(body) > 0 {
		key = fmt.Sprintf("'%x'", md5.Sum(body))

		payload, err = k.codec.Deserialize(body)
		if err != nil {
			zap.S().Errorf("failed to deserialize content: %s", err.Error())

			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{
				"message": "failed to deserialize content",
			})
			return
		}
	}

	queryParams := r.URL.Query()
	for k := range queryParams {
		payload[k] = r.URL.Query().Get(k)
	}

	zap.S().Infof("processing request [%s %s => %v]", r.Method, r.RequestURI, payload)

	if err := k.target.Attach(key, payload); err != nil {
		zap.S().Errorf("failed to attach content: %s", err.Error())

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "failed to attach content",
		})
		return
	}

	if !k.target.CanFlush() {
		w.WriteHeader(http.StatusAccepted)
		json.NewEncoder(w).Encode(payload)
		return
	}

	if err := k.target.Flush(); err != nil {
		zap.S().Errorf("failed to flush event: %s", err.Error())

		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{
			"message": "failed to flush event",
		})
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(payload)
}
