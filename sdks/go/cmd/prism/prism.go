// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// prism is a stand alone local Beam Runner. It produces a JobManagement service endpoint
// against which jobs can be submited, and a web UI to inspect running and completed jobs.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	jobpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/jobmanagement_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"
	"github.com/golang-cz/devslog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	jobPort             = flag.Int("job_port", 8073, "specify the job management service port")
	webPort             = flag.Int("web_port", 8074, "specify the web ui port")
	jobManagerEndpoint  = flag.String("jm_override", "", "set to only stand up a web ui that refers to a seperate JobManagement endpoint")
	serveHTTP           = flag.Bool("serve_http", true, "enable or disable the web ui")
	idleShutdownTimeout = flag.Duration("idle_shutdown_timeout", -1, "duration that prism will wait for a new job before shutting itself down. Negative durations disable auto shutdown. Defaults to never shutting down.")
)

// Logging flags
var (
	debug = flag.Bool("debug", false,
		"Enables full verbosity debug logging from the runner by default. Used to build SDKs or debug Prism itself. Supersedes the log_level flag. Equivalent to --log_level=debug.")
	logKind = flag.String("log_kind", "dev",
		"Determines the format of prism's logging to std err: valid values are `dev', 'json', or 'text'. Default is `dev`.")
	logLevelFlag = flag.String("log_level", "info",
		"Sets the minimum log level of Prism. Valid options are 'debug', 'info','warn', and 'error'. Default is 'info'")
)

var logLevel = new(slog.LevelVar)

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancelCause(context.Background())

	var logHandler slog.Handler
	loggerOutput := os.Stderr
	handlerOpts := &slog.HandlerOptions{
		Level:     logLevel,
		AddSource: *debug,
	}
	switch strings.ToLower(*logLevelFlag) {
	case "debug":
		logLevel.Set(slog.LevelDebug)
	case "info":
		logLevel.Set(slog.LevelInfo)
	case "warn":
		logLevel.Set(slog.LevelWarn)
	case "error":
		logLevel.Set(slog.LevelError)
	default:
		log.Fatalf("Invalid value for log_level: %v, must be 'debug', 'info', 'warn', or 'error'", *logKind)
	}
	if *debug {
		logLevel.Set(slog.LevelDebug)
		// Print the Prism source line for a log in debug mode.
		handlerOpts.AddSource = true
	}
	switch strings.ToLower(*logKind) {
	case "dev":
		logHandler =
			devslog.NewHandler(loggerOutput, &devslog.Options{
				TimeFormat:         "[" + time.RFC3339Nano + "]",
				StringerFormatter:  true,
				HandlerOptions:     handlerOpts,
				StringIndentation:  false,
				NewLineAfterLog:    true,
				MaxErrorStackTrace: 3,
			})
	case "json":
		logHandler = slog.NewJSONHandler(loggerOutput, handlerOpts)
	case "text":
		logHandler = slog.NewTextHandler(loggerOutput, handlerOpts)
	default:
		log.Fatalf("Invalid value for log_kind: %v, must be 'dev', 'json', or 'text'", *logKind)
	}

	slog.SetDefault(slog.New(logHandler))

	cli, err := makeJobClient(ctx,
		prism.Options{
			Port:                *jobPort,
			IdleShutdownTimeout: *idleShutdownTimeout,
			CancelFn:            cancel,
		},
		*jobManagerEndpoint)
	if err != nil {
		log.Fatalf("error creating job server: %v", err)
	}
	if *serveHTTP {
		if err := prism.CreateWebServer(ctx, cli, prism.Options{Port: *webPort}); err != nil {
			log.Fatalf("error creating web server: %v", err)
		}
	}
	// Block main thread forever to keep main from exiting.
	<-ctx.Done()
}

func makeJobClient(ctx context.Context, opts prism.Options, endpoint string) (jobpb.JobServiceClient, error) {
	if endpoint != "" {
		clientConn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			return nil, fmt.Errorf("error connecting to job server at %v: %v", endpoint, err)
		}
		return jobpb.NewJobServiceClient(clientConn), nil
	}
	cli, err := prism.CreateJobServer(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("error creating local job server: %v", err)
	}
	return cli, nil
}
