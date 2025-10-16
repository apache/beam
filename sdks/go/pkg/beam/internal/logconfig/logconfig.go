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

// Package logconfig contains functionality for set up structural logger level and kind.
package logconfig

import (
	"log"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/golang-cz/devslog"
)

var (
	LogLevel = "info" // The logging level for slog. Valid values are `debug`, `info`, `warn` or `error`. Default is `info`.
	LogKind  = "text" // The logging format for slog. Valid values are `dev', 'json', or 'text'. Default is `text`.
)

// ConfigureLoggingWithDefault initializes the structural logger with default settings.
// It configures the logging level and format based on the global LogLevel and LogKind variables.
// If LogLevel or LogKind are invalid, it will cause a fatal error.
func ConfigureLoggingWithDefault() {
	var logLevel = new(slog.LevelVar)
	var logHandler slog.Handler
	loggerOutput := os.Stderr
	handlerOpts := &slog.HandlerOptions{
		Level: logLevel,
	}
	switch strings.ToLower(LogLevel) {
	case "debug":
		logLevel.Set(slog.LevelDebug)
		handlerOpts.AddSource = true
	case "info":
		logLevel.Set(slog.LevelInfo)
	case "warn":
		logLevel.Set(slog.LevelWarn)
	case "error":
		logLevel.Set(slog.LevelError)
	default:
		log.Fatalf("Invalid value for log_level: %v, must be 'debug', 'info', 'warn', or 'error'", LogLevel)
	}
	switch strings.ToLower(LogKind) {
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
		log.Fatalf("Invalid value for log_kind: %v, must be 'dev', 'json', or 'text'", LogKind)
	}

	slog.SetDefault(slog.New(logHandler))
}

// ConfigureLogging initializes the structural logger with the provided logging level and kind.
// It sets the global LogLevel and LogKind variables and then calls ConfigureLoggingWithDefault.
func ConfigureLogging(logLevel, logKind string) {
	LogLevel = logLevel
	LogKind = logKind
	ConfigureLoggingWithDefault()
}
