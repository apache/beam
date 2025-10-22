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
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/golang-cz/devslog"
)

const (
	DefaultLogLevel = "info" // The default logging level for slog. Valid values are `debug`, `info`, `warn` or `error`.
	DefaultLogKind  = "text" // The default logging format for slog. Valid values are `dev', 'json', or 'text'.
)

// CreateLogger creates a new slog.Logger with the specified logging level and format.
//
// logLevel: The logging level for slog. Valid values are `debug`, `info`, `warn`, or `error`.
// logKind: The logging format for slog. Valid values are `dev`, `json`, or `text`.
func CreateLogger(logLevel, logKind string) (*slog.Logger, error) {
	var level = new(slog.LevelVar)
	var handler slog.Handler
	loggerOutput := os.Stderr
	handlerOpts := &slog.HandlerOptions{
		Level: level,
	}
	switch strings.ToLower(logLevel) {
	case "debug":
		level.Set(slog.LevelDebug)
		handlerOpts.AddSource = true
	case "info":
		level.Set(slog.LevelInfo)
	case "warn":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		return nil, fmt.Errorf("invalid value for log_level: %v, must be 'debug', 'info', 'warn', or 'error'", logLevel)
	}
	switch strings.ToLower(logKind) {
	case "dev":
		handler =
			devslog.NewHandler(loggerOutput, &devslog.Options{
				TimeFormat:         "[" + time.RFC3339Nano + "]",
				StringerFormatter:  true,
				HandlerOptions:     handlerOpts,
				StringIndentation:  false,
				NewLineAfterLog:    true,
				MaxErrorStackTrace: 3,
			})
	case "json":
		handler = slog.NewJSONHandler(loggerOutput, handlerOpts)
	case "text":
		handler = slog.NewTextHandler(loggerOutput, handlerOpts)
	default:
		return nil, fmt.Errorf("invalid value for log_kind: %v, must be 'dev', 'json', or 'text'", logKind)
	}

	return slog.New(handler), nil
}

func CreateLoggerWithDefault() (*slog.Logger, error) {
	return CreateLogger(DefaultLogLevel, DefaultLogKind)
}
