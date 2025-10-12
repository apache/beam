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

// Package log contains a re-targetable context-aware logging system. Notably,
// it allows Beam runners to transparently provide appropriate logging context
// -- such as DoFn or bundle information -- for user code logging.
package log

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/golang-cz/devslog"
)

// Severity is the severity of the log message.
type Severity int

const (
	SevUnspecified Severity = iota
	SevDebug
	SevInfo
	SevWarn
	SevError
	SevFatal
)

var (
	LogLevel = "info" // The logging level for slog. Valid values are `debug`, `info`, `warn` or `error`. Default is `info`.
	LogKind  = "text" // The logging format for slog. Valid values are `dev', 'json', or 'text'. Default is `text`.
)

// Logger is a context-aware logging backend. The richer context allows for
// more sophisticated logging setups. Must be concurrency safe.
type Logger interface {
	// Log logs the message in some implementation-dependent way. Log should
	// always return regardless of the severity.
	Log(ctx context.Context, sev Severity, calldepth int, msg string)
}

var logger atomic.Value

// concreteLogger works around atomic.Value's requirement that the type
// be identical for all callers.
type concreteLogger struct {
	Logger
}

func init() {
	logger.Store(&concreteLogger{&Standard{}})
}

// SetLogger sets the global Logger. Intended to be called during initialization
// only.
func SetLogger(l Logger) {
	if l == nil {
		panic("Logger cannot be nil")
	}
	logger.Store(&concreteLogger{l})
}

// Output logs the given message to the global logger. Calldepth is the count
// of the number of frames to skip when computing the file name and line number.
func Output(ctx context.Context, sev Severity, calldepth int, msg string) {
	logger.Load().(Logger).Log(ctx, sev, calldepth+1, msg) // +1 for this frame
}

// User-facing logging functions.

// Debug writes the fmt.Sprint-formatted arguments to the global logger with
// debug severity.
func Debug(ctx context.Context, v ...any) {
	Output(ctx, SevDebug, 1, fmt.Sprint(v...))
}

// Debugf writes the fmt.Sprintf-formatted arguments to the global logger with
// debug severity.
func Debugf(ctx context.Context, format string, v ...any) {
	Output(ctx, SevDebug, 1, fmt.Sprintf(format, v...))
}

// Debugln writes the fmt.Sprintln-formatted arguments to the global logger with
// debug severity.
func Debugln(ctx context.Context, v ...any) {
	Output(ctx, SevDebug, 1, fmt.Sprintln(v...))
}

// Info writes the fmt.Sprint-formatted arguments to the global logger with
// info severity.
func Info(ctx context.Context, v ...any) {
	Output(ctx, SevInfo, 1, fmt.Sprint(v...))
}

// Infof writes the fmt.Sprintf-formatted arguments to the global logger with
// info severity.
func Infof(ctx context.Context, format string, v ...any) {
	Output(ctx, SevInfo, 1, fmt.Sprintf(format, v...))
}

// Infoln writes the fmt.Sprintln-formatted arguments to the global logger with
// info severity.
func Infoln(ctx context.Context, v ...any) {
	Output(ctx, SevInfo, 1, fmt.Sprintln(v...))
}

// Warn writes the fmt.Sprint-formatted arguments to the global logger with
// warn severity.
func Warn(ctx context.Context, v ...any) {
	Output(ctx, SevWarn, 1, fmt.Sprint(v...))
}

// Warnf writes the fmt.Sprintf-formatted arguments to the global logger with
// warn severity.
func Warnf(ctx context.Context, format string, v ...any) {
	Output(ctx, SevWarn, 1, fmt.Sprintf(format, v...))
}

// Warnln writes the fmt.Sprintln-formatted arguments to the global logger with
// warn severity.
func Warnln(ctx context.Context, v ...any) {
	Output(ctx, SevWarn, 1, fmt.Sprintln(v...))
}

// Error writes the fmt.Sprint-formatted arguments to the global logger with
// error severity.
func Error(ctx context.Context, v ...any) {
	Output(ctx, SevError, 1, fmt.Sprint(v...))
}

// Errorf writes the fmt.Sprintf-formatted arguments to the global logger with
// error severity.
func Errorf(ctx context.Context, format string, v ...any) {
	Output(ctx, SevError, 1, fmt.Sprintf(format, v...))
}

// Errorln writes the fmt.Sprintln-formatted arguments to the global logger with
// error severity.
func Errorln(ctx context.Context, v ...any) {
	Output(ctx, SevError, 1, fmt.Sprintln(v...))
}

// Fatal writes the fmt.Sprint-formatted arguments to the global logger with
// fatal severity. It then panics.
func Fatal(ctx context.Context, v ...any) {
	msg := fmt.Sprint(v...)
	Output(ctx, SevFatal, 1, msg)
	panic(msg)
}

// Fatalf writes the fmt.Sprintf-formatted arguments to the global logger with
// fatal severity. It then panics.
func Fatalf(ctx context.Context, format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	Output(ctx, SevFatal, 1, msg)
	panic(msg)
}

// Fatalln writes the fmt.Sprintln-formatted arguments to the global logger with
// fatal severity. It then panics.
func Fatalln(ctx context.Context, v ...any) {
	msg := fmt.Sprintln(v...)
	Output(ctx, SevFatal, 1, msg)
	panic(msg)
}

// Exit writes the fmt.Sprint-formatted arguments to the global logger with
// fatal severity. It then exits.
func Exit(ctx context.Context, v ...any) {
	Output(ctx, SevFatal, 1, fmt.Sprint(v...))
	os.Exit(1)
}

// Exitf writes the fmt.Sprintf-formatted arguments to the global logger with
// fatal severity. It then exits.
func Exitf(ctx context.Context, format string, v ...any) {
	Output(ctx, SevFatal, 1, fmt.Sprintf(format, v...))
	os.Exit(1)
}

// Exitln writes the fmt.Sprintln-formatted arguments to the global logger with
// fatal severity. It then exits.
func Exitln(ctx context.Context, v ...any) {
	Output(ctx, SevFatal, 1, fmt.Sprintln(v...))
	os.Exit(1)
}

func SetupLoggingWithDefault() {
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

func SetupLogging(logLevel, logKind string) {
	LogLevel = logLevel
	LogKind = logKind
	SetupLoggingWithDefault()
}
