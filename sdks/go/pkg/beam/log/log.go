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
	"os"
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

// Logger is a context-aware logging backend. The richer context allows for
// more sophisticated logging setups. Must be concurrency safe.
type Logger interface {
	// Log logs the message in some implementation-dependent way. Log should
	// always return regardless of the severity.
	Log(ctx context.Context, sev Severity, calldepth int, msg string)
}

var (
	logger Logger = &Standard{}
)

// SetLogger sets the global Logger. Intended to be called during initialization
// only.
func SetLogger(l Logger) {
	if l == nil {
		panic("Logger cannot be nil")
	}
	logger = l
}

// Output logs the given message to the global logger. Calldepth is the count
// of the number of frames to skip when computing the file name and line number.
func Output(ctx context.Context, sev Severity, calldepth int, msg string) {
	logger.Log(ctx, sev, calldepth+1, msg) // +1 for this frame
}

// User-facing logging functions.

// Debug writes the fmt.Sprint-formatted arguments to the global logger with
// debug severity.
func Debug(ctx context.Context, v ...interface{}) {
	Output(ctx, SevDebug, 2, fmt.Sprint(v...))
}

// Debugf writes the fmt.Sprintf-formatted arguments to the global logger with
// debug severity.
func Debugf(ctx context.Context, format string, v ...interface{}) {
	Output(ctx, SevDebug, 2, fmt.Sprintf(format, v...))
}

// Debugln writes the fmt.Sprintln-formatted arguments to the global logger with
// debug severity.
func Debugln(ctx context.Context, v ...interface{}) {
	Output(ctx, SevDebug, 2, fmt.Sprintln(v...))
}

// Info writes the fmt.Sprint-formatted arguments to the global logger with
// info severity.
func Info(ctx context.Context, v ...interface{}) {
	Output(ctx, SevInfo, 2, fmt.Sprint(v...))
}

// Infof writes the fmt.Sprintf-formatted arguments to the global logger with
// info severity.
func Infof(ctx context.Context, format string, v ...interface{}) {
	Output(ctx, SevInfo, 2, fmt.Sprintf(format, v...))
}

// Infoln writes the fmt.Sprintln-formatted arguments to the global logger with
// info severity.
func Infoln(ctx context.Context, v ...interface{}) {
	Output(ctx, SevInfo, 2, fmt.Sprintln(v...))
}

// Warn writes the fmt.Sprint-formatted arguments to the global logger with
// warn severity.
func Warn(ctx context.Context, v ...interface{}) {
	Output(ctx, SevWarn, 2, fmt.Sprint(v...))
}

// Warnf writes the fmt.Sprintf-formatted arguments to the global logger with
// warn severity.
func Warnf(ctx context.Context, format string, v ...interface{}) {
	Output(ctx, SevWarn, 2, fmt.Sprintf(format, v...))
}

// Warnln writes the fmt.Sprintln-formatted arguments to the global logger with
// warn severity.
func Warnln(ctx context.Context, v ...interface{}) {
	Output(ctx, SevWarn, 2, fmt.Sprintln(v...))
}

// Error writes the fmt.Sprint-formatted arguments to the global logger with
// error severity.
func Error(ctx context.Context, v ...interface{}) {
	Output(ctx, SevError, 2, fmt.Sprint(v...))
}

// Errorf writes the fmt.Sprintf-formatted arguments to the global logger with
// error severity.
func Errorf(ctx context.Context, format string, v ...interface{}) {
	Output(ctx, SevError, 2, fmt.Sprintf(format, v...))
}

// Errorln writes the fmt.Sprintln-formatted arguments to the global logger with
// error severity.
func Errorln(ctx context.Context, v ...interface{}) {
	Output(ctx, SevError, 2, fmt.Sprintln(v...))
}

// Fatal writes the fmt.Sprint-formatted arguments to the global logger with
// fatal severity. It then panics.
func Fatal(ctx context.Context, v ...interface{}) {
	msg := fmt.Sprint(v...)
	Output(ctx, SevFatal, 2, msg)
	panic(msg)
}

// Fatalf writes the fmt.Sprintf-formatted arguments to the global logger with
// fatal severity. It then panics.
func Fatalf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	Output(ctx, SevFatal, 2, msg)
	panic(msg)
}

// Fatalln writes the fmt.Sprintln-formatted arguments to the global logger with
// fatal severity. It then panics.
func Fatalln(ctx context.Context, v ...interface{}) {
	msg := fmt.Sprintln(v...)
	Output(ctx, SevFatal, 2, msg)
	panic(msg)
}

// Exit writes the fmt.Sprint-formatted arguments to the global logger with
// fatal severity. It then exits.
func Exit(ctx context.Context, v ...interface{}) {
	Output(ctx, SevFatal, 2, fmt.Sprint(v...))
	os.Exit(1)
}

// Exitf writes the fmt.Sprintf-formatted arguments to the global logger with
// fatal severity. It then exits.
func Exitf(ctx context.Context, format string, v ...interface{}) {
	Output(ctx, SevFatal, 2, fmt.Sprintf(format, v...))
	os.Exit(1)
}

// Exitln writes the fmt.Sprintln-formatted arguments to the global logger with
// fatal severity. It then exits.
func Exitln(ctx context.Context, v ...interface{}) {
	Output(ctx, SevFatal, 2, fmt.Sprintln(v...))
	os.Exit(1)
}
