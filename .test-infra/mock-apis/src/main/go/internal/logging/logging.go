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

// Package logging performs structured output of log entries.
package logging

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"runtime"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/apiv2/loggingpb"
)

var (
	defaultWriter  = os.Stdout
	defaultTimeout = time.Second * 3
)

func New(name string, opts ...Option) Logger {
	l := &logger{
		name:    name,
		w:       defaultWriter,
		timeout: defaultTimeout,
	}
	for _, opt := range opts {
		opt.apply(l)
	}
	return l
}

type Logger interface {
	Debug(ctx context.Context, message string, fields ...Field)
	Info(ctx context.Context, message string, fields ...Field)
	Error(ctx context.Context, err error, fields ...Field)
	Fatal(ctx context.Context, err error, fields ...Field)
}

type logger struct {
	w       io.Writer
	timeout time.Duration
	name    string
}

type Field struct {
	Key   string
	Value any
}

type Source struct {
	File string
	Line int64
	Func string
}

type Option interface {
	apply(logger *logger)
}

func WithWriter(w io.Writer) Option {
	return &writerOpt{w: w}
}

func WithTimeout(timeout time.Duration) Option {
	return timeoutOpt(timeout)
}

func (l *logger) Info(ctx context.Context, message string, fields ...Field) {
	e := l.entry(logging.Info, message, fields...)
	l.write(ctx, e)
}

func (l *logger) Error(ctx context.Context, err error, fields ...Field) {
	e := l.entry(logging.Error, err.Error(), fields...)
	l.write(ctx, e)
}

func (l *logger) Debug(ctx context.Context, message string, fields ...Field) {
	e := l.entry(logging.Debug, message, fields...)
	l.write(ctx, e)

}

func (l *logger) Fatal(ctx context.Context, err error, fields ...Field) {
	e := l.entry(logging.Error, err.Error(), fields...)
	l.write(ctx, e)
	os.Exit(1)
}

func (l *logger) entry(severity logging.Severity, message string, fields ...Field) logging.Entry {
	payload := map[string]any{
		"message": message,
	}

	for _, f := range fields {
		payload[f.Key] = f.Value
	}

	_, file, line, _ := runtime.Caller(2)

	return logging.Entry{
		Severity:  severity,
		Payload:   payload,
		LogName:   l.name,
		Timestamp: time.Now(),
		SourceLocation: &loggingpb.LogEntrySourceLocation{
			File: file,
			Line: int64(line),
		},
	}
}

func (l *logger) write(ctx context.Context, entry logging.Entry) {
	ctx, cancel := context.WithTimeout(ctx, l.timeout)
	defer cancel()
	if err := json.NewEncoder(l.w).Encode(entry); err != nil {
		panic(err)
	}
	for {
		select {
		case <-ctx.Done():
			return
		}
	}
}

type timeoutOpt time.Duration

func (opt timeoutOpt) apply(logger *logger) {
	logger.timeout = time.Duration(opt)
}

type writerOpt struct {
	w io.Writer
}

func (w *writerOpt) apply(logger *logger) {
	logger.w = w.w
}
