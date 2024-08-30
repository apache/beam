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
	"log/slog"
	"os"
	"path"
	"runtime"
	"sync"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/apiv2/loggingpb"
)

// Options for the slog.Logger
type Options struct {
	*slog.HandlerOptions
	Name   string
	Writer io.Writer
	Client *logging.Client
}

// New instantiates a slog.Logger to output using Google Cloud logging entries.
// When running locally, output is JSON strings of Cloud logging entries and
// does not make any API calls to the service. When running in Google Cloud,
// logging entries are submitted to the Cloud logging service.
func New(opts *Options) *slog.Logger {
	if opts.HandlerOptions == nil {
		opts.HandlerOptions = &slog.HandlerOptions{}
	}

	opts.AddSource = true

	if opts.Writer == nil {
		opts.Writer = os.Stdout
	}

	handler := &gcpHandler{
		name:        opts.Name,
		mu:          &sync.Mutex{},
		out:         opts.Writer,
		JSONHandler: slog.NewJSONHandler(opts.Writer, opts.HandlerOptions),
	}

	if opts.Client != nil {
		handler.logger = opts.Client.Logger(path.Base(opts.Name))
	}

	return slog.New(handler)
}

var _ slog.Handler = &gcpHandler{}

type gcpHandler struct {
	name string
	*slog.JSONHandler
	mu     *sync.Mutex
	out    io.Writer
	logger *logging.Logger
}

func (g *gcpHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return g.JSONHandler.Enabled(ctx, level)
}

func severity(lvl slog.Level) logging.Severity {
	switch lvl {
	case slog.LevelDebug:
		return logging.Debug
	case slog.LevelInfo:
		return logging.Info
	case slog.LevelWarn:
		return logging.Warning
	case slog.LevelError:
		return logging.Error
	}
	return logging.Default
}

func (g *gcpHandler) Handle(_ context.Context, record slog.Record) error {
	payload := map[string]any{
		"message": record.Message,
	}
	record.Attrs(func(attr slog.Attr) bool {
		payload[attr.Key] = attr.Value.Any()
		return true
	})
	fs := runtime.CallersFrames([]uintptr{record.PC})
	f, _ := fs.Next()
	entry := logging.Entry{
		LogName:   g.name,
		Timestamp: record.Time,
		Severity:  severity(record.Level),
		Payload:   payload,
		SourceLocation: &loggingpb.LogEntrySourceLocation{
			File: f.File,
			Line: int64(f.Line),
		},
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.logger == nil {
		return json.NewEncoder(g.out).Encode(entry)
	}

	entry.LogName = ""
	g.logger.Log(entry)
	return g.logger.Flush()
}

func (g *gcpHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h := g.JSONHandler
	return h.WithAttrs(attrs)
}

func (g *gcpHandler) WithGroup(name string) slog.Handler {
	h := g.JSONHandler
	return h.WithGroup(name)
}
