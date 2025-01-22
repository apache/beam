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

package tools

import (
	"context"
	"log"
	"os"
	"strings"
	"time"
)

const (
	initialLogSize       int           = 255
	defaultFlushInterval time.Duration = 15 * time.Second
)

// BufferedLogger is a wrapper around the FnAPI logging client meant to be used
// in place of stdout and stderr in bootloader subprocesses. Not intended for
// Beam end users.
type BufferedLogger struct {
	logger               *Logger
	builder              strings.Builder
	logs                 []string
	lastFlush            time.Time
	flushInterval        time.Duration
	periodicFlushContext context.Context
	now                  func() time.Time
}

// NewBufferedLogger returns a new BufferedLogger type by reference.
func NewBufferedLogger(logger *Logger) *BufferedLogger {
	return &BufferedLogger{logger: logger, lastFlush: time.Now(), flushInterval: defaultFlushInterval, periodicFlushContext: context.Background(), now: time.Now}
}

// NewBufferedLoggerWithFlushInterval returns a new BufferedLogger type by reference. This type will
// flush logs periodically on Write() calls as well as when Flush*() functions are called.
func NewBufferedLoggerWithFlushInterval(ctx context.Context, logger *Logger, interval time.Duration) *BufferedLogger {
	return &BufferedLogger{logger: logger, lastFlush: time.Now(), flushInterval: interval, periodicFlushContext: ctx, now: time.Now}
}

// Write implements the io.Writer interface, converting input to a string
// and storing it in the BufferedLogger's buffer. If a logger is not provided,
// the output is sent directly to os.Stderr.
func (b *BufferedLogger) Write(p []byte) (int, error) {
	if b.logger == nil {
		return os.Stderr.Write(p)
	}
	n, err := b.builder.Write(p)
	if b.logs == nil {
		b.logs = make([]string, 0, initialLogSize)
	}
	b.logs = append(b.logs, b.builder.String())
	b.builder.Reset()
	if b.now().Sub(b.lastFlush) > b.flushInterval {
		b.FlushAtDebug(b.periodicFlushContext)
	}
	return n, err
}

// FlushAtError flushes the contents of the buffer to the logging
// service at Error.
func (b *BufferedLogger) FlushAtError(ctx context.Context) {
	if b.logger == nil {
		return
	}
	for _, message := range b.logs {
		b.logger.Errorf(ctx, message)
	}
	b.logs = nil
	b.lastFlush = time.Now()
}

// FlushAtDebug flushes the contents of the buffer to the logging
// service at Debug.
func (b *BufferedLogger) FlushAtDebug(ctx context.Context) {
	if b.logger == nil {
		return
	}
	for _, message := range b.logs {
		b.logger.Printf(ctx, message)
	}
	b.logs = nil
	b.lastFlush = time.Now()
}

// Prints directly to the logging service. If the logger is nil, prints directly to the
// console. Used for the container pre-build workflow.
func (b *BufferedLogger) Printf(ctx context.Context, format string, args ...any) {
	if b.logger == nil {
		log.Printf(format, args...)
		return
	}
	b.logger.Printf(ctx, format, args...)
}
