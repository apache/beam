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
	"bytes"
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

// Write implements the io.Writer interface. It buffers byte streams line-by-line
// into memory and flushes periodically or upon calling Flush(), FlushAtError(), or
// FlushAtDebug(). It is used primarily to redirect stdout/stderr of subprocesses or
// standard Go log output. If a logger is not provided, the output is sent directly to os.Stderr.
func (b *BufferedLogger) Write(p []byte) (int, error) {
	if b.logger == nil {
		return os.Stderr.Write(p)
	}

	if b.logs == nil {
		b.logs = make([]string, 0, initialLogSize)
	}

	start := 0
	for {
		// Look for the next newline in the incoming byte slice directly
		nl := bytes.IndexByte(p[start:], '\n')
		if nl == -1 {
			break
		}

		// Write the segment up to the newline into the builder
		b.builder.Write(p[start : start+nl])

		// The builder now contains any previous partial line + the current complete segment
		b.logs = append(b.logs, strings.TrimSuffix(b.builder.String(), "\r"))
		b.builder.Reset()

		start += nl + 1
	}

	// Buffer any remaining bytes that didn't end in a newline
	if start < len(p) {
		b.builder.Write(p[start:])
	}

	if b.now().Sub(b.lastFlush) > b.flushInterval {
		b.FlushAtDebug(b.periodicFlushContext)
	}

	return len(p), nil
}

// Flush flushes the contents of the buffer to the logging service.
// If err is non-nil, it flushes at Error severity; otherwise it flushes at Debug severity.
// It returns the provided error.
func (b *BufferedLogger) Flush(ctx context.Context, err error) error {
	if err != nil {
		b.FlushAtError(ctx)
	} else {
		b.FlushAtDebug(ctx)
	}
	return err
}

// FlushAtError flushes the contents of the buffer to the logging
// service at Error.
func (b *BufferedLogger) FlushAtError(ctx context.Context) {
	if b.logger == nil {
		return
	}
	if b.builder.Len() > 0 {
		b.logs = append(b.logs, strings.TrimSuffix(b.builder.String(), "\r"))
		b.builder.Reset()
	}
	for _, message := range b.logs {
		b.logger.Errorf(ctx, "%s", message)
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
	if b.builder.Len() > 0 {
		b.logs = append(b.logs, strings.TrimSuffix(b.builder.String(), "\r"))
		b.builder.Reset()
	}
	for _, message := range b.logs {
		b.logger.Printf(ctx, "%s", message)
	}
	b.logs = nil
	b.lastFlush = time.Now()
}

// Printf directly writes formatted messages to the underlying logger/service,
// bypassing line buffering. If the logger is nil, it prints directly to the
// console. Used for direct informational logs and the container pre-build workflow.
func (b *BufferedLogger) Printf(ctx context.Context, format string, args ...any) {
	if b.logger == nil {
		log.Printf(format, args...)
		return
	}
	b.logger.Printf(ctx, format, args...)
}
