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
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/grpcx"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Logger is a wrapper around the FnAPI Logging Client, intended for
// container boot loader use. Not intended for Beam end users.
type Logger struct {
	Endpoint string

	client  logSender
	closeFn func()
	mu      sync.Mutex // To protect Send in the rare case multiple goroutines are calling this logger.
}

type logSender interface {
	Send(*fnpb.LogEntry_List) error
	CloseSend() error
}

// Close closes the grpc logging client.
func (l *Logger) Close() {
	if l.closeFn != nil {
		l.client.CloseSend()
		l.closeFn()
		l.closeFn = nil
		l.client = nil
	}
}

// Log a message with the given severity.
func (l *Logger) Log(ctx context.Context, sev fnpb.LogEntry_Severity_Enum, message string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var exitErr error
	defer func() {
		if exitErr != nil {
			log.Println("boot.go: error logging message over FnAPI. endpoint", l.Endpoint, "error:", exitErr, "message follows")
			log.Println(sev.String(), message)
		}
	}()
	if l.client == nil {
		if l.Endpoint == "" {
			exitErr = errors.New("no logging endpoint set")
			return
		}
		cc, err := grpcx.Dial(ctx, l.Endpoint, 2*time.Minute)
		if err != nil {
			exitErr = err
			return
		}
		l.closeFn = func() { cc.Close() }

		l.client, err = fnpb.NewBeamFnLoggingClient(cc).Logging(ctx)
		if err != nil {
			exitErr = err
			l.Close()
			return
		}
	}

	err := l.client.Send(&fnpb.LogEntry_List{
		LogEntries: []*fnpb.LogEntry{
			{
				Severity:  sev,
				Timestamp: timestamppb.Now(),
				Message:   message,
			},
		},
	})
	if err != nil {
		exitErr = err
		return
	}
}

// Printf logs the message with Debug severity.
func (l *Logger) Printf(ctx context.Context, format string, args ...any) {
	l.Log(ctx, fnpb.LogEntry_Severity_DEBUG, fmt.Sprintf(format, args...))
}

// Warnf logs the message with Warning severity.
func (l *Logger) Warnf(ctx context.Context, format string, args ...any) {
	l.Log(ctx, fnpb.LogEntry_Severity_WARN, fmt.Sprintf(format, args...))
}

// Errorf logs the message with Error severity.
func (l *Logger) Errorf(ctx context.Context, format string, args ...any) {
	l.Log(ctx, fnpb.LogEntry_Severity_ERROR, fmt.Sprintf(format, args...))
}

// Fatalf logs the message with Critical severity, and then calls os.Exit(1).
func (l *Logger) Fatalf(ctx context.Context, format string, args ...any) {
	l.Log(ctx, fnpb.LogEntry_Severity_CRITICAL, fmt.Sprintf(format, args...))
	os.Exit(1)
}
