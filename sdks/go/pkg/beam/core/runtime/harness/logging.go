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

package harness

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type logger struct {
	out chan<- *fnpb.LogEntry
}

func (l *logger) Log(ctx context.Context, sev log.Severity, calldepth int, msg string) {
	now := timestamppb.New(time.Now())

	entry := &fnpb.LogEntry{
		Timestamp: now,
		Severity:  convertSeverity(sev),
		Message:   msg,
	}
	if _, file, line, ok := runtime.Caller(calldepth + 1); ok {
		entry.LogLocation = fmt.Sprintf("%v:%v", file, line)
	}
	entry.InstructionId = metrics.GetBundleID(ctx)
	entry.TransformId = metrics.GetTransformID(ctx)

	select {
	case l.out <- entry:
		// ok
	default:
		// buffer full: drop to stderr.
		fmt.Fprintln(os.Stderr, msg)
	}
}

func convertSeverity(sev log.Severity) fnpb.LogEntry_Severity_Enum {
	switch sev {
	case log.SevDebug:
		return fnpb.LogEntry_Severity_DEBUG
	case log.SevInfo:
		return fnpb.LogEntry_Severity_INFO
	case log.SevWarn:
		return fnpb.LogEntry_Severity_WARN
	case log.SevError:
		return fnpb.LogEntry_Severity_ERROR
	case log.SevFatal:
		return fnpb.LogEntry_Severity_CRITICAL
	default:
		return fnpb.LogEntry_Severity_INFO
	}
}

type remoteLoggingKey string

// DefaultRemoteLoggingHook is the key used for the default remote logging hook.
// If a runner wants to use an alternative logging solution, it can be
// disabled with hooks.DisableHook(harness.DefaultRemoteLoggingHook).
const DefaultRemoteLoggingHook = "default_remote_logging"

var loggingEndpointCtxKey = remoteLoggingKey(DefaultRemoteLoggingHook)

func init() {
	hooks.RegisterHook(DefaultRemoteLoggingHook, func(args []string) hooks.Hook {
		return hooks.Hook{
			Init: func(ctx context.Context) (context.Context, error) {
				loggingEndpoint := ctx.Value(loggingEndpointCtxKey)
				setupRemoteLogging(ctx, loggingEndpoint.(string))
				return ctx, nil
			},
		}
	})
	hooks.EnableHook(DefaultRemoteLoggingHook)
}

// setupRemoteLogging redirects local log messages to FnHarness. It will
// try to reconnect, if a connection goes bad. Falls back to stdout.
func setupRemoteLogging(ctx context.Context, endpoint string) {
	buf := make(chan *fnpb.LogEntry, 2000)
	log.SetLogger(&logger{out: buf})

	w := &remoteWriter{buffer: buf, endpoint: endpoint}
	go w.Run(ctx)
}

type remoteWriter struct {
	buffer   chan *fnpb.LogEntry
	endpoint string
}

func (w *remoteWriter) Run(ctx context.Context) error {
	for {
		err := w.connect(ctx, w.dialLogClient)
		if err == io.EOF {
			return nil
		}
		// Abort loop if the context is cancelled.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		fmt.Fprintf(os.Stderr, "Remote logging failed: %v. Retrying in 5 sec ...\n", err)
		time.Sleep(5 * time.Second)
	}
}

func (w *remoteWriter) dialLogClient(ctx context.Context) (logSender, func(), error) {
	conn, err := dial(ctx, w.endpoint, "logging", 30*time.Second)
	if err != nil {
		return nil, func() {}, err
	}

	client, err := fnpb.NewBeamFnLoggingClient(conn).Logging(ctx)
	if err != nil {
		conn.Close()
		return nil, func() {}, err
	}

	toDefer := func() {
		client.CloseSend()
		conn.Close()
	}
	return client, toDefer, nil
}

type logSender interface {
	Send(*fnpb.LogEntry_List) error
}

var errBuffClosed = errors.New("internal: log message buffer closed")

func (w *remoteWriter) connect(ctx context.Context, makeClient func(ctx context.Context) (logSender, func(), error)) error {
	client, toDefer, err := makeClient(ctx)
	if err != nil {
		return err
	}
	defer toDefer()

	for {
		const batchSize = 64
		msgs := make([]*fnpb.LogEntry, 0, batchSize)
		var flush bool
		select {
		case <-ctx.Done():
			return nil
		case newMsg, ok := <-w.buffer:
			if !ok {
				return errBuffClosed
			}
			msgs = append(msgs, newMsg)
			flush = newMsg.Severity >= fnpb.LogEntry_Severity_CRITICAL
		}

		// If there are still messages in the buffer, drain them out to batch them to a cap.
		// If there's a critical message, flush immeadiately.
		for len(w.buffer) > 0 && len(msgs) < batchSize && !flush {
			newMsg, ok := <-w.buffer
			if !ok {
				return errBuffClosed
			}
			msgs = append(msgs, newMsg)
			flush = newMsg.Severity >= fnpb.LogEntry_Severity_CRITICAL
		}

		list := &fnpb.LogEntry_List{
			LogEntries: msgs,
		}

		if err := client.Send(list); err != nil {
			if err == io.EOF {
				for _, msg := range msgs {
					(&log.Standard{}).Log(ctx, log.SevInfo, 0, msg.GetMessage())
				}
				return io.EOF
			}
			fmt.Fprintf(os.Stderr, "Failed to send %v messages. Messages follow error.\n %v", len(msgs), err)
			for _, msg := range msgs {
				fmt.Fprintf(os.Stderr, "Failed to send message: %v\n %v", err, msg.GetMessage())
			}
			return err
		}
	}
}
