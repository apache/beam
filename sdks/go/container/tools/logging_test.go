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
	"errors"
	"log"
	"strings"
	"testing"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
)

type logCatcher struct {
	msgs []*fnpb.LogEntry_List
	err  error
}

func (l *logCatcher) Send(msg *fnpb.LogEntry_List) error {
	l.msgs = append(l.msgs, msg)
	return l.err
}

func (l *logCatcher) CloseSend() error {
	return nil
}

func TestLogger(t *testing.T) {
	ctx := context.Background()
	t.Run("SuccessfulLogging", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}

		l.Printf(ctx, "foo %v", "bar")

		received := catcher.msgs[0].GetLogEntries()[0]

		if got, want := received.Message, "foo bar"; got != want {
			t.Errorf("l.Printf(\"foo %%v\", \"bar\"): got message %q, want %q", got, want)
		}

		if got, want := received.Severity, fnpb.LogEntry_Severity_DEBUG; got != want {
			t.Errorf("l.Printf(\"foo %%v\", \"bar\"): got severity %v, want %v", got, want)
		}
	})
	t.Run("SuccessfulLoggingAtError", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}

		l.Errorf(ctx, "failed to install dependency %v", "bar")

		received := catcher.msgs[0].GetLogEntries()[0]

		if got, want := received.Message, "failed to install dependency bar"; got != want {
			t.Errorf("l.Printf(\"foo %%v\", \"bar\"): got message %q, want %q", got, want)
		}

		if got, want := received.Severity, fnpb.LogEntry_Severity_ERROR; got != want {
			t.Errorf("l.Errorf(\"failed to install dependency %%v\", \"bar\"): got severity %v, want %v", got, want)
		}
	})
	t.Run("backup path", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}

		// Validate error outputs.
		var buf bytes.Buffer
		ll := log.Default()
		ll.SetOutput(&buf)

		catcher.err = errors.New("test error")
		wantMsg := "checking for error?"
		l.Printf(ctx, wantMsg)

		line, err := buf.ReadString('\n')
		if err != nil {
			t.Errorf("unexpected error reading form backup log buffer: %v", err)
		}

		if got, want := line, "boot.go: error logging message over FnAPI"; !strings.Contains(got, want) {
			t.Errorf("backup log buffer didn't contain expected log, got %q, want it to contain %q", got, want)
		}
		if got, want := line, "test error"; !strings.Contains(got, want) {
			t.Errorf("backup log buffer didn't contain expected log, got %q, want it to contain %q", got, want)
		}

		line, err = buf.ReadString('\n')
		if err != nil {
			t.Errorf("unexpected error reading form backup log buffer: %v", err)
		}

		if got, want := line, wantMsg; !strings.Contains(got, want) {
			t.Errorf("backup log buffer didn't contain the message, got %q, want it to contain %q", got, want)
		}
	})

	t.Run("no endpoint", func(t *testing.T) {
		l := &Logger{}

		var buf bytes.Buffer
		ll := log.Default()
		ll.SetOutput(&buf)

		l.Printf(ctx, "trying to log")

		line, err := buf.ReadString('\n')
		if err != nil {
			t.Errorf("unexpected error reading form backup log buffer: %v", err)
		}
		if got, want := line, "no logging endpoint set"; !strings.Contains(got, want) {
			t.Errorf("backup log buffer didn't contain expected error, got %q, want it to contain %q", got, want)
		}

	})

}
