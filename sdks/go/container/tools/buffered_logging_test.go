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
	"testing"
	"time"

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
)

func getAllLogEntries(catcher *logCatcher) []*fnpb.LogEntry {
	var entries []*fnpb.LogEntry
	for _, list := range catcher.msgs {
		entries = append(entries, list.GetLogEntries()...)
	}
	return entries
}

func TestBufferedLogger(t *testing.T) {
	ctx := context.Background()

	t.Run("write", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}
		bl := NewBufferedLogger(l)

		message := []byte("test message\n")
		n, err := bl.Write(message)
		if err != nil {
			t.Errorf("got error %v", err)
		}
		if got, want := n, len(message); got != want {
			t.Errorf("got %d bytes written, want %d", got, want)
		}
		if got, want := bl.logs[0], "test message"; got != want {
			t.Errorf("got message %q, want %q", got, want)
		}
	})

	t.Run("flush single message", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}
		bl := NewBufferedLogger(l)

		message := []byte("test message")
		n, err := bl.Write(message)

		if err != nil {
			t.Errorf("got error %v", err)
		}
		if got, want := n, len(message); got != want {
			t.Errorf("got %d bytes written, want %d", got, want)
		}

		bl.FlushAtDebug(ctx)

		received := catcher.msgs[0].GetLogEntries()[0]

		if got, want := received.Message, "test message"; got != want {
			t.Errorf("got message %q, want %q", got, want)
		}

		if got, want := received.Severity, fnpb.LogEntry_Severity_DEBUG; got != want {
			t.Errorf("got severity %v, want %v", got, want)
		}
	})

	t.Run("flush multiple messages", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}
		bl := NewBufferedLogger(l)

		messages := []string{"foo\n", "bar\n", "baz\n"}
		expected := []string{"foo", "bar", "baz"}

		for _, message := range messages {
			messBytes := []byte(message)
			n, err := bl.Write(messBytes)

			if err != nil {
				t.Errorf("got error %v", err)
			}
			if got, want := n, len(messBytes); got != want {
				t.Errorf("got %d bytes written, want %d", got, want)
			}
		}

		bl.FlushAtDebug(ctx)

		received := getAllLogEntries(catcher)

		if got, want := len(received), len(expected); got != want {
			t.Fatalf("expected %d log entries received, got %d", want, got)
		}

		for i, message := range received {
			if got, want := message.Message, expected[i]; got != want {
				t.Errorf("got message %q, want %q", got, want)
			}

			if got, want := message.Severity, fnpb.LogEntry_Severity_DEBUG; got != want {
				t.Errorf("got severity %v, want %v", got, want)
			}
		}
	})

	t.Run("flush single message at error", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}
		bl := NewBufferedLogger(l)

		message := []byte("test error")
		n, err := bl.Write(message)

		if err != nil {
			t.Errorf("got error %v", err)
		}
		if got, want := n, len(message); got != want {
			t.Errorf("got %d bytes written, want %d", got, want)
		}

		bl.FlushAtError(ctx)

		received := catcher.msgs[0].GetLogEntries()[0]

		if got, want := received.Message, "test error"; got != want {
			t.Errorf("got message %q, want %q", got, want)
		}

		if got, want := received.Severity, fnpb.LogEntry_Severity_ERROR; got != want {
			t.Errorf("got severity %v, want %v", got, want)
		}
	})

	t.Run("flush multiple messages at error", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}
		bl := NewBufferedLogger(l)

		messages := []string{"foo\n", "bar\n", "baz\n"}
		expected := []string{"foo", "bar", "baz"}

		for _, message := range messages {
			messBytes := []byte(message)
			n, err := bl.Write(messBytes)

			if err != nil {
				t.Errorf("got error %v", err)
			}
			if got, want := n, len(messBytes); got != want {
				t.Errorf("got %d bytes written, want %d", got, want)
			}
		}

		bl.FlushAtError(ctx)

		received := getAllLogEntries(catcher)

		if got, want := len(received), len(expected); got != want {
			t.Fatalf("expected %d log entries received, got %d", want, got)
		}

		for i, message := range received {
			if got, want := message.Message, expected[i]; got != want {
				t.Errorf("got message %q, want %q", got, want)
			}

			if got, want := message.Severity, fnpb.LogEntry_Severity_ERROR; got != want {
				t.Errorf("got severity %v, want %v", got, want)
			}
		}
	})

	t.Run("direct print", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}
		bl := NewBufferedLogger(l)

		bl.Printf(ctx, "foo %v", "bar")

		received := catcher.msgs[0].GetLogEntries()[0]

		if got, want := received.Message, "foo bar"; got != want {
			t.Errorf("l.Printf(\"foo %%v\", \"bar\"): got message %q, want %q", got, want)
		}

		if got, want := received.Severity, fnpb.LogEntry_Severity_DEBUG; got != want {
			t.Errorf("l.Printf(\"foo %%v\", \"bar\"): got severity %v, want %v", got, want)
		}
	})

	t.Run("debug flush at interval", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}
		interval := 5 * time.Second
		bl := NewBufferedLoggerWithFlushInterval(context.Background(), l, interval)

		startTime := time.Now()
		bl.now = func() time.Time { return startTime }

		messages := []string{"foo\n", "bar\n"}
		expected := []string{"foo", "bar"}

		for i, message := range messages {
			if i > 1 {
				bl.now = func() time.Time { return startTime.Add(6 * time.Second) }
			}
			messBytes := []byte(message)
			n, err := bl.Write(messBytes)

			if err != nil {
				t.Errorf("got error %v", err)
			}
			if got, want := n, len(messBytes); got != want {
				t.Errorf("got %d bytes written, want %d", got, want)
			}
		}

		lastMessage := "baz\n"
		expected = append(expected, "baz")
		bl.now = func() time.Time { return startTime.Add(6 * time.Second) }
		messBytes := []byte(lastMessage)
		n, err := bl.Write(messBytes)

		if err != nil {
			t.Errorf("got error %v", err)
		}
		if got, want := n, len(messBytes); got != want {
			t.Errorf("got %d bytes written, want %d", got, want)
		}

		// Type should have auto-flushed at debug after the third message
		received := getAllLogEntries(catcher)

		if got, want := len(received), len(expected); got != want {
			t.Fatalf("expected %d log entries received, got %d", want, got)
		}

		for i, message := range received {
			if got, want := message.Message, expected[i]; got != want {
				t.Errorf("got message %q, want %q", got, want)
			}

			if got, want := message.Severity, fnpb.LogEntry_Severity_DEBUG; got != want {
				t.Errorf("got severity %v, want %v", got, want)
			}
		}
	})

	t.Run("partial write splitting", func(t *testing.T) {
		catcher := &logCatcher{}
		l := &Logger{client: catcher}
		bl := NewBufferedLogger(l)

		// Write a partial line
		n, err := bl.Write([]byte("hello "))
		if err != nil {
			t.Errorf("got error %v", err)
		}
		if n != 6 {
			t.Errorf("got %d, want 6", n)
		}
		if len(bl.logs) != 0 {
			t.Errorf("expected no logs buffered yet, got %d", len(bl.logs))
		}

		// Write remainder and a second line
		n, err = bl.Write([]byte("world\nline2\npartial"))
		if err != nil {
			t.Errorf("got error %v", err)
		}
		if n != 19 {
			t.Errorf("got %d, want 19", n)
		}

		if got, want := len(bl.logs), 2; got != want {
			t.Errorf("expected 2 logs buffered, got %d", got)
		}
		if got, want := bl.logs[0], "hello world"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}
		if got, want := bl.logs[1], "line2"; got != want {
			t.Errorf("got %q, want %q", got, want)
		}

		// Flush should flush the final partial message
		bl.FlushAtDebug(ctx)
		received := getAllLogEntries(catcher)
		if got, want := len(received), 3; got != want {
			t.Fatalf("expected 3 log entries received, got %d", got)
		}
		if got, want := received[0].Message, "hello world"; got != want {
			t.Errorf("got message %q, want %q", got, want)
		}
		if got, want := received[1].Message, "line2"; got != want {
			t.Errorf("got message %q, want %q", got, want)
		}
		if got, want := received[2].Message, "partial"; got != want {
			t.Errorf("got message %q, want %q", got, want)
		}
	})
}
