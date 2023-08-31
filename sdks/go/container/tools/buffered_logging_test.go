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

	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
)

func TestBufferedLogger(t *testing.T) {
	ctx := context.Background()

	t.Run("write", func(t *testing.T) {
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

		messages := []string{"foo", "bar", "baz"}

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

		received := catcher.msgs[0].GetLogEntries()

		for i, message := range received {
			if got, want := message.Message, messages[i]; got != want {
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

		messages := []string{"foo", "bar", "baz"}

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

		received := catcher.msgs[0].GetLogEntries()

		for i, message := range received {
			if got, want := message.Message, messages[i]; got != want {
				t.Errorf("got message %q, want %q", got, want)
			}

			if got, want := message.Severity, fnpb.LogEntry_Severity_ERROR; got != want {
				t.Errorf("got severity %v, want %v", got, want)
			}
		}
	})
}
