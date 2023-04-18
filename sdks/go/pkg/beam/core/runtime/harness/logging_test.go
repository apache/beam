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
	"errors"
	"strings"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/fnexecution_v1"
)

func TestLogger(t *testing.T) {
	ch := make(chan *fnpb.LogEntry, 1)
	l := logger{out: ch}

	instID := "INST"
	transformID := "TRANSFORM"
	ctx := metrics.SetBundleID(context.Background(), instID)
	ctx = metrics.SetPTransformID(ctx, transformID)
	msg := "expectedMessage"
	l.Log(ctx, log.SevInfo, 0, msg)

	e := <-ch

	if got, want := e.GetInstructionId(), instID; got != want {
		t.Errorf("incorrect InstructionID: got %v, want %v", got, want)
	}
	if got, want := e.GetTransformId(), transformID; got != want {
		t.Errorf("incorrect TransformID: got %v, want %v", got, want)
	}
	if got, want := e.GetMessage(), msg; got != want {
		t.Errorf("incorrect Message: got %v, want %v", got, want)
	}
	// This check will fail if the imports change.
	if got, want := e.GetLogLocation(), "logging_test.go:38"; !strings.HasSuffix(got, want) {
		t.Errorf("incorrect LogLocation: got %v, want suffix %v", got, want)
	}
	if got, want := e.GetSeverity(), fnpb.LogEntry_Severity_INFO; got != want {
		t.Errorf("incorrect Severity: got %v, want %v", got, want)
	}
}

type logCacher struct {
	logs []*fnpb.LogEntry_List
}

func (l *logCacher) Send(v *fnpb.LogEntry_List) error {
	l.logs = append(l.logs, v)
	return nil
}

func TestLogger_connect(t *testing.T) {
	buf := make(chan *fnpb.LogEntry, 20)
	l := logger{out: buf}
	w := &remoteWriter{buffer: buf, endpoint: "dummyEndpoint"}

	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(cancelFn)

	cacher := &logCacher{}

	// Batch up several messages.
	l.Log(ctx, log.SevDebug, 0, "debug")
	l.Log(ctx, log.SevInfo, 0, "info")
	l.Log(ctx, log.SevWarn, 0, "warn")
	l.Log(ctx, log.SevError, 0, "error")
	l.Log(ctx, log.SevFatal, 0, "fatal") // batch flushed from fatal
	l.Log(ctx, log.SevFatal, 0, "fatal") // immeadiately flushed from fatal
	l.Log(ctx, log.SevFatal, 0, "fatal") // immeadiately flushed from fatal
	l.Log(ctx, log.SevInfo, 0, "info")
	l.Log(ctx, log.SevInfo, 0, "info")
	l.Log(ctx, log.SevInfo, 0, "info") // batched since nothing else in the buffer
	close(buf)

	err := w.connect(ctx, func(ctx context.Context) (logSender, func(), error) {
		return cacher, func() {}, nil
	})

	if got, want := err, errBuffClosed; !errors.Is(got, want) {
		t.Errorf("connect error: got %v, want %v", got, want)
	}

	if got, want := len(cacher.logs), 4; got != want {
		t.Errorf("batching error: got %v batches, want %v", got, want)
	}

	var count int
	for _, batch := range cacher.logs {
		count += len(batch.LogEntries)
	}

	if got, want := count, 10; got != want {
		t.Errorf("missing messages: got %v, want %v", got, want)
	}
}
