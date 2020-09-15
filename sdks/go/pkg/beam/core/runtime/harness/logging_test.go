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
	"strings"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/log"
	fnpb "github.com/apache/beam/sdks/go/pkg/beam/model/fnexecution_v1"
)

func TestLogger(t *testing.T) {
	ch := make(chan *fnpb.LogEntry, 1)
	l := logger{out: ch}

	instID := "INST"
	ctx := setInstID(context.Background(), instructionID(instID))
	msg := "expectedMessage"
	l.Log(ctx, log.SevInfo, 0, msg)

	e := <-ch

	if got, want := e.GetInstructionId(), instID; got != want {
		t.Errorf("incorrect InstructionID: got %v, want %v", got, want)
	}
	if got, want := e.GetMessage(), msg; got != want {
		t.Errorf("incorrect Message: got %v, want %v", got, want)
	}
	// This check will fail if the imports change.
	if got, want := e.GetLogLocation(), "logging_test.go:34"; !strings.HasSuffix(got, want) {
		t.Errorf("incorrect LogLocation: got %v, want suffix %v", got, want)
	}
	if got, want := e.GetSeverity(), fnpb.LogEntry_Severity_INFO; got != want {
		t.Errorf("incorrect Severity: got %v, want %v", got, want)
	}
}
