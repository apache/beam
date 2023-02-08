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

package worker

import (
	"testing"
)

func TestWorker_New(t *testing.T) {
	w := New("test")
	if got, want := w.ID, "test"; got != want {
		t.Errorf("New(%q) = %v, want %v", want, got, want)
	}
}

func TestWorker_NextInst(t *testing.T) {
	w := New("test")

	instIDs := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		instIDs[w.NextInst()] = struct{}{}
	}
	if got, want := len(instIDs), 100; got != want {
		t.Errorf("calling w.NextInst() got %v unique ids, want %v", got, want)
	}
}

func TestWorker_NextBund(t *testing.T) {
	w := New("test")

	stageIDs := map[string]struct{}{}
	for i := 0; i < 100; i++ {
		stageIDs[w.NextStage()] = struct{}{}
	}
	if got, want := len(stageIDs), 100; got != want {
		t.Errorf("calling w.NextInst() got %v unique ids, want %v", got, want)
	}
}
