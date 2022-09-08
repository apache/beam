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

package harnessopts

import (
	"testing"

	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/harness" // Imports the cache hook
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
)

func TestHeapDumpFrequency(t *testing.T) {
	err := HeapDumpFrequency(10, 30)
	if err != nil {
		t.Errorf("HeapDumpFrequency failed when it should have succeeded, got %v", err)
	}
	ok, opts := hooks.IsEnabled(diagnosticsHook)
	if !ok {
		t.Fatalf("HeapDumpFrequency hook is not enabled")
	}
	if len(opts) != 2 {
		t.Errorf("num opts mismatch, got %v, want 2", len(opts))
	}
	if opts[0] != "10" {
		t.Errorf("cache size option mismatch, got %v, want %v", opts[0], 10)
	}
	if opts[1] != "30" {
		t.Errorf("cache size option mismatch, got %v, want %v", opts[1], 30)
	}
}

func TestHeapDumpFrequency_Bad(t *testing.T) {
	err := HeapDumpFrequency(5, 4)
	if err == nil {
		t.Errorf("HeapDumpFrequency succeeded when it should have failed")
	}
}
