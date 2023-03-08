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
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/hooks"
)

func TestSampleInterval(t *testing.T) {
	err := SampleInterval(100 * time.Millisecond)
	if err != nil {
		t.Error(err)
	}
	ok, opts := hooks.IsEnabled(samplePeriodHook)
	if !ok {
		t.Fatalf("Sample Period hook is not enabled")
	}
	if len(opts) != 1 {
		t.Errorf("num opts mismatch, got %v, want 1", len(opts))
	}
}

func TestSampleInterval_Bad(t *testing.T) {
	err := SampleInterval(time.Microsecond)
	if err == nil {
		t.Error("sample period of less than 1ms worked when it shouldn't.")
	}
}
