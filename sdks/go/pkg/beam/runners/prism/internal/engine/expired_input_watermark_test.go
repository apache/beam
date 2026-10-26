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

// Regression for expired input hidden by pending later windows.
package engine

import (
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
)

func TestAggregateExpiredInputWithPendingLaterWindow(t *testing.T) {
	for _, tc := range []struct {
		name         string
		upstream     mtime.Time
		secondInput  mtime.Time
		wantAccepted int
	}{
		{"expiredWithOutputHeld", 16000, 16000, 0},
		{"withinLateness", 14000, 16000, 1},
		{"atCompletion", 14999, 16000, 1},
		{"pastCompletion", 15000, 16000, 0},
		{"minimumOfUpstreamInputs", 16000, 14000, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			em := NewElementManager(Config{})
			em.AddStage("aggregate", []string{"input", "other"}, []string{"output"}, nil)
			em.StageAggregates("aggregate", WinStrat{
				AllowedLateness: 5 * time.Second,
				Accumulating:    true,
				Trigger:         &TriggerNever{},
			})
			ss := em.stages["aggregate"]
			ss.input = 11000
			ss.output = 11000
			ss.updateUpstreamWatermark("input", tc.upstream)
			ss.updateUpstreamWatermark("other", tc.secondInput)
			// A later window pins internal progress, but cannot permit expired A.
			ss.pendingByKeys = map[string]*dataAndTimers{
				"B": {elements: elementHeap{{
					window:    window.IntervalWindow{Start: 10000, End: 20000},
					timestamp: 11000,
					keyBytes:  []byte("B"),
					elmBytes:  []byte{4},
				}}},
			}
			got := ss.AddPending(em, []element{{
				window:    window.IntervalWindow{Start: 0, End: 10000},
				timestamp: 5000,
				keyBytes:  []byte("A"),
				elmBytes:  []byte{100},
			}})
			if got != tc.wantAccepted {
				t.Fatalf("accepted = %v, want %v", got, tc.wantAccepted)
			}
			if len(ss.pendingByKeys["B"].elements) != 1 {
				t.Fatal("unrelated later-window input was lost")
			}
			if ss.input != 11000 || ss.output != 11000 {
				t.Fatal("input admission changed internal watermark holds")
			}
		})
	}
}
