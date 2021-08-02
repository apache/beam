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

package teststream

import (
	"bytes"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
)

func TestNewConfig(t *testing.T) {
	con := NewConfig()
	if con.elmCoder != nil {
		t.Fatalf("coder is not correct, expected nil, got %v", con.elmCoder.Kind)
	}
	if len(con.events) != 0 {
		t.Fatalf("config has too many elements, expected 0, got %v", len(con.events))
	}
	if con.endpoint.Url != "" {
		t.Errorf("config has URL endpoint when it should be empty")
	}
}

func TestAdvanceWatermark(t *testing.T) {
	con := NewConfig()
	con.AdvanceWatermark(500)
	if w := con.watermark; w != 500 {
		t.Errorf("default watermark expected 500, got %v", w)
	}
	if len(con.events) != 1 {
		t.Errorf("expected only 1 event in config, got %v", len(con.events))
	}
	if eventWatermark := con.events[0].GetWatermarkEvent().NewWatermark; eventWatermark != 500 {
		t.Errorf("expected watermark in event was 500, got %v", eventWatermark)
	}
}

func TestAdvanceWatermark_Bad(t *testing.T) {
	con := NewConfig()
	if errOne := con.AdvanceWatermark(500); errOne != nil {
		t.Fatalf("first advance watermark failed when it should have succeeded, got %v", errOne)
	}
	if errTwo := con.AdvanceWatermark(200); errTwo == nil {
		t.Errorf("second advance watermark succeeded when it should have failed")
	}
}

func TestAdvanceProcessingTime(t *testing.T) {
	con := NewConfig()
	con.AdvanceProcessingTime(100)
	if len(con.events) != 1 {
		t.Fatalf("expected only 1 event in config, got %v", len(con.events))
	}
	event := con.events[0].GetProcessingTimeEvent()
	if event.GetAdvanceDuration() != 100 {
		t.Errorf("expected duration of 100, got %v", event.GetAdvanceDuration())
	}
}

func TestAddElements(t *testing.T) {
	tests := []struct {
		name          string
		elementGroups [][]interface{}
		elementCoder  *coder.Coder
	}{
		{
			"bools",
			[][]interface{}{{true, false}},
			coder.NewBool(),
		},
		{
			"multiple bools",
			[][]interface{}{{true, false}, {true, false}},
			coder.NewBool(),
		},
		{
			"strings",
			[][]interface{}{{"test", "other test"}},
			coder.NewString(),
		},
		{
			"doubles",
			[][]interface{}{{1.1, 2.2, 3.3}},
			coder.NewDouble(),
		},
	}
	for _, tc := range tests {
		con := NewConfig()
		dec := beam.NewElementDecoder(tc.elementCoder.T.Type())
		for i, elements := range tc.elementGroups {
			if err := con.AddElements(100, elements...); err != nil {
				t.Fatalf("%v failed to add elements to config, got %v", tc.name, err)
			}
			for j, event := range con.events[i].GetElementEvent().GetElements() {
				buf := bytes.NewReader(event.GetEncodedElement())
				val, err := dec.Decode(buf)
				if err != nil {
					t.Errorf("%v, error decoding element, got %v", tc.name, err)
				}
				if val != elements[j] {
					t.Errorf("%v added element mismatch, expected %v, got %v", tc.name, elements[j], val)
				}
			}
		}
	}
}
