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

func TestMakeConfig(t *testing.T) {
	con := MakeConfig(coder.NewDouble())
	if con.ElmCoder.Kind != coder.Double {
		t.Fatalf("coder type is not correct, expected double, got %v", con.ElmCoder.Kind)
	}
	if len(con.Events) != 0 {
		t.Fatalf("config has too many elements, expected 0, got %v", len(con.Events))
	}
	if con.Endpoint.Url != "" {
		t.Errorf("config has URL endpoint when it should be empty")
	}
}

func TestAdvanceWatermark(t *testing.T) {
	con := MakeConfig(coder.NewDouble())
	con.AdvanceWatermark(500)
	if w := con.Watermark; w != 500 {
		t.Errorf("default watermark expected 500, got %v", w)
	}
	if len(con.Events) != 1 {
		t.Errorf("expected only 1 event in config, got %v", len(con.Events))
	}
	if eventWatermark := con.Events[0].GetWatermarkEvent().NewWatermark; eventWatermark != 500 {
		t.Errorf("expected watermark in event was 500, got %v", eventWatermark)
	}
}

func TestAdvanceWatermark_Bad(t *testing.T) {
	con := MakeConfig(coder.NewDouble())
	if errOne := con.AdvanceWatermark(500); errOne != nil {
		t.Fatalf("first advance watermark failed when it should have succeeded, got %v", errOne)
	}
	if errTwo := con.AdvanceWatermark(200); errTwo == nil {
		t.Errorf("second advance watermark succeeded when it should have failed")
	}
}

func TestAdvanceProcessingTime(t *testing.T) {
	con := MakeConfig(coder.NewDouble())
	con.AdvanceProcessingTime(100)
	if len(con.Events) != 1 {
		t.Fatalf("expected only 1 event in config, got %v", len(con.Events))
	}
	event := con.Events[0].GetProcessingTimeEvent()
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
		con := MakeConfig(tc.elementCoder)
		dec := beam.NewElementDecoder(tc.elementCoder.T.Type())
		for i, elements := range tc.elementGroups {
			if err := con.AddElements(100, elements...); err != nil {
				t.Fatalf("%v failed to add elements to config, got %v", tc.name, err)
			}
			for j, event := range con.Events[i].GetElementEvent().GetElements() {
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

func TestTestStream(t *testing.T) {
	_, s := beam.NewPipelineWithRoot()
	c := MakeConfig(coder.NewString())
	c.AddElements(100, "a", "b", "c")
	c.AdvanceWatermark(200)
	cols := TestStream(s, c)
	if len(cols) != 1 {
		t.Errorf("expected 1 PCollection, got %v", len(cols))
	}
}
