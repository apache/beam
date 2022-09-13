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

package exec

import (
	"bytes"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func equalTimers(a, b typex.TimerMap) bool {
	return a.Key == b.Key && a.Tag == b.Tag && a.FireTimestamp == b.FireTimestamp && a.Clear == b.Clear
}

func TestTimerEncodingDecoding(t *testing.T) {
	wec := MakeWindowEncoder(window.NewGlobalWindows().Coder())
	win, err := EncodeWindow(wec, window.SingleGlobalWindow[0])
	tc := coder.NewT(coder.NewString(), window.NewGlobalWindows().Coder())
	ec := MakeElementEncoder(coder.SkipW(tc))
	dec := MakeElementDecoder(coder.SkipW(tc))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		tm     typex.TimerMap
		result bool
	}{
		{
			name: "all set fields",
			tm: typex.TimerMap{
				Key:           "Basic",
				Tag:           "first",
				Windows:       win,
				Clear:         false,
				FireTimestamp: time.Now().UnixMilli(),
			},
			result: true,
		},
		{
			name: "without tag",
			tm: typex.TimerMap{
				Key:           "Basic",
				Tag:           "",
				Windows:       win,
				Clear:         false,
				FireTimestamp: time.Now().UnixMilli(),
			},
			result: true,
		},
		{
			name: "with clear set",
			tm: typex.TimerMap{
				Key:           "Basic",
				Tag:           "first",
				Windows:       win,
				Clear:         true,
				FireTimestamp: time.Now().UnixMilli(),
			},
			result: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fv := FullValue{Elm: test.tm}
			var buf bytes.Buffer
			err = ec.Encode(&fv, &buf)
			if err != nil {
				t.Fatalf("error encoding timer: %#v, got: %v", test.tm, err)
			}

			gotFv, err := dec.Decode(&buf)
			if err != nil {
				t.Fatalf("failed to decode timer, got %v", err)
			}

			if got, want := gotFv.Elm.(typex.TimerMap), test.tm; test.result != equalTimers(got, want) {
				t.Errorf("got timer %v, want %v", got, want)
			}
		})
	}

}
