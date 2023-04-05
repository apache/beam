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

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
)

func equalTimers(a, b typex.TimerMap) bool {
	return a.Key == b.Key && a.Tag == b.Tag && (a.FireTimestamp) == b.FireTimestamp && a.Clear == b.Clear
}

func TestTimerEncodingDecoding(t *testing.T) {
	tc := coder.NewT(coder.NewString(), window.NewGlobalWindows().Coder())
	ec := MakeElementEncoder(coder.SkipW(tc))
	dec := MakeElementDecoder(coder.SkipW(tc))

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
				Windows:       window.SingleGlobalWindow,
				Clear:         false,
				FireTimestamp: mtime.Now(),
			},
			result: true,
		},
		{
			name: "without tag",
			tm: typex.TimerMap{
				Key:           "Basic",
				Tag:           "",
				Windows:       window.SingleGlobalWindow,
				Clear:         false,
				FireTimestamp: mtime.Now(),
			},
			result: true,
		},
		{
			name: "with clear set",
			tm: typex.TimerMap{
				Key:           "Basic",
				Tag:           "first",
				Windows:       window.SingleGlobalWindow,
				Clear:         true,
				FireTimestamp: mtime.Now(),
			},
			result: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fv := FullValue{Elm: test.tm}
			var buf bytes.Buffer
			err := ec.Encode(&fv, &buf)
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
