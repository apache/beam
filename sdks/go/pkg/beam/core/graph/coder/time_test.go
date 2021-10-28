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

package coder

import (
	"bytes"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
)

func TestEncodeDecodeEventTime(t *testing.T) {
	tests := []struct {
		time mtime.Time
	}{
		{time: mtime.ZeroTimestamp},
		{time: mtime.MinTimestamp},
		{time: mtime.MaxTimestamp},
		{time: mtime.Now()},
		{time: mtime.Time(1257894000000)},
		{time: mtime.Time(1257894000000000)},
	}

	for _, test := range tests {
		var buf bytes.Buffer
		err := EncodeEventTime(test.time, &buf)
		if err != nil {
			t.Fatalf("EncodeEventTime(%v) failed: %v", test.time, err)
		}
		t.Logf("Encoded %v to %v", test.time, buf.Bytes())

		if len(buf.Bytes()) != 8 {
			t.Errorf("EncodeEventTime(%v) = %v, want %v", test.time, len(buf.Bytes()), 8)
		}

		actual, err := DecodeEventTime(&buf)
		if err != nil {
			t.Fatalf("DecodeEventTime(<%v>) failed: %v", test.time, err)
		}
		if (mtime.Time)(actual) != test.time {
			t.Errorf("DecodeEventTime(<%v>) = %v, want %v", test.time, actual, test.time)
		}
	}
}
