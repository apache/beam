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

package periodic

import (
	"os"
	"testing"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/rtrackers/offsetrange"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/options/jobopts"
	_ "github.com/apache/beam/sdks/v2/go/pkg/beam/runners/prism"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestMain(m *testing.M) {
	// Since we force loopback with prism, avoid cross-compilation.
	f, _ := os.CreateTemp("", "dummy")
	*jobopts.WorkerBinary = f.Name()
	os.Exit(ptest.MainRetWithDefault(m, "prism"))
}

func TestSequence(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	sd := SequenceDefinition{
		Interval: time.Second,
		Start:    0,
		End:      time.Minute.Milliseconds(),
	}
	in := beam.Create(s, sd)
	out := Sequence(s, in)
	passert.Count(s, out, "SecondsInMinute", 60)
	ptest.RunAndValidate(t, p)
}

func TestImpulse(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	interval := time.Second
	start := time.Unix(0, 0)
	end := start.Add(time.Minute)
	out := Impulse(s, start, end, interval, false)
	passert.Count(s, out, "SecondsInMinute", 60)
	ptest.RunAndValidate(t, p)
}

func TestSize(t *testing.T) {
	sd := SequenceDefinition{
		Interval: 10 * time.Second,
		Start:    0,
		End:      1000 * time.Minute.Milliseconds(),
	}
	end := int64((1000 * time.Minute) / (10 * time.Second))

	sizeTests := []struct {
		now, startIndex, endIndex, want int64
	}{
		{100, 10, end, 0},
		{100, 9, end, 8},
		{100, 8, end, 16},
		{101, 9, end, 8},
		{10000, 0, end, 8 * 10000 / 10},
		{10000, 1002, 1003, 0},
		{10100, 1002, 1003, 8},
	}

	for _, test := range sizeTests {
		got := CalculateByteSizeOfSequence(
			time.Unix(test.now, 0),
			sd,
			offsetrange.Restriction{
				Start: int64(test.startIndex),
				End:   int64(test.endIndex),
			})
		if got != test.want {
			t.Errorf("TestBytes(%v, %v, %v) = %v, want %v",
				test.now, test.startIndex, test.endIndex, got, test.want)
		}
	}
}
