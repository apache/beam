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
