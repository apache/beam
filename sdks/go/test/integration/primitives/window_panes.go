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

package primitives

import (
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window/trigger"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/teststream"
)

func init() {
	register.Function3x0(panesFn)

	register.Emitter1[int]()
}

// panesFn is DoFn that simply emits the pane timing value.
func panesFn(pn beam.PaneInfo, value float64, emit func(int)) {
	emit(int(pn.Timing))
}

// Panes constructs a teststream and applies a pardo to get the pane timings.
func Panes(s beam.Scope) {
	s.Scope("increment")
	con := teststream.NewConfig()
	con.AddElements(1000, 1.0, 2.0, 3.0)
	con.AdvanceWatermark(11000)
	col := teststream.Create(s, con)
	windowSize := 10 * time.Second

	windowed := beam.WindowInto(s, window.NewFixedWindows(windowSize), col, []beam.WindowIntoOption{
		beam.Trigger(trigger.Always()),
	}...)
	sums := beam.ParDo(s, panesFn, windowed)
	sums = beam.WindowInto(s, window.NewGlobalWindows(), sums)
	passert.Count(s, sums, "number of firings", 3)
}
