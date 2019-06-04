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
	"context"
	"fmt"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// WindowInto places each element in one or more windows.
type WindowInto struct {
	UID UnitID
	Fn  *window.Fn
	Out Node
}

func (w *WindowInto) ID() UnitID {
	return w.UID
}

func (w *WindowInto) Up(ctx context.Context) error {
	return nil
}

func (w *WindowInto) StartBundle(ctx context.Context, id string, data DataContext) error {
	return w.Out.StartBundle(ctx, id, data)
}

func (w *WindowInto) ProcessElement(ctx context.Context, elm *FullValue, values ...ReStream) error {
	windowed := &FullValue{
		Windows:   assignWindows(w.Fn, elm.Timestamp),
		Timestamp: elm.Timestamp,
		Elm:       elm.Elm,
		Elm2:      elm.Elm2,
	}
	return w.Out.ProcessElement(ctx, windowed, values...)
}

func assignWindows(wfn *window.Fn, ts typex.EventTime) []typex.Window {
	switch wfn.Kind {
	case window.GlobalWindows:
		return window.SingleGlobalWindow

	case window.FixedWindows:
		start := ts - (ts.Add(wfn.Size) % mtime.FromDuration(wfn.Size))
		end := mtime.Min(start.Add(wfn.Size), mtime.EndOfGlobalWindowTime.Add(time.Millisecond))
		return []typex.Window{window.IntervalWindow{Start: start, End: end}}

	case window.SlidingWindows:
		var ret []typex.Window

		period := mtime.FromDuration(wfn.Period)
		lastStart := ts - (ts.Add(wfn.Size) % period)
		for start := lastStart; start > ts.Subtract(wfn.Size); start -= period {
			ret = append(ret, window.IntervalWindow{Start: start, End: start.Add(wfn.Size)})
		}
		return ret

	default:
		panic(fmt.Sprintf("Unexpected window fn: %v", wfn))
	}
}

func (w *WindowInto) FinishBundle(ctx context.Context) error {
	return w.Out.FinishBundle(ctx)
}

func (w *WindowInto) Down(ctx context.Context) error {
	return nil
}

func (w *WindowInto) String() string {
	return fmt.Sprintf("WindowInto[%v]. Out:%v", w.Fn, w.Out.ID())
}
