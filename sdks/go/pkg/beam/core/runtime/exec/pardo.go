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
	"path"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/util/errorx"
)

// ParDo is a DoFn executor.
type ParDo struct {
	UID     UnitID
	Fn      *graph.DoFn
	Inbound []*graph.Inbound
	Side    []ReStream
	Out     []Node

	PID       string
	ready     bool
	sideinput []ReusableInput
	emitters  []ReusableEmitter
	extra     []interface{}

	status Status
	err    errorx.GuardedError
	ctx    context.Context
	inv    *invoker
}

func (n *ParDo) ID() UnitID {
	return n.UID
}

func (n *ParDo) Up(ctx context.Context) error {
	if n.status != Initializing {
		return fmt.Errorf("invalid status for pardo %v: %v, want Initializing", n.UID, n.status)
	}
	n.status = Up
	n.inv = newInvoker(n.Fn.ProcessElementFn())

	if _, err := InvokeWithoutEventTime(ctx, n.Fn.SetupFn(), nil); err != nil {
		return n.fail(err)
	}
	return nil
}

func (n *ParDo) StartBundle(ctx context.Context, id string, data DataManager) error {
	if n.status != Up {
		return fmt.Errorf("invalid status for pardo %v: %v, want Up", n.UID, n.status)
	}
	n.status = Active
	// Allocating contexts all the time is expensive, but we seldom re-write them,
	// and never accept modified contexts from users, so we will cache them per-bundle
	// per-unit, to avoid the constant allocation overhead.
	n.ctx = metrics.SetPTransformID(ctx, n.PID)

	if err := MultiStartBundle(n.ctx, id, data, n.Out...); err != nil {
		return n.fail(err)
	}

	// TODO(BEAM-3303) 12/12/2017: recalculate side inputs per bundle. The results may
	// not be valid in the presence of windowing.

	// NOTE(herohde) 12/13/2017: we require that side input is available on StartBundle.

	if err := n.initIfNeeded(); err != nil {
		return n.fail(err)
	}

	// TODO(BEAM-3303): what to set for StartBundle/FinishBundle emitter timestamp?

	if _, err := n.invokeDataFn(n.ctx, window.SingleGlobalWindow, mtime.ZeroTimestamp, n.Fn.StartBundleFn(), nil); err != nil {
		return n.fail(err)
	}
	return nil
}

func (n *ParDo) ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for pardo %v: %v, want Active", n.UID, n.status)
	}
	// If the function observes windows, we must invoke it for each window. The expected fast path
	// is that either there is a single window or the function doesn't observes windows.

	if !mustExplodeWindows(n.inv.fn, elm) {
		val, err := n.invokeProcessFn(n.ctx, elm.Windows, elm.Timestamp, &MainInput{Key: elm, Values: values})
		if err != nil {
			return n.fail(err)
		}

		// Forward direct output, if any. It is always a main output.
		if val != nil {
			return n.Out[0].ProcessElement(n.ctx, *val)
		}
	} else {
		for _, w := range elm.Windows {
			wElm := FullValue{Elm: elm.Elm, Elm2: elm.Elm2, Timestamp: elm.Timestamp, Windows: []typex.Window{w}}

			val, err := n.invokeProcessFn(n.ctx, wElm.Windows, wElm.Timestamp, &MainInput{Key: wElm, Values: values})
			if err != nil {
				return n.fail(err)
			}

			// Forward direct output, if any. It is always a main output.
			if val != nil {
				return n.Out[0].ProcessElement(n.ctx, *val)
			}
		}
	}
	return nil
}

// mustExplodeWindows returns true iif we need to call the function
// for each window.
func mustExplodeWindows(fn *funcx.Fn, elm FullValue) bool {
	if len(elm.Windows) < 2 {
		return false
	}
	_, explode := fn.Window()
	return explode
}

func (n *ParDo) FinishBundle(ctx context.Context) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for pardo %v: %v, want Active", n.UID, n.status)
	}
	n.status = Up
	n.inv.Reset()

	if _, err := n.invokeDataFn(n.ctx, window.SingleGlobalWindow, mtime.ZeroTimestamp, n.Fn.FinishBundleFn(), nil); err != nil {
		return n.fail(err)
	}
	if err := MultiFinishBundle(n.ctx, n.Out...); err != nil {
		return n.fail(err)
	}
	return nil
}

func (n *ParDo) Down(ctx context.Context) error {
	if n.status == Down {
		return n.err.Error()
	}
	n.status = Down

	if _, err := InvokeWithoutEventTime(ctx, n.Fn.TeardownFn(), nil); err != nil {
		n.err.TrySetError(err)
	}
	return n.err.Error()
}

func (n *ParDo) initIfNeeded() error {
	if n.ready {
		return nil
	}
	n.ready = true

	// Setup reusable side input and emitters. It's currently a requirement that data
	// processing methods consume the same side input/emitters.

	var err error
	n.sideinput, err = makeSideInputs(n.Fn.ProcessElementFn(), n.Inbound, n.Side)
	if err != nil {
		return n.fail(err)
	}
	n.emitters, err = makeEmitters(n.Fn.ProcessElementFn(), n.Out)
	if err != nil {
		return n.fail(err)
	}
	for _, s := range n.sideinput {
		n.extra = append(n.extra, s.Value())
	}
	for _, e := range n.emitters {
		n.extra = append(n.extra, e.Value())
	}
	return err
}

// invokeDataFn handle non-per element invocations.
func (n *ParDo) invokeDataFn(ctx context.Context, ws []typex.Window, ts typex.EventTime, fn *funcx.Fn, opt *MainInput) (*FullValue, error) {
	if fn == nil {
		return nil, nil
	}
	if err := n.preInvoke(ctx, ws, ts); err != nil {
		return nil, err
	}
	val, err := Invoke(ctx, ws, ts, fn, opt, n.extra...)
	if err != nil {
		return nil, err
	}
	if err := n.postInvoke(); err != nil {
		return nil, err
	}
	return val, nil
}

// invokeProcessFn handles the per element invocations
func (n *ParDo) invokeProcessFn(ctx context.Context, ws []typex.Window, ts typex.EventTime, opt *MainInput) (*FullValue, error) {
	if err := n.preInvoke(ctx, ws, ts); err != nil {
		return nil, err
	}
	val, err := n.inv.Invoke(ctx, ws, ts, opt, n.extra...)
	if err != nil {
		return nil, err
	}
	if err := n.postInvoke(); err != nil {
		return nil, err
	}
	return val, nil
}

func (n *ParDo) preInvoke(ctx context.Context, ws []typex.Window, ts typex.EventTime) error {
	for _, e := range n.emitters {
		if err := e.Init(ctx, ws, ts); err != nil {
			return err
		}
	}
	for _, s := range n.sideinput {
		if err := s.Init(); err != nil {
			return err
		}
	}
	return nil
}

func (n *ParDo) postInvoke() error {
	for _, s := range n.sideinput {
		if err := s.Reset(); err != nil {
			return err
		}
	}
	return nil
}

func (n *ParDo) fail(err error) error {
	n.status = Broken
	n.err.TrySetError(err)
	return err
}

func (n *ParDo) String() string {
	return fmt.Sprintf("ParDo[%v] Out:%v", path.Base(n.Fn.Name()), IDs(n.Out...))
}
