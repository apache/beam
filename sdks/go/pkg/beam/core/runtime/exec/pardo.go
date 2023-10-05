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
	goerrors "errors"
	"fmt"
	"io"
	"path"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/timers"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/errorx"
)

// ParDo is a DoFn executor.
type ParDo struct {
	UID          UnitID
	Fn           *graph.DoFn
	Inbound      []*graph.Inbound
	Side         []SideInputAdapter
	UState       UserStateAdapter
	TimerTracker *userTimerAdapter
	Out          []Node

	PID      string
	emitters []ReusableEmitter
	ctx      context.Context
	inv      *invoker
	bf       *bundleFinalizer
	we       sdf.WatermarkEstimator

	onTimerInvoker *invoker
	timerManager   DataManager
	reader         StateReader
	cache          *cacheElm

	status Status
	err    errorx.GuardedError

	states *metrics.PTransformState
}

// GetPID returns the PTransformID for this ParDo.
func (n *ParDo) GetPID() string {
	return n.PID
}

// cacheElm holds per-window cached information about side input.
type cacheElm struct {
	key       typex.Window
	sideinput []ReusableInput
	extra     []any
}

// ID returns the UnitID for this ParDo.
func (n *ParDo) ID() UnitID {
	return n.UID
}

// HasOnTimer returns if this ParDo wraps a DoFn that has an OnTimer method.
func (n *ParDo) HasOnTimer() bool {
	return n.TimerTracker != nil
}

// Up initializes this ParDo and does one-time DoFn setup.
func (n *ParDo) Up(ctx context.Context) error {
	if n.status != Initializing {
		return errors.Errorf("invalid status for pardo %v: %v, want Initializing", n.UID, n.status)
	}
	n.status = Up
	n.inv = newInvoker(n.Fn.ProcessElementFn())
	if fn, ok := n.Fn.OnTimerFn(); ok {
		n.onTimerInvoker = newInvoker(fn)
	}

	n.states = metrics.NewPTransformState(n.PID)

	// We can't cache the context during Setup since it runs only once per bundle.
	// Subsequent bundles might run this same node, and the context here would be
	// incorrectly refering to the older bundleId.
	setupCtx := metrics.SetPTransformID(ctx, n.PID)
	if _, err := InvokeWithOptsWithoutEventTime(setupCtx, n.Fn.SetupFn(), InvokeOpts{}); err != nil {
		return n.fail(err)
	}

	emitters, err := makeEmitters(n.Fn.ProcessElementFn(), n.Out)
	if err != nil {
		return n.fail(err)
	}
	n.emitters = emitters
	return nil
}

func (n *ParDo) AttachFinalizer(bf *bundleFinalizer) {
	n.bf = bf
}

// StartBundle does pre-bundle processing operation for the DoFn.
func (n *ParDo) StartBundle(ctx context.Context, id string, data DataContext) error {
	if n.status != Up {
		return errors.Errorf("invalid status for pardo %v: %v, want Up", n.UID, n.status)
	}
	n.status = Active
	n.reader = data.State
	n.timerManager = data.Data
	// Allocating contexts all the time is expensive, but we seldom re-write them,
	// and never accept modified contexts from users, so we will cache them per-bundle
	// per-unit, to avoid the constant allocation overhead.
	n.ctx = metrics.SetPTransformID(ctx, n.PID)

	n.states.Set(n.ctx, metrics.StartBundle)

	if err := MultiStartBundle(n.ctx, id, data, n.Out...); err != nil {
		return n.fail(err)
	}

	// TODO(BEAM-3303): what to set for StartBundle/FinishBundle window and emitter timestamp?

	if _, err := n.invokeDataFn(n.ctx, typex.NoFiringPane(), window.SingleGlobalWindow, mtime.ZeroTimestamp, n.Fn.StartBundleFn(), nil); err != nil {
		return n.fail(err)
	}
	return nil
}

// ProcessElement processes each parallel element with the DoFn.
func (n *ParDo) ProcessElement(_ context.Context, elm *FullValue, values ...ReStream) error {
	if n.status != Active {
		return errors.Errorf("invalid status for pardo %v: %v, want Active", n.UID, n.status)
	}

	n.states.Set(n.ctx, metrics.ProcessBundle)

	return n.processMainInput(&MainInput{Key: *elm, Values: values})
}

// processMainInput processes an element that has been converted into a
// MainInput. Splitting this away from ProcessElement allows other nodes to wrap
// a ParDo's ProcessElement functionality with their own construction of
// MainInputs.
func (n *ParDo) processMainInput(mainIn *MainInput) error {
	n.TimerTracker.SetCurrentKey(mainIn)
	elm := &mainIn.Key

	// If the function observes windows or uses per window state, we must invoke it for each window.
	// The expected fast path is that either there is a single window or the function doesn't observe
	// windows, so we can optimize it by treating all windows as a single one.
	if !mustExplodeWindows(n.inv.fn, elm, len(n.Side) > 0) {
		// The ProcessContinuation return value is ignored because only SDFs can return ProcessContinuations.
		_, processResult := n.processSingleWindow(mainIn)
		return processResult
	} else {
		for _, w := range elm.Windows {
			elm := &mainIn.Key
			wElm := FullValue{Elm: elm.Elm, Elm2: elm.Elm2, Timestamp: elm.Timestamp, Windows: []typex.Window{w}, Pane: elm.Pane}
			// The ProcessContinuation return value is ignored because only SDFs can return ProcessContinuations.
			_, err := n.processSingleWindow(&MainInput{Key: wElm, Values: mainIn.Values, RTracker: mainIn.RTracker})
			if err != nil {
				return n.fail(err)
			}
		}
	}
	return nil
}

// processSingleWindow processes an element given as a MainInput with a single
// window. If the element has multiple windows, they are treated as a single
// window. For DoFns that observe windows, this function should be called on
// each individual window by exploding the windows first.
func (n *ParDo) processSingleWindow(mainIn *MainInput) (sdf.ProcessContinuation, error) {
	elm := &mainIn.Key
	val, err := n.invokeProcessFn(n.ctx, elm.Pane, elm.Windows, elm.Timestamp, mainIn)
	if err != nil {
		return nil, n.fail(err)
	}

	// Forward direct output, if any. It is always a main output.
	if val != nil {
		// Check for incomplete processing of a restriction without a checkpoint
		if mainIn.RTracker != nil && !mainIn.RTracker.IsDone() && val.Continuation == nil {
			return nil, rtErrHelper(mainIn.RTracker.GetError())
		}
		// We do not forward a ProcessContinuation on its own
		if val.Elm == nil {
			return val.Continuation, nil
		}
		return val.Continuation, n.Out[0].ProcessElement(n.ctx, val)
	}

	if mainIn.RTracker != nil && !mainIn.RTracker.IsDone() {
		return nil, rtErrHelper(mainIn.RTracker.GetError())
	}

	return nil, nil
}

func rtErrHelper(err error) error {
	if err != nil {
		return err
	}
	return errors.New("DoFn terminated without fully processing restriction")
}

// mustExplodeWindows returns true iif we need to call the function
// for each window. It is needed if the function either observes the
// window, either directly or indirectly via (windowed) side inputs or state.
func mustExplodeWindows(fn *funcx.Fn, elm *FullValue, usesSideInput bool) bool {
	if len(elm.Windows) < 2 {
		return false
	}
	_, explode := fn.Window()
	_, observesState := fn.StateProvider()
	return explode || usesSideInput || observesState
}

// FinishBundle does post-bundle processing operations for the DoFn.
// Note: This is not a "FinalizeBundle" operation. Data is not yet durably
// persisted at this point.
func (n *ParDo) FinishBundle(_ context.Context) error {
	if n.status != Active {
		return errors.Errorf("invalid status for pardo %v: %v, want Active", n.UID, n.status)
	}
	n.status = Up
	n.inv.Reset()
	if n.onTimerInvoker != nil {
		n.onTimerInvoker.Reset()
	}

	n.states.Set(n.ctx, metrics.FinishBundle)

	if _, err := n.invokeDataFn(n.ctx, typex.NoFiringPane(), window.SingleGlobalWindow, mtime.ZeroTimestamp, n.Fn.FinishBundleFn(), nil); err != nil {
		return n.fail(err)
	}
	// Flush timers if any.
	if err := n.TimerTracker.FlushAndReset(n.ctx, n.timerManager); err != nil {
		return n.fail(err)
	}
	n.reader = nil
	n.cache = nil
	n.timerManager = nil

	if err := MultiFinishBundle(n.ctx, n.Out...); err != nil {
		return n.fail(err)
	}
	return nil
}

// Down performs best-effort teardown of DoFn resources. (May not run.)
func (n *ParDo) Down(ctx context.Context) error {
	if n.status == Down {
		return n.err.Error()
	}
	n.status = Down
	n.reader = nil
	n.cache = nil
	n.timerManager = nil

	if _, err := InvokeWithOptsWithoutEventTime(ctx, n.Fn.TeardownFn(), InvokeOpts{}); err != nil {
		n.err.TrySetError(err)
	}
	return n.err.Error()
}

func (n *ParDo) initSideInput(ctx context.Context, w typex.Window) error {
	if n.cache == nil {
		// First time: init single-element cache. We know that side input
		// must come before emitters in the signature.

		sideCount := len(n.Side)
		emitCount := len(n.emitters)

		n.cache = &cacheElm{
			key:   w,
			extra: make([]any, sideCount+emitCount),
		}
		attachEstimator := false
		if n.we != nil {
			// var ok bool
			if _, ok := n.we.(sdf.TimestampObservingEstimator); ok {
				attachEstimator = true
			}
		}
		for i, emit := range n.emitters {
			if attachEstimator {
				if weEmit, ok := emit.(ReusableTimestampObservingWatermarkEmitter); ok {
					weEmit.AttachEstimator(&n.we)
					n.cache.extra[i+sideCount] = weEmit.Value()
				} else {
					return errors.Errorf("Invalid emitter. Emitter %v must implement "+
						"ReusableTimestampObservingWatermarkEmitter interface because it is "+
						"used in a ParDo with a timestamp observing estimator. If you are not "+
						"using a custom emitter, you may need to regenerate your shims with the code "+
						"generator.", reflect.TypeOf(emit))
				}
			} else {
				n.cache.extra[i+sideCount] = emit.Value()
			}
		}
	} else if w.Equals(n.cache.key) {
		// Fast path: same window. Just unwind the side inputs.

		for _, s := range n.cache.sideinput {
			if err := s.Init(); err != nil {
				return err
			}
		}
		return nil
	}

	// Slow path: init side input for the given window

	sideinput, err := makeSideInputs(ctx, w, n.Side, n.reader, n.Fn.ProcessElementFn(), n.Inbound)
	if err != nil {
		return err
	}
	n.cache.key = w
	n.cache.sideinput = sideinput
	for i := 0; i < len(n.Side); i++ {
		n.cache.extra[i] = sideinput[i].Value()
	}

	for _, s := range n.cache.sideinput {
		if err := s.Init(); err != nil {
			return err
		}
	}
	return nil
}

// invokeDataFn handle non-per element invocations.
func (n *ParDo) invokeDataFn(ctx context.Context, pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, fn *funcx.Fn, opt *MainInput) (val *FullValue, err error) {
	if fn == nil {
		return nil, nil
	}
	// Defer side input clean-up in case of panic
	defer func() {
		if postErr := n.postInvoke(); postErr != nil {
			err = postErr
		}
	}()
	if err := n.preInvoke(ctx, ws, ts); err != nil {
		return nil, err
	}
	val, err = Invoke(ctx, pn, ws, ts, fn, opt, n.bf, n.we, n.UState, n.reader, n.cache.extra...)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// decodeBundleTimers is a helper to decode a batch of timers for a bundle, handling the io.EOF from the reader.
func decodeBundleTimers(spec timerFamilySpec, r io.Reader) ([]TimerRecv, error) {
	var bundleTimers []TimerRecv
	for {
		tmap, err := decodeTimer(spec.KeyDecoder, spec.WinDecoder, r)
		if err != nil {
			if goerrors.Is(err, io.EOF) {
				break
			}
			return nil, errors.WithContext(err, "error decoding received timer callback")
		}
		bundleTimers = append(bundleTimers, tmap)
	}
	return bundleTimers, nil
}

// ProcessTimers processes all timers in firing order from the runner for a timer family ID.
//
// A timer refers to a specific combination of Key+Window + Family + Tag. They also
// have a fireing time, and a data watermark hold time. The SDK doesn't determine
// if a timer is ready to fire or not, that's up to the runner.
//
// This method fires timers in the order from the runner. During this process, the user
// code may set additional firings for one or more timers, which may overwrite orderings
// from the runner.
//
// In particular, if runner sent timer produces a new firing that is earlier than a 2nd runner sent timer,
// then it is processed before that 2nd timer. This will override any subsequent firing of the same timer,
// and as a result, must add a clear to the set of timer modifications.
func (n *ParDo) ProcessTimers(timerFamilyID string, r io.Reader) (err error) {
	// Lookup actual domain for family here.
	spec := n.TimerTracker.familyToSpec[timerFamilyID]

	bundleTimers, err := decodeBundleTimers(spec, r)
	if err != nil {
		return err
	}
	for _, tmap := range bundleTimers {
		n.TimerTracker.SetCurrentKeyString(tmap.KeyString)
		for i, w := range tmap.Windows {
			ws := tmap.Windows[i : i+1]
			modifications := n.TimerTracker.GetModifications(windowKeyPair{window: w, key: tmap.KeyString})

			indexKey := sortableTimer{
				Domain: spec.Domain,
				TimerMap: timers.TimerMap{
					Family:        timerFamilyID,
					Tag:           tmap.Tag,
					Clear:         tmap.Clear,
					FireTimestamp: tmap.FireTimestamp,
					HoldTimestamp: tmap.HoldTimestamp,
				},
				Pane: tmap.Pane,
			}

			iter := modifications.earlierTimers[int(spec.Domain)].HeadSetIter(indexKey)
			for {
				insertedTimer, ok := iter()
				if !ok {
					break
				}
				// Check if this timer is still valid (no other timers have overwritten it.)
				if modifications.IsModified(insertedTimer) {
					continue
				}

				// If so, add a clear to the set for the tag + family, and process the inserted timer.
				// This clear is necessary to prevent double firing the same timer.
				// Timers may only have one active expiry (the last one set), and this timer is set
				// to fire before the next one in the same bundle.
				modifications.modified[timerKey{family: insertedTimer.Family, tag: insertedTimer.Tag}] = insertedTimer.Cleared()

				err := n.processTimer(timerFamilyID, ws, TimerRecv{
					Key:       tmap.Key,
					KeyString: tmap.KeyString,
					Windows:   ws,
					TimerMap:  insertedTimer.TimerMap,
					Pane:      insertedTimer.Pane,
				})
				if err != nil {
					return errors.WithContextf(err, "error processing inline timer %v", tmap)
				}
			}

			// If not modified by inserted timers, execute the timer from the bundle.
			if !modifications.IsModified(indexKey) {
				// Not modified means we can process the original timer.
				err := n.processTimer(timerFamilyID, ws, tmap)
				if err != nil {
					return errors.WithContextf(err, "error processing timer %v", tmap)
				}
			}
		}
	}
	return nil
}

func (n *ParDo) processTimer(timerFamilyID string, singleWindow []typex.Window, tmap TimerRecv) (err error) {
	// Defer side input clean-up in case of panic
	defer func() {
		if postErr := n.postInvoke(); postErr != nil {
			err = postErr
		}
	}()
	if err := n.preInvoke(n.ctx, singleWindow, tmap.HoldTimestamp); err != nil {
		return err
	}

	var extra []any
	extra = append(extra, timers.Context{Family: timerFamilyID, Tag: tmap.Tag})
	extra = append(extra, n.cache.extra...)
	_, err = n.onTimerInvoker.invokeWithOpts(n.ctx, tmap.Pane, singleWindow, tmap.HoldTimestamp, InvokeOpts{
		opt:   &MainInput{Key: *tmap.Key},
		bf:    n.bf,
		we:    n.we,
		sa:    n.UState,
		sr:    n.reader,
		ta:    n.TimerTracker,
		tm:    n.timerManager,
		extra: extra,
	})
	return err
}

// invokeProcessFn handles the per element invocations
func (n *ParDo) invokeProcessFn(ctx context.Context, pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, opt *MainInput) (val *FullValue, err error) {
	// Defer side input clean-up in case of panic
	defer func() {
		if postErr := n.postInvoke(); postErr != nil {
			err = postErr
		}
	}()
	if err := n.preInvoke(ctx, ws, ts); err != nil {
		return nil, err
	}
	val, err = n.inv.invokeWithOpts(ctx, pn, ws, ts, InvokeOpts{opt: opt, bf: n.bf, we: n.we, sa: n.UState, sr: n.reader, ta: n.TimerTracker, tm: n.timerManager, extra: n.cache.extra})
	if err != nil {
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
	return n.initSideInput(ctx, ws[0])
}

// postInvoke cleans up all of the open side inputs. postInvoke is deferred in invokeDataFn() and invokeProcessFn() to
// ensure that it is called even if a panic occurs. ReIter side input types may leak memory if the spawned iterators
// are not fully read before a panic/bundle failure occurs as they do not track the iterators they return.
func (n *ParDo) postInvoke() error {
	if n.cache != nil {
		for _, s := range n.cache.sideinput {
			if err := s.Reset(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (n *ParDo) fail(err error) error {
	n.status = Broken
	if err2, ok := err.(*doFnError); ok {
		return err2
	}

	parDoError := &doFnError{
		doFn: n.Fn.Name(),
		err:  err,
		uid:  n.UID,
		pid:  n.PID,
	}
	n.err.TrySetError(parDoError)
	return parDoError
}

func (n *ParDo) String() string {
	return fmt.Sprintf("ParDo[%v] Out:%v Sig: %v, SideInputs: %v", path.Base(n.Fn.Name()), IDs(n.Out...), n.Fn.ProcessElementFn().Fn.Type(), n.Side)
}
