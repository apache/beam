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
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

//go:generate specialize --input=fn_arity.tmpl
//go:generate gofmt -w fn_arity.go

// MainInput is the main input and is unfolded in the invocation, if present.
type MainInput struct {
	Key      FullValue
	Values   []ReStream
	RTracker sdf.RTracker
}

type bundleFinalizationCallback struct {
	callback   func() error
	validUntil time.Time
}

// bundleFinalizer holds all the user defined callbacks to be run on bundle finalization.
// Implements typex.BundleFinalization
type bundleFinalizer struct {
	callbacks         []bundleFinalizationCallback
	lastValidCallback time.Time // Used to track when we can safely gc the bundleFinalizer
}

type needsBundleFinalization interface {
	AttachFinalizer(*bundleFinalizer)
}

// RegisterCallback is used to register callbacks during DoFn execution.
func (bf *bundleFinalizer) RegisterCallback(t time.Duration, cb func() error) {
	callback := bundleFinalizationCallback{
		callback:   cb,
		validUntil: time.Now().Add(t),
	}
	bf.callbacks = append(bf.callbacks, callback)
	if bf.lastValidCallback.Before(callback.validUntil) {
		bf.lastValidCallback = callback.validUntil
	}
}

// Invoke invokes the fn with the given values. The extra values must match the non-main
// side input and emitters. It returns the direct output, if any.
//
// Deprecated: prefer InvokeWithOpts
func Invoke(ctx context.Context, pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, fn *funcx.Fn, opt *MainInput, bf *bundleFinalizer, we sdf.WatermarkEstimator, sa UserStateAdapter, sr StateReader, extra ...any) (*FullValue, error) {
	if fn == nil {
		return nil, nil
	}
	inv := newInvoker(fn)
	return inv.invokeWithOpts(ctx, pn, ws, ts, InvokeOpts{opt: opt, bf: bf, we: we, sa: sa, sr: sr, extra: extra})
}

// InvokeOpts are optional parameters to invoke a Fn.
type InvokeOpts struct {
	opt   *MainInput
	bf    *bundleFinalizer
	we    sdf.WatermarkEstimator
	sa    UserStateAdapter
	sr    StateReader
	ta    *userTimerAdapter
	tm    DataManager
	extra []any
}

// InvokeWithOpts invokes the fn with the given values. The extra values must match the non-main
// side input and emitters. It returns the direct output, if any.
func InvokeWithOpts(ctx context.Context, fn *funcx.Fn, pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, opts InvokeOpts) (*FullValue, error) {
	if fn == nil {
		return nil, nil // ok: nothing to Invoke
	}
	inv := newInvoker(fn)
	return inv.invokeWithOpts(ctx, pn, ws, ts, opts)
}

// InvokeWithOptsWithoutEventTime runs the given function at time 0 in the global window.
func InvokeWithOptsWithoutEventTime(ctx context.Context, fn *funcx.Fn, opts InvokeOpts) (*FullValue, error) {
	return InvokeWithOpts(ctx, fn, typex.NoFiringPane(), window.SingleGlobalWindow, mtime.ZeroTimestamp, opts)
}

// InvokeWithoutEventTime runs the given function at time 0 in the global window.
//
// Deprecated: prefer InvokeWithOptsWithoutEventTime
func InvokeWithoutEventTime(ctx context.Context, fn *funcx.Fn, opt *MainInput, bf *bundleFinalizer, we sdf.WatermarkEstimator, sa UserStateAdapter, reader StateReader, extra ...any) (*FullValue, error) {
	if fn == nil {
		return nil, nil // ok: nothing to Invoke
	}
	inv := newInvoker(fn)
	return inv.InvokeWithoutEventTime(ctx, opt, bf, we, sa, reader, extra...)
}

// invoker is a container struct for hot path invocations of DoFns, to avoid
// repeating fixed set up per element.
type invoker struct {
	fn   *funcx.Fn
	args []any
	sp   *stateProvider
	tp   *timerProvider

	// TODO(lostluck):  2018/07/06 consider replacing with a slice of functions to run over the args slice, as an improvement.
	ctxIdx, pnIdx, wndIdx, etIdx, bfIdx, weIdx, spIdx, tpIdx int   // specialized input indexes
	outEtIdx, outPcIdx, outErrIdx                            int   // specialized output indexes
	in, out                                                  []int // general indexes

	ret                     FullValue     // ret is a cached allocation for passing to the next Unit. Units never modify the passed in FullValue.
	elmConvert, elm2Convert func(any) any // Cached conversion functions, which assums this invoker is always used with the same parameter types.
	call                    func(pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime) (*FullValue, error)
}

func newInvoker(fn *funcx.Fn) *invoker {
	n := &invoker{
		fn:   fn,
		args: make([]any, len(fn.Param)),
		in:   fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter | funcx.FnEmit | funcx.FnMultiMap | funcx.FnRTracker),
		out:  fn.Returns(funcx.RetValue),
	}
	var ok bool
	if n.ctxIdx, ok = fn.Context(); !ok {
		n.ctxIdx = -1
	}
	if n.pnIdx, ok = fn.Pane(); !ok {
		n.pnIdx = -1
	}
	if n.wndIdx, ok = fn.Window(); !ok {
		n.wndIdx = -1
	}
	if n.etIdx, ok = fn.EventTime(); !ok {
		n.etIdx = -1
	}
	if n.weIdx, ok = fn.WatermarkEstimator(); !ok {
		n.weIdx = -1
	}
	if n.spIdx, ok = fn.StateProvider(); !ok {
		n.spIdx = -1
	}
	if n.tpIdx, ok = fn.TimerProvider(); !ok {
		n.tpIdx = -1
	}
	if n.outEtIdx, ok = fn.OutEventTime(); !ok {
		n.outEtIdx = -1
	}
	if n.outErrIdx, ok = fn.Error(); !ok {
		n.outErrIdx = -1
	}
	if n.bfIdx, ok = fn.BundleFinalization(); !ok {
		n.bfIdx = -1
	}
	if n.outPcIdx, ok = fn.ProcessContinuation(); !ok {
		n.outPcIdx = -1
	}

	n.initCall()

	return n
}

// Reset zeroes argument entries in the cached slice to allow values to be garbage collected after the bundle ends.
func (n *invoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
	// Avoid leaking user elements after bundle termination.
	n.ret = FullValue{}
}

// InvokeWithoutEventTime runs the function at time 0 in the global window.
func (n *invoker) InvokeWithoutEventTime(ctx context.Context, opt *MainInput, bf *bundleFinalizer, we sdf.WatermarkEstimator, sa UserStateAdapter, reader StateReader, extra ...any) (*FullValue, error) {
	return n.Invoke(ctx, typex.NoFiringPane(), window.SingleGlobalWindow, mtime.ZeroTimestamp, opt, bf, we, sa, reader, extra...)
}

// Invoke invokes the fn with the given values. The extra values must match the non-main
// side input and emitters. It returns the direct output, if any.
func (n *invoker) Invoke(ctx context.Context, pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, opt *MainInput, bf *bundleFinalizer, we sdf.WatermarkEstimator, sa UserStateAdapter, sr StateReader, extra ...any) (*FullValue, error) {
	return n.invokeWithOpts(ctx, pn, ws, ts, InvokeOpts{opt: opt, bf: bf, we: we, sa: sa, sr: sr, extra: extra})
}

func (n *invoker) invokeWithOpts(ctx context.Context, pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, opts InvokeOpts) (*FullValue, error) {
	// (1) Populate contexts
	// extract these to make things easier to read.
	args := n.args
	fn := n.fn
	in := n.in

	if n.ctxIdx >= 0 {
		args[n.ctxIdx] = ctx
	}
	if n.pnIdx >= 0 {
		args[n.pnIdx] = pn
	}
	if n.wndIdx >= 0 {
		if len(ws) != 1 {
			return nil, errors.Errorf("DoFns that observe windows must be invoked with single window: %v", opts.opt.Key.Windows)
		}
		args[n.wndIdx] = ws[0]
	}
	if n.etIdx >= 0 {
		args[n.etIdx] = ts
	}
	if n.bfIdx >= 0 {
		args[n.bfIdx] = opts.bf
	}
	if n.weIdx >= 0 {
		args[n.weIdx] = opts.we
	}

	if n.spIdx >= 0 {
		sp, err := opts.sa.NewStateProvider(ctx, opts.sr, ws[0], opts.opt)
		if err != nil {
			return nil, err
		}
		n.sp = &sp
		args[n.spIdx] = n.sp
	}

	if n.tpIdx >= 0 {
		n.tp = opts.ta.NewTimerProvider(pn, ws)
		args[n.tpIdx] = n.tp
	}

	// (2) Main input from value, if any.
	i := 0
	if opts.opt != nil {
		if opts.opt.RTracker != nil {
			args[in[i]] = opts.opt.RTracker
			i++
		}
		if n.elmConvert == nil {
			from := reflect.TypeOf(opts.opt.Key.Elm)
			n.elmConvert = ConvertFn(from, fn.Param[in[i]].T)
		}
		args[in[i]] = n.elmConvert(opts.opt.Key.Elm)
		i++
		if opts.opt.Key.Elm2 != nil {
			if n.elm2Convert == nil {
				from := reflect.TypeOf(opts.opt.Key.Elm2)
				n.elm2Convert = ConvertFn(from, fn.Param[in[i]].T)
			}
			args[in[i]] = n.elm2Convert(opts.opt.Key.Elm2)
			i++
		}

		for _, iter := range opts.opt.Values {
			param := fn.Param[in[i]]

			if param.Kind != funcx.FnIter {
				return nil, errors.Errorf("GBK/CoGBK result values must be iterable: %v", param)
			}

			// TODO(herohde) 12/12/2017: allow form conversion on GBK results?

			it := makeIter(param.T, iter)
			it.Init()
			args[in[i]] = it.Value()
			// Ensure main value iterators are reset & closed after the invoke to avoid
			// short read problems.
			defer it.Reset()
			i++
		}
	}

	// (3) Precomputed side input and emitters (or other output).
	for _, arg := range opts.extra {
		args[in[i]] = arg
		i++
	}

	// (4) Invoke
	return n.call(pn, ws, ts)
}

// ret1 handles processing of a single return value.
// Errors, single values, or a ProcessContinuation are the only options.
func (n *invoker) ret1(pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, r0 any) (*FullValue, error) {
	switch {
	case n.outErrIdx == 0:
		if r0 != nil {
			return nil, r0.(error)
		}
		return nil, nil
	case n.outPcIdx == 0:
		if r0 == nil {
			panic(fmt.Sprintf("invoker.ret1: cannot return a nil process continuation from function %v", n.fn))
		}
		n.ret = FullValue{Windows: ws, Timestamp: ts, Pane: pn, Continuation: r0.(sdf.ProcessContinuation)}
		return &n.ret, nil
	case n.outEtIdx == 0:
		panic("invoker.ret1: cannot return event time without a value")
	default:
		n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Pane: pn}
		return &n.ret, nil
	}
}

// ret2 handles processing of a pair of return values.
func (n *invoker) ret2(pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, r0, r1 any) (*FullValue, error) {
	switch {
	case n.outErrIdx == 1:
		if r1 != nil {
			return nil, r1.(error)
		}
		if n.outPcIdx == 0 {
			if r0 == nil {
				panic(fmt.Sprintf("invoker.ret2: cannot return a nil process continuation from function %v", n.fn))
			}
			n.ret = FullValue{Windows: ws, Timestamp: ts, Pane: pn, Continuation: r0.(sdf.ProcessContinuation)}
			return &n.ret, nil
		}
		n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Pane: pn}
		return &n.ret, nil
	case n.outEtIdx == 0:
		if n.outPcIdx == 1 {
			panic("invoker.ret2: cannot return event time without a value")
		}
		n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Pane: pn}
		return &n.ret, nil
	case n.outPcIdx == 1:
		if r1 == nil {
			panic(fmt.Sprintf("invoker.ret2: cannot return a nil process continuation from function %v", n.fn))
		}
		n.ret = FullValue{Windows: ws, Timestamp: ts, Pane: pn, Elm: r0, Continuation: r1.(sdf.ProcessContinuation)}
		return &n.ret, nil
	default:
		n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Elm2: r1, Pane: pn}
		return &n.ret, nil
	}
}

// ret3 handles processing of a trio of return values.
func (n *invoker) ret3(pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, r0, r1, r2 any) (*FullValue, error) {
	switch {
	case n.outEtIdx == 0:
		if n.outErrIdx == 2 {
			if r2 != nil {
				return nil, r2.(error)
			}
			n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Pane: pn}
			return &n.ret, nil
		}
		if n.outPcIdx == 2 {
			if r2 == nil {
				panic(fmt.Sprintf("invoker.ret3: cannot return a nil process continuation from function %v", n.fn))
			}
			n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Pane: pn, Continuation: r2.(sdf.ProcessContinuation)}
			return &n.ret, nil
		}
		n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Elm2: r2, Pane: pn}
		return &n.ret, nil
	case n.outErrIdx == 2:
		if r2 != nil {
			return nil, r2.(error)
		}
		if n.outPcIdx == 1 {
			if r1 == nil {
				panic(fmt.Sprintf("invoker.ret3: cannot return a nil process continuation from function %v", n.fn))
			}
			n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Pane: pn, Continuation: r1.(sdf.ProcessContinuation)}
			return &n.ret, nil
		}
		n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Elm2: r1, Pane: pn}
		return &n.ret, nil
	default:
		if r2 == nil {
			panic(fmt.Sprintf("invoker.ret3: cannot return a nil process continuation from function %v", n.fn))
		}
		n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Elm2: r1, Pane: pn, Continuation: r2.(sdf.ProcessContinuation)}
		return &n.ret, nil
	}
}

// ret4 handles processing of a quad of return values.
func (n *invoker) ret4(pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, r0, r1, r2, r3 any) (*FullValue, error) {
	if n.outEtIdx == 0 {
		if n.outErrIdx == 3 {
			if r3 != nil {
				return nil, r3.(error)
			}
			if n.outPcIdx == 2 {
				if r2 == nil {
					panic(fmt.Sprintf("invoker.ret4: cannot return a nil process continuation from function %v", n.fn))
				}
				n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Pane: pn, Continuation: r2.(sdf.ProcessContinuation)}
				return &n.ret, nil
			}
			n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Elm2: r2, Pane: pn}
			return &n.ret, nil
		}
		if r3 == nil {
			panic(fmt.Sprintf("invoker.ret4: cannot return a nil process continuation from function %v", n.fn))
		}
		n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Elm2: r2, Pane: pn, Continuation: r3.(sdf.ProcessContinuation)}
		return &n.ret, nil
	}

	if r3 != nil {
		return nil, r3.(error)
	}
	if r2 == nil {
		panic(fmt.Sprintf("invoker.ret4: cannot return a nil process continuation from function %v", n.fn))
	}
	n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Elm2: r1, Pane: pn, Continuation: r2.(sdf.ProcessContinuation)}
	return &n.ret, nil
}

// ret5 handles processing five return values.
func (n *invoker) ret5(pn typex.PaneInfo, ws []typex.Window, ts typex.EventTime, r0, r1, r2, r3, r4 any) (*FullValue, error) {
	if r4 != nil {
		return nil, r4.(error)
	}
	if r3 == nil {
		panic(fmt.Sprintf("invoker.ret5: cannot return a nil process continuation from function %v", n.fn))
	}
	n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Elm2: r2, Continuation: r3.(sdf.ProcessContinuation)}
	return &n.ret, nil
}

func makeSideInputs(ctx context.Context, w typex.Window, side []SideInputAdapter, reader StateReader, fn *funcx.Fn, in []*graph.Inbound) ([]ReusableInput, error) {
	if len(side) == 0 {
		return nil, nil // ok: no side input
	}

	if len(in) != len(side)+1 {
		return nil, errors.Errorf("found %v inbound, want %v", len(in), len(side)+1)
	}
	param := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter | funcx.FnMultiMap)
	if len(param) <= len(side) {
		return nil, errors.Errorf("found %v params, want >%v", len(param), len(side))
	}

	// The side input are last of the above params, so we can compute the offset easily.
	offset := len(param) - len(side)

	var ret []ReusableInput
	for i, adapter := range side {
		inKind := in[i+1].Kind
		params := fn.Param[param[i+offset]].T
		// Handle MultiMaps separately since they require more/different information
		// than the other side inputs
		if inKind == graph.MultiMap {
			s := makeMultiMap(ctx, params, side[i], reader, w)
			ret = append(ret, s)
			continue
		}
		stream, err := adapter.NewIterable(ctx, reader, w)
		if err != nil {
			return nil, err
		}
		s, err := makeSideInput(inKind, params, stream)
		if err != nil {
			return nil, errors.WithContextf(err, "making side input %v for %v", i, fn)
		}
		ret = append(ret, s)
	}
	return ret, nil
}

func makeEmitters(fn *funcx.Fn, nodes []Node) ([]ReusableEmitter, error) {
	if len(nodes) == 0 {
		return nil, nil // ok: no output nodes
	}

	offset := 0
	if len(fn.Returns(funcx.RetValue)) > 0 {
		offset = 1
	}
	out := fn.Params(funcx.FnEmit)
	if len(out) != len(nodes)-offset {
		return nil, errors.Errorf("found %v emitters, want %v", len(out), len(nodes)-offset)
	}

	var ret []ReusableEmitter
	for i := 0; i < len(out); i++ {
		param := fn.Param[out[i]]
		ret = append(ret, makeEmit(param.T, nodes[i+offset]))
	}
	return ret, nil
}

// makeSideInput returns a reusable side input of the given kind and type.
func makeSideInput(kind graph.InputKind, t reflect.Type, values ReStream) (ReusableInput, error) {
	switch kind {
	case graph.Singleton:
		elms, err := ReadAll(values)
		if err != nil {
			return nil, err
		}
		if len(elms) != 1 {
			return nil, errors.Errorf("got %d values, want one value for %v side input of type %v", len(elms), graph.Singleton, t)
		}
		return &fixedValue{val: Convert(elms[0].Elm, t)}, nil

	case graph.Slice:
		elms, err := ReadAll(values)
		if err != nil {
			return nil, err
		}
		slice := reflect.MakeSlice(t, len(elms), len(elms))
		for i := 0; i < len(elms); i++ {
			slice.Index(i).Set(reflect.ValueOf(Convert(elms[i].Elm, t.Elem())))
		}
		return &fixedValue{val: slice.Interface()}, nil

	case graph.Iter:
		return makeIter(t, values), nil

	case graph.ReIter:
		return makeReIter(t, values), nil

	default:
		panic(fmt.Sprintf("Unexpected side input kind: %v", kind))
	}
}
