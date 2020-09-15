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

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

//go:generate specialize --input=fn_arity.tmpl
//go:generate gofmt -w fn_arity.go

// MainInput is the main input and is unfolded in the invocation, if present.
type MainInput struct {
	Key      FullValue
	Values   []ReStream
	RTracker sdf.RTracker
}

// Invoke invokes the fn with the given values. The extra values must match the non-main
// side input and emitters. It returns the direct output, if any.
func Invoke(ctx context.Context, ws []typex.Window, ts typex.EventTime, fn *funcx.Fn, opt *MainInput, extra ...interface{}) (*FullValue, error) {
	if fn == nil {
		return nil, nil // ok: nothing to Invoke
	}
	inv := newInvoker(fn)
	return inv.Invoke(ctx, ws, ts, opt, extra...)
}

// InvokeWithoutEventTime runs the given function at time 0 in the global window.
func InvokeWithoutEventTime(ctx context.Context, fn *funcx.Fn, opt *MainInput, extra ...interface{}) (*FullValue, error) {
	if fn == nil {
		return nil, nil // ok: nothing to Invoke
	}
	inv := newInvoker(fn)
	return inv.InvokeWithoutEventTime(ctx, opt, extra...)
}

// invoker is a container struct for hot path invocations of DoFns, to avoid
// repeating fixed set up per element.
type invoker struct {
	fn   *funcx.Fn
	args []interface{}
	// TODO(lostluck):  2018/07/06 consider replacing with a slice of functions to run over the args slice, as an improvement.
	ctxIdx, wndIdx, etIdx int   // specialized input indexes
	outEtIdx, outErrIdx   int   // specialized output indexes
	in, out               []int // general indexes

	ret                     FullValue                     // ret is a cached allocation for passing to the next Unit. Units never modify the passed in FullValue.
	elmConvert, elm2Convert func(interface{}) interface{} // Cached conversion functions, which assums this invoker is always used with the same parameter types.
	call                    func(ws []typex.Window, ts typex.EventTime) (*FullValue, error)
}

func newInvoker(fn *funcx.Fn) *invoker {
	n := &invoker{
		fn:   fn,
		args: make([]interface{}, len(fn.Param)),
		in:   fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter | funcx.FnEmit | funcx.FnRTracker),
		out:  fn.Returns(funcx.RetValue),
	}
	var ok bool
	if n.ctxIdx, ok = fn.Context(); !ok {
		n.ctxIdx = -1
	}
	if n.wndIdx, ok = fn.Window(); !ok {
		n.wndIdx = -1
	}
	if n.etIdx, ok = fn.EventTime(); !ok {
		n.etIdx = -1
	}
	if n.outEtIdx, ok = fn.OutEventTime(); !ok {
		n.outEtIdx = -1
	}
	if n.outErrIdx, ok = fn.Error(); !ok {
		n.outErrIdx = -1
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
func (n *invoker) InvokeWithoutEventTime(ctx context.Context, opt *MainInput, extra ...interface{}) (*FullValue, error) {
	return n.Invoke(ctx, window.SingleGlobalWindow, mtime.ZeroTimestamp, opt, extra...)
}

// Invoke invokes the fn with the given values. The extra values must match the non-main
// side input and emitters. It returns the direct output, if any.
func (n *invoker) Invoke(ctx context.Context, ws []typex.Window, ts typex.EventTime, opt *MainInput, extra ...interface{}) (*FullValue, error) {
	// (1) Populate contexts
	// extract these to make things easier to read.
	args := n.args
	fn := n.fn
	in := n.in

	if n.ctxIdx >= 0 {
		args[n.ctxIdx] = ctx
	}
	if n.wndIdx >= 0 {
		if len(ws) != 1 {
			return nil, errors.Errorf("DoFns that observe windows must be invoked with single window: %v", opt.Key.Windows)
		}
		args[n.wndIdx] = ws[0]
	}
	if n.etIdx >= 0 {
		args[n.etIdx] = ts
	}

	// (2) Main input from value, if any.
	i := 0
	if opt != nil {
		if opt.RTracker != nil {
			args[in[i]] = opt.RTracker
			i++
		}
		if n.elmConvert == nil {
			from := reflect.TypeOf(opt.Key.Elm)
			n.elmConvert = ConvertFn(from, fn.Param[in[i]].T)
		}
		args[in[i]] = n.elmConvert(opt.Key.Elm)
		i++
		if opt.Key.Elm2 != nil {
			if n.elm2Convert == nil {
				from := reflect.TypeOf(opt.Key.Elm2)
				n.elm2Convert = ConvertFn(from, fn.Param[in[i]].T)
			}
			args[in[i]] = n.elm2Convert(opt.Key.Elm2)
			i++
		}

		for _, iter := range opt.Values {
			param := fn.Param[in[i]]

			if param.Kind != funcx.FnIter {
				return nil, errors.Errorf("GBK/CoGBK result values must be iterable: %v", param)
			}

			// TODO(herohde) 12/12/2017: allow form conversion on GBK results?

			it := makeIter(param.T, iter)
			it.Init()
			args[in[i]] = it.Value()
			i++
		}
	}

	// (3) Precomputed side input and emitters (or other output).
	for _, arg := range extra {
		args[in[i]] = arg
		i++
	}

	// (4) Invoke
	return n.call(ws, ts)
}

// ret1 handles processing of a single return value.
// Errors or single values are the only options.
func (n *invoker) ret1(ws []typex.Window, ts typex.EventTime, r0 interface{}) (*FullValue, error) {
	switch {
	case n.outErrIdx >= 0:
		if r0 != nil {
			return nil, r0.(error)
		}
		return nil, nil
	case n.outEtIdx >= 0:
		panic("invoker.ret1: cannot return event time without a value")
	default:
		n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0}
		return &n.ret, nil
	}
}

// ret2 handles processing of a pair of return values.
func (n *invoker) ret2(ws []typex.Window, ts typex.EventTime, r0, r1 interface{}) (*FullValue, error) {
	switch {
	case n.outErrIdx >= 0:
		if r1 != nil {
			return nil, r1.(error)
		}
		n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0}
		return &n.ret, nil
	case n.outEtIdx == 0:
		n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1}
		return &n.ret, nil
	default:
		n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Elm2: r1}
		return &n.ret, nil
	}
}

// ret3 handles processing of a trio of return values.
func (n *invoker) ret3(ws []typex.Window, ts typex.EventTime, r0, r1, r2 interface{}) (*FullValue, error) {
	switch {
	case n.outErrIdx >= 0:
		if r2 != nil {
			return nil, r2.(error)
		}
		if n.outEtIdx < 0 {
			n.ret = FullValue{Windows: ws, Timestamp: ts, Elm: r0, Elm2: r1}
			return &n.ret, nil
		}
		n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1}
		return &n.ret, nil
	case n.outEtIdx == 0:
		n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Elm2: r2}
		return &n.ret, nil
	default:
		panic(fmt.Sprintf("invoker.ret3: %T, %T, and %T don't match permitted return values.", r0, r1, r2))
	}
}

// ret4 handles processing of a quad of return values.
func (n *invoker) ret4(ws []typex.Window, ts typex.EventTime, r0, r1, r2, r3 interface{}) (*FullValue, error) {
	if r3 != nil {
		return nil, r3.(error)
	}
	n.ret = FullValue{Windows: ws, Timestamp: r0.(typex.EventTime), Elm: r1, Elm2: r2}
	return &n.ret, nil
}

func makeSideInputs(fn *funcx.Fn, in []*graph.Inbound, side []ReStream) ([]ReusableInput, error) {
	if len(side) == 0 {
		return nil, nil // ok: no side input
	}

	if len(in) != len(side)+1 {
		return nil, errors.Errorf("found %v inbound, want %v", len(in), len(side)+1)
	}
	param := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter)
	if len(param) <= len(side) {
		return nil, errors.Errorf("found %v params, want >%v", len(param), len(side))
	}

	// The side input are last of the above params, so we can compute the offset easily.
	offset := len(param) - len(side)

	var ret []ReusableInput
	for i := 0; i < len(side); i++ {
		s, err := makeSideInput(in[i+1].Kind, fn.Param[param[i+offset]].T, side[i])
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
