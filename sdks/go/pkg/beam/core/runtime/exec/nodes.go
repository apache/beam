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
	"context"
	"fmt"
	"io"
	"path"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// Discard silently discard all elements. It is implicitly inserted for any
// loose ends in the pipeline.
type Discard struct {
	UID UnitID
}

func (d *Discard) ID() UnitID {
	return d.UID
}

func (d *Discard) Up(ctx context.Context) error {
	return nil
}

func (d *Discard) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	return nil
}

func (d *Discard) Down(ctx context.Context) error {
	return nil
}

func (d *Discard) String() string {
	return "Discard"
}

// Multiplex is a fan-out node.
type Multiplex struct {
	UID UnitID
	Out []Node
}

func (m *Multiplex) ID() UnitID {
	return m.UID
}

func (m *Multiplex) Up(ctx context.Context) error {
	return Up(ctx, m.Out...)
}

func (m *Multiplex) ProcessElement(ctx context.Context, elm FullValue, values ...ReStream) error {
	for _, out := range m.Out {
		if err := out.ProcessElement(ctx, elm, values...); err != nil {
			return err
		}
	}
	return nil
}

func (m *Multiplex) Down(ctx context.Context) error {
	return Down(ctx, m.Out...)
}

func (m *Multiplex) String() string {
	return fmt.Sprintf("Multiplex. Out:%v", IDs(m.Out...))
}

// NOTE(herohde) 5/1/2017: flatten is implicit in the execution model, so there
// is no need for a separate unit.

// TODO(herohde) 5/22/2017: Setup/StartBundle would be separate once we don't
// use purely single-bundle processing.

// TODO(herohde) 4/26/2017: SideInput representation? We want it to be amenable
// to the State API. For now, just use Stream.

// TODO(herohde) 4/27/2017: What is the semantics and enforcement for side
// input? Immutable? Bug or feature, if touched by user code?

type ParDo struct {
	UID  UnitID
	Edge *graph.MultiEdge
	Side []ReStream
	Out  []Node
}

func (n *ParDo) ID() UnitID {
	return n.UID
}

func (n *ParDo) Up(ctx context.Context) error {
	if err := Up(ctx, n.Out...); err != nil {
		return err
	}

	// TODO: setup, validate side input
	// TODO: specialize based on type?

	if err := n.invoke(ctx, n.Edge.DoFn.SetupFn(), false, FullValue{}); err != nil {
		return err
	}
	return n.invoke(ctx, n.Edge.DoFn.StartBundleFn(), false, FullValue{})
}

func (n *ParDo) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	return n.invoke(ctx, n.Edge.DoFn.ProcessElementFn(), true, value, values...)
}

func (n *ParDo) invoke(ctx context.Context, fn *funcx.Fn, hasMainInput bool, value FullValue, values ...ReStream) error {
	if fn == nil {
		return nil
	}

	// TODO: precompute args construction to re-use args and just plug in
	// context and FullValue as well as reset iterators.

	// (1) Populate contexts

	args := make([]reflect.Value, len(fn.Param))

	if index, ok := fn.Context(); ok {
		args[index] = reflect.ValueOf(ctx)
	}

	// (2) Main input from value
	in := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter)
	i := 0

	if hasMainInput {
		if index, ok := fn.EventTime(); ok {
			args[index] = reflect.ValueOf(value.Timestamp)
		}

		args[in[i]] = Convert(value.Elm, fn.Param[in[i]].T)
		i++
		if typex.IsWKV(n.Edge.Input[0].From.Type()) {
			args[in[i]] = Convert(value.Elm2, fn.Param[in[i]].T)
			i++
		}

		for _, iter := range values {
			param := fn.Param[in[i]]

			if param.Kind != funcx.FnIter {
				return fmt.Errorf("GBK result values must be iterable: %v", param)
			}

			// TODO: allow form conversion on GBK results?

			args[in[i]] = makeIter(param.T, iter.Open())
			i++
		}
	}

	// (3) Side inputs and form conversion

	for k, side := range n.Side {
		input := n.Edge.Input[k+1]
		param := fn.Param[in[i]]

		value, err := makeSideInput(input, param, side)
		if err != nil {
			return err
		}
		args[in[i]] = value
		i++
	}

	// (4) Outputs

	offset := 0
	hasDirectOutput := len(fn.Returns(funcx.RetValue)) > 0
	if hasDirectOutput {
		offset = 1
	}

	out := fn.Params(funcx.FnEmit)
	for i := 0; i < len(out); i++ {
		param := fn.Param[out[i]]
		args[out[i]] = makeEmit(ctx, param.T, n.Out[i+offset])
	}

	// (5) Invoke

	ret := fn.Fn.Call(args)
	if index, ok := fn.Error(); ok && ret[index].Interface() != nil {
		panic(fmt.Sprintf("Call %v failed: %v", fn.Name, ret[index].Interface()))
		// return ret[index].Interface().(error)
	}

	// (6) Direct output, if any

	if hasDirectOutput {
		value := FullValue{}
		if index, ok := fn.OutEventTime(); ok {
			value.Timestamp = ret[index].Interface().(typex.EventTime)
		}

		indices := fn.Returns(funcx.RetValue)
		value.Elm = ret[indices[0]]
		if len(indices) > 1 {
			value.Elm2 = ret[indices[1]]
		}

		if err := n.Out[0].ProcessElement(ctx, value); err != nil {
			panic(err) // see below
		}
	}
	return nil
}

func (n *ParDo) Down(ctx context.Context) error {
	if err := n.invoke(ctx, n.Edge.DoFn.FinishBundleFn(), false, FullValue{}); err != nil {
		return err
	}
	if err := n.invoke(ctx, n.Edge.DoFn.TeardownFn(), false, FullValue{}); err != nil {
		return err
	}
	return Down(ctx, n.Out...)
}

func (n *ParDo) String() string {
	return fmt.Sprintf("ParDo[%v] Out:%v", path.Base(n.Edge.DoFn.Name()), IDs(n.Out...))
}

type Combine struct {
	UID  UnitID
	Edge *graph.MultiEdge
	Side []ReStream
	Out  []Node

	accum reflect.Value // global accumulator, only used/valid if isPerKey == false
	first bool

	isPerKey, usesKey bool
}

func (n *Combine) ID() UnitID {
	return n.UID
}

func (n *Combine) Up(ctx context.Context) error {
	if err := Up(ctx, n.Out...); err != nil {
		return err
	}

	// TODO: setup, validate side input
	// TODO: specialize based on type?

	// TODO(herohde) 6/28/2017: maybe record the per-key mode in the Edge
	// instead of inferring it here?

	n.isPerKey = typex.IsWGBK(n.Edge.Input[0].From.Type())
	n.usesKey = typex.IsWKV(n.Edge.Input[0].Type)
	if n.isPerKey {
		return nil
	}

	if err := n.invoke(ctx, n.Edge.CombineFn.SetupFn()); err != nil {
		return err
	}

	a, err := n.newAccum(reflect.Value{})
	if err != nil {
		panic(fmt.Sprintf("CreateAccumulator failed: %v", err))
	}
	n.accum = a
	n.first = true
	return nil
}

func (n *Combine) invoke(ctx context.Context, fn *funcx.Fn) error {
	if fn == nil {
		return nil
	}

	// Setup/Teardown allows context and error only.

	args := make([]reflect.Value, len(fn.Param))
	if index, ok := fn.Context(); ok {
		args[index] = reflect.ValueOf(ctx)
	}

	ret := fn.Fn.Call(args)
	if index, ok := fn.Error(); ok && ret[index].Interface() != nil {
		return ret[index].Interface().(error)
	}
	return nil
}

func (n *Combine) newAccum(key reflect.Value) (reflect.Value, error) {
	fn := n.Edge.CombineFn.CreateAccumulatorFn()
	if fn == nil {
		return reflect.Zero(n.Edge.CombineFn.MergeAccumulatorsFn().Ret[0].T), nil
	}

	args := make([]reflect.Value, len(fn.Param))

	in := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter)
	i := 0

	if n.usesKey {
		args[in[i]] = key
		i++
	}
	for k, side := range n.Side {
		input := n.Edge.Input[k]
		param := fn.Param[in[i]]

		value, err := makeSideInput(input, param, side)
		if err != nil {
			return reflect.Value{}, err
		}
		args[in[i]] = value
		i++
	}

	ret := fn.Fn.Call(args)
	if index, ok := fn.Error(); ok && ret[index].Interface() != nil {
		return reflect.Value{}, ret[index].Interface().(error)
	}
	return ret[fn.Returns(funcx.RetValue)[0]], nil
}

func (n *Combine) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	if n.isPerKey {
		// For per-key combine, all processing can be done here. Note that
		// we do not explicitly call merge, although it may be called implicitly
		// when adding input.

		a, err := n.newAccum(value.Elm)
		if err != nil {
			panic(fmt.Sprintf("CreateAccumulator failed: %v", err))
		}
		first := true

		stream := values[0].Open()
		for {
			v, err := stream.Read()
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err) // see below
			}

			a, err = n.addInput(ctx, a, value.Elm, v.Elm, value.Timestamp, first)
			if err != nil {
				panic(fmt.Sprintf("AddInput failed: %v", err))
			}
			first = false
		}
		stream.Close()

		out, err := n.extract(a)
		if err != nil {
			panic(fmt.Sprintf("ExtractOutput failed: %v", err))
		}

		for _, unit := range n.Out {
			if err := unit.ProcessElement(ctx, FullValue{Elm: value.Elm, Elm2: out, Timestamp: value.Timestamp}); err != nil {
				panic(err) // see below
			}
		}
		return nil
	}

	// Accumulate globally

	a, err := n.addInput(ctx, n.accum, reflect.Value{}, value.Elm, value.Timestamp, n.first)
	if err != nil {
		panic(fmt.Sprintf("AddInput failed: %v", err))
	}
	n.accum = a
	n.first = false
	return nil
}

func (n *Combine) addInput(ctx context.Context, accum, key, value reflect.Value, timestamp typex.EventTime, first bool) (reflect.Value, error) {
	// log.Printf("AddInput: %v %v into %v", key, value, accum)

	fn := n.Edge.CombineFn.AddInputFn()
	if fn == nil {
		// Merge function only. The input value is an accumulator. We only do a binary
		// merge if we've actually seen a value.
		if first {
			return value, nil
		}

		// TODO(herohde) 7/5/2017: do we want to allow addInput to be optional
		// if non-binary merge is defined?
		return n.Edge.CombineFn.MergeAccumulatorsFn().Fn.Call(
			[]reflect.Value{reflectx.UnderlyingType(accum), reflectx.UnderlyingType(value)})[0], nil
	}

	// (1) Populate contexts

	args := make([]reflect.Value, len(fn.Param))

	if index, ok := fn.Context(); ok {
		args[index] = reflect.ValueOf(ctx)
	}

	// (2) Accumulator and main input from value
	in := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter)
	i := 0

	if index, ok := fn.EventTime(); ok {
		args[index] = reflect.ValueOf(timestamp)
	}

	args[in[i]] = accum
	i++
	if n.usesKey {
		args[in[i]] = Convert(key, fn.Param[i].T)
		i++
	}
	args[in[i]] = Convert(value, fn.Param[i].T)
	i++

	// (3) Side inputs and form conversion

	for k, side := range n.Side {
		input := n.Edge.Input[k+1]
		param := fn.Param[in[i]]

		value, err := makeSideInput(input, param, side)
		if err != nil {
			return reflect.Value{}, err
		}
		args[in[i]] = value
		i++
	}

	// (4) Invoke

	ret := fn.Fn.Call(args)
	if index, ok := fn.Error(); ok && ret[index].Interface() != nil {
		return reflect.Value{}, ret[index].Interface().(error)
	}
	return ret[fn.Returns(funcx.RetValue)[0]], nil
}

func (n *Combine) extract(accum reflect.Value) (reflect.Value, error) {
	fn := n.Edge.CombineFn.ExtractOutputFn()
	if fn == nil {
		// Merge function only. Accumulator type is the output type.
		return accum, nil
	}

	ret := fn.Fn.Call([]reflect.Value{accum})
	if index, ok := fn.Error(); ok && ret[index].Interface() != nil {
		return reflect.Value{}, ret[index].Interface().(error)
	}

	return ret[fn.Returns(funcx.RetValue)[0]], nil
}

func (n *Combine) Down(ctx context.Context) error {
	if n.isPerKey {
		return Down(ctx, n.Out...)
	}

	out, err := n.extract(n.accum)
	if err != nil {
		panic(fmt.Sprintf("ExtractOutput failed: %v", err))
	}

	if err := n.invoke(ctx, n.Edge.CombineFn.TeardownFn()); err != nil {
		return err
	}

	for _, unit := range n.Out {
		// TODO(herohde) 6/1/2017: populate FullValue.Timestamp
		if err := unit.ProcessElement(ctx, FullValue{Elm: out}); err != nil {
			panic(err) // see below
		}
	}
	return Down(ctx, n.Out...)
}

func (n *Combine) String() string {
	// Re-compute: the corresponding fields are not necessarily set yet.
	isPerKey := typex.IsWGBK(n.Edge.Input[0].From.Type())
	usesKey := typex.IsWKV(n.Edge.Input[0].Type)

	return fmt.Sprintf("Combine[%v] Keyed:%v (Use:%v) Out:%v", path.Base(n.Edge.CombineFn.Name()), isPerKey, usesKey, IDs(n.Out...))
}

// DataSource is a Root execution unit.
type DataSource struct {
	UID  UnitID
	Edge *graph.MultiEdge

	InstID string
	Source DataReader
	Out    Node
}

func (n *DataSource) ID() UnitID {
	return n.UID
}

func (n *DataSource) Up(ctx context.Context) error {
	return n.Out.Up(ctx)
}

func (n *DataSource) Process(ctx context.Context) error {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: n.InstID}

	r, err := n.Source.OpenRead(ctx, sid)
	if err != nil {
		return err
	}
	defer r.Close()

	c := n.Edge.Output[0].To.Coder
	for {
		t, err := DecodeWindowedValueHeader(c, r)
		if err != nil {
			if err == io.EOF {
				// log.Printf("EOF")
				break
			}
			return fmt.Errorf("source failed: %v", err)
		}

		switch {
		case coder.IsWGBK(c):
			ck := coder.SkipW(c).Components[0]
			cv := coder.SkipW(c).Components[1]

			// Decode key

			key, err := DecodeElement(ck, r)
			if err != nil {
				return fmt.Errorf("source decode failed: %v", err)
			}
			key.Timestamp = t

			// TODO(herohde) 4/30/2017: the State API will be handle re-iterations
			// and only "small" value streams would be inline. Presumably, that
			// would entail buffering the whole stream. We do that for now.

			var buf []FullValue

			size, err := coder.DecodeInt32(r)
			if err != nil {
				return fmt.Errorf("stream size decoding failed: %v", err)
			}

			if size > -1 {
				// Single chunk stream.

				// log.Printf("Fixed size=%v", size)

				for i := int32(0); i < size; i++ {
					value, err := DecodeElement(cv, r)
					if err != nil {
						return fmt.Errorf("stream value decode failed: %v", err)
					}
					buf = append(buf, value)
				}
			} else {
				// Multi-chunked stream.

				for {
					chunk, err := coder.DecodeVarUint64(r)
					if err != nil {
						return fmt.Errorf("stream chunk size decoding failed: %v", err)
					}

					// log.Printf("Chunk size=%v", chunk)

					if chunk == 0 {
						break
					}

					for i := uint64(0); i < chunk; i++ {
						value, err := DecodeElement(cv, r)
						if err != nil {
							return fmt.Errorf("stream value decode failed: %v", err)
						}
						buf = append(buf, value)
					}
				}
			}

			values := &FixedReStream{Buf: buf}
			if err := n.Out.ProcessElement(ctx, key, values); err != nil {
				panic(err)
			}

		case coder.IsWCoGBK(c):
			panic("NYI")

		default:
			elm, err := DecodeElement(coder.SkipW(c), r)
			if err != nil {
				return fmt.Errorf("source decode failed: %v", err)
			}
			elm.Timestamp = t

			// log.Printf("READ: %v %v", elm.Elm.Type(), elm.Elm.Interface())

			if err := n.Out.ProcessElement(ctx, elm); err != nil {
				panic(err)
			}
		}
	}
	return nil
}

func (n *DataSource) Down(ctx context.Context) error {
	return n.Out.Down(ctx)
}

func (n *DataSource) String() string {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: n.InstID}
	return fmt.Sprintf("DataSource[%v] Out:%v", sid, n.Out.ID())
}

// DataSink is a Node.
type DataSink struct {
	UID  UnitID
	Edge *graph.MultiEdge

	InstID string
	Sink   DataWriter

	w io.WriteCloser
}

func (n *DataSink) ID() UnitID {
	return n.UID
}

func (n *DataSink) Up(ctx context.Context) error {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: n.InstID}

	w, err := n.Sink.OpenWrite(ctx, sid)
	if err != nil {
		return err
	}
	n.w = w
	return nil
}

func (n *DataSink) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	// Marshal the pieces into a temporary buffer since they must be transmitted on FnAPI as a single
	// unit.
	var b bytes.Buffer

	c := n.Edge.Input[0].From.Coder
	if err := EncodeWindowedValueHeader(c, value.Timestamp, &b); err != nil {
		return err
	}
	if err := EncodeElement(coder.SkipW(c), value, &b); err != nil {
		return err
	}

	if _, err := n.w.Write(b.Bytes()); err != nil {
		return err
	}
	return nil
}

func (n *DataSink) Down(ctx context.Context) error {
	return n.w.Close()
}

func (n *DataSink) String() string {
	sid := StreamID{Port: *n.Edge.Port, Target: *n.Edge.Target, InstID: n.InstID}
	return fmt.Sprintf("DataSink[%v]", sid)
}

func makeReIter(t reflect.Type, s ReStream) reflect.Value {
	if !funcx.IsReIter(t) {
		panic(fmt.Sprintf("illegal re-iter type: %v", t))
	}
	return reflect.MakeFunc(t, func(args []reflect.Value) []reflect.Value {
		iter := makeIter(t.Out(0), s.Open())
		return []reflect.Value{iter}
	})
}

func makeIter(t reflect.Type, s Stream) reflect.Value {
	types, ok := funcx.UnfoldIter(t)
	if !ok {
		panic(fmt.Sprintf("illegal iter type: %v", t))
	}
	return reflect.MakeFunc(t, func(args []reflect.Value) []reflect.Value {
		elm, err := s.Read()
		if err != nil {
			if err == io.EOF {
				return []reflect.Value{reflect.ValueOf(false)}
			}
			panic(fmt.Sprintf("Broken stream: %v", err))
		}

		// We expect 1-3 out parameters: func (*int, *string) bool.

		isKey := true
		for i, t := range types {
			var v reflect.Value
			switch {
			case t == typex.EventTimeType:
				v = reflect.ValueOf(elm.Timestamp)
			case isKey:
				v = Convert(elm.Elm, t)
				isKey = false
			default:
				v = Convert(elm.Elm2, t)
			}
			args[i].Elem().Set(v)
		}
		return []reflect.Value{reflect.ValueOf(true)}
	})
}

func makeEmit(ctx context.Context, t reflect.Type, n Node) reflect.Value {
	types, ok := funcx.UnfoldEmit(t)
	if !ok {
		panic(fmt.Sprintf("illegal emit type: %v", t))
	}
	return reflect.MakeFunc(t, func(args []reflect.Value) []reflect.Value {
		value := FullValue{}
		isKey := true
		for i, t := range types {
			switch {
			case t == typex.EventTimeType:
				value.Timestamp = args[i].Interface().(typex.EventTime)
			case isKey:
				value.Elm = args[i]
				isKey = false
			default:
				value.Elm2 = args[i]
			}
		}

		// log.Printf("Emit to %v: %v", n.ID(), value)

		if err := n.ProcessElement(ctx, value); err != nil {
			// TODO: use panic to unwind errors _through_ user code? Or capture
			// stack trace and store in node until the end?
			panic(err)
		}
		return nil
	})
}

func makeSideInput(input *graph.Inbound, param funcx.FnParam, values ReStream) (reflect.Value, error) {
	switch input.Kind {
	case graph.Singleton:
		elms, err := ReadAll(values.Open())
		if err != nil {
			return reflect.Value{}, err
		}
		if len(elms) != 1 {
			return reflect.Value{}, fmt.Errorf("singleton side input %v for %v ill-defined", input, param)
		}
		return Convert(elms[0].Elm, param.T), nil

	case graph.Slice:
		elms, err := ReadAll(values.Open())
		if err != nil {
			return reflect.Value{}, err
		}
		slice := reflect.MakeSlice(param.T, len(elms), len(elms))
		for i := 0; i < len(elms); i++ {
			slice.Index(i).Set(Convert(elms[i].Elm, param.T.Elem()))
		}
		return slice, nil

	case graph.Iter:
		return makeIter(param.T, values.Open()), nil

	case graph.ReIter:
		return makeReIter(param.T, values), nil

	default:
		panic(fmt.Sprintf("Unexpected side input kind: %v", input))
	}
}
