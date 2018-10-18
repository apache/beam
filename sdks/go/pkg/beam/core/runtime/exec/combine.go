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
	"io"
	"path"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/util/errorx"
)

// Combine is a Combine executor. Combiners do not have side inputs (or output).
type Combine struct {
	UID     UnitID
	Fn      *graph.CombineFn
	UsesKey bool
	Out     Node

	binaryMergeFn reflectx.Func2x1 // optimized caller in the case of binary merge accumulators

	status Status
	err    errorx.GuardedError
}

// ID returns the UnitID for this node.
func (n *Combine) ID() UnitID {
	return n.UID
}

// Up initializes this CombineFn and runs its SetupFn() method.
func (n *Combine) Up(ctx context.Context) error {
	if n.status != Initializing {
		return fmt.Errorf("invalid status for combine %v: %v", n.UID, n.status)
	}
	n.status = Up

	if _, err := InvokeWithoutEventTime(ctx, n.Fn.SetupFn(), nil); err != nil {
		return n.fail(err)
	}

	if n.Fn.AddInputFn() == nil {
		n.optimizeMergeFn()
	}
	return nil
}

func (n *Combine) optimizeMergeFn() {
	typ := n.Fn.MergeAccumulatorsFn().Fn.Type()
	if typ.NumIn() == 2 && typ.NumOut() == 1 {
		n.binaryMergeFn = reflectx.ToFunc2x1(n.Fn.MergeAccumulatorsFn().Fn)
	}
}

func (n *Combine) mergeAccumulators(ctx context.Context, a, b interface{}) (interface{}, error) {
	if n.binaryMergeFn != nil {
		// Fast path for binary MergeAccumulatorsFn
		return n.binaryMergeFn.Call2x1(a, b), nil
	}

	in := &MainInput{Key: FullValue{Elm: a}}
	val, err := InvokeWithoutEventTime(ctx, n.Fn.MergeAccumulatorsFn(), in, b)
	if err != nil {
		return nil, n.fail(fmt.Errorf("MergeAccumulators failed: %v", err))
	}
	return val.Elm, nil
}

// StartBundle initializes processing this bundle for combines.
func (n *Combine) StartBundle(ctx context.Context, id string, data DataContext) error {
	if n.status != Up {
		return fmt.Errorf("invalid status for combine %v: %v", n.UID, n.status)
	}
	n.status = Active

	if err := n.Out.StartBundle(ctx, id, data); err != nil {
		return n.fail(err)
	}
	return nil
}

// ProcessElement combines elements grouped by key using the CombineFn's
// AddInput, MergeAccumulators, and ExtractOutput functions.
func (n *Combine) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for combine %v: %v", n.UID, n.status)
	}

	// Note that we do not explicitly call merge, although it may
	// be called implicitly when adding input.

	a, err := n.newAccum(ctx, value.Elm)
	if err != nil {
		return n.fail(err)
	}
	first := true

	stream, err := values[0].Open()
	if err != nil {
		return n.fail(err)
	}
	defer stream.Close()
	for {
		v, err := stream.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return n.fail(err)
		}

		a, err = n.addInput(ctx, a, value.Elm, v.Elm, value.Timestamp, first)
		if err != nil {
			return n.fail(err)
		}
		first = false
	}

	out, err := n.extract(ctx, a)
	if err != nil {
		return n.fail(err)
	}
	return n.Out.ProcessElement(ctx, FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: out, Timestamp: value.Timestamp})
}

// FinishBundle completes this node's processing of a bundle.
func (n *Combine) FinishBundle(ctx context.Context) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for combine %v: %v", n.UID, n.status)
	}
	n.status = Up

	if err := n.Out.FinishBundle(ctx); err != nil {
		return n.fail(err)
	}
	return nil
}

// Down runs the ParDo's TeardownFn.
func (n *Combine) Down(ctx context.Context) error {
	if n.status == Down {
		return n.err.Error()
	}
	n.status = Down

	if _, err := InvokeWithoutEventTime(ctx, n.Fn.TeardownFn(), nil); err != nil {
		n.err.TrySetError(err)
	}
	return n.err.Error()
}

func (n *Combine) newAccum(ctx context.Context, key interface{}) (interface{}, error) {
	fn := n.Fn.CreateAccumulatorFn()
	if fn == nil {
		return reflect.Zero(n.Fn.MergeAccumulatorsFn().Ret[0].T).Interface(), nil
	}

	var opt *MainInput
	if n.UsesKey {
		opt = &MainInput{Key: FullValue{Elm: key}}
	}

	val, err := InvokeWithoutEventTime(ctx, fn, opt)
	if err != nil {
		return nil, fmt.Errorf("CreateAccumulator failed: %v", err)
	}
	return val.Elm, nil
}

func (n *Combine) addInput(ctx context.Context, accum, key, value interface{}, timestamp typex.EventTime, first bool) (interface{}, error) {
	// log.Printf("AddInput: %v %v into %v", key, value, accum)

	fn := n.Fn.AddInputFn()
	if fn == nil {
		// Merge function only. The input value is an accumulator. We only do a binary
		// merge if we've actually seen a value.
		if first {
			return value, nil
		}

		// TODO(herohde) 7/5/2017: do we want to allow addInput to be optional
		// if non-binary merge is defined?

		return n.mergeAccumulators(ctx, accum, value)
	}

	opt := &MainInput{
		Key: FullValue{
			Elm:       accum,
			Timestamp: timestamp,
		},
	}

	in := fn.Params(funcx.FnValue | funcx.FnIter | funcx.FnReIter)
	i := 1
	if n.UsesKey {
		opt.Key.Elm2 = Convert(key, fn.Param[in[i]].T)
		i++
	}
	v := Convert(value, fn.Param[i].T)

	val, err := InvokeWithoutEventTime(ctx, n.Fn.AddInputFn(), opt, v)
	if err != nil {
		return nil, n.fail(fmt.Errorf("AddInput failed: %v", err))
	}
	return val.Elm, err
}

func (n *Combine) extract(ctx context.Context, accum interface{}) (interface{}, error) {
	fn := n.Fn.ExtractOutputFn()
	if fn == nil {
		// Merge function only. Accumulator type is the output type.
		return accum, nil
	}

	val, err := InvokeWithoutEventTime(ctx, n.Fn.ExtractOutputFn(), nil, accum)
	if err != nil {
		return nil, n.fail(fmt.Errorf("ExtractOutput failed: %v", err))
	}
	return val.Elm, err
}

func (n *Combine) fail(err error) error {
	n.status = Broken
	n.err.TrySetError(err)
	return err
}

func (n *Combine) String() string {
	return fmt.Sprintf("Combine[%v] Keyed:%v Out:%v", path.Base(n.Fn.Name()), n.UsesKey, n.Out.ID())
}

// The nodes below break apart the Combine into components to support
// Combiner Lifting optimizations.

// LiftedCombine is an executor for combining values before grouping by keys
// for a lifted combine. Partially groups values by key within a bundle,
// accumulating them in an in memory cache, before emitting them in the
// FinishBundle step.
type LiftedCombine struct {
	*Combine

	cache map[interface{}]FullValue
}

func (n *LiftedCombine) String() string {
	return fmt.Sprintf("LiftedCombine[%v] Keyed:%v Out:%v", path.Base(n.Fn.Name()), n.UsesKey, n.Out.ID())
}

// StartBundle initializes the in memory cache of keys to accumulators.
func (n *LiftedCombine) StartBundle(ctx context.Context, id string, data DataContext) error {
	if err := n.Combine.StartBundle(ctx, id, data); err != nil {
		return err
	}
	n.cache = make(map[interface{}]FullValue)
	return nil
}

// ProcessElement takes a KV pair and combines values with the same into an accumulator,
// caching them until the bundle is complete.
func (n *LiftedCombine) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for precombine %v: %v", n.UID, n.status)
	}

	// Value is a KV so Elm & Elm2 are populated.
	// Check the cache for an already present accumulator

	afv, notfirst := n.cache[value.Elm]
	var a interface{}
	if notfirst {
		a = afv.Elm2
	} else {
		b, err := n.newAccum(ctx, value.Elm)
		if err != nil {
			return n.fail(err)
		}
		a = b
	}

	a, err := n.addInput(ctx, a, value.Elm, value.Elm2, value.Timestamp, !notfirst)
	if err != nil {
		return n.fail(err)
	}

	// Cache the accumulator with the key
	n.cache[value.Elm] = FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: a, Timestamp: value.Timestamp}

	return nil
}

// FinishBundle iterates through the cached (key, accumulator) pairs, and then
// processes the value in the bundle as normal.
func (n *LiftedCombine) FinishBundle(ctx context.Context) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for precombine %v: %v", n.UID, n.status)
	}
	n.status = Up

	// Need to run n.Out.ProcessElement for all the cached precombined KVs, and
	// then finally Finish bundle as normal.
	for _, a := range n.cache {
		n.Out.ProcessElement(ctx, a)
	}

	if err := n.Out.FinishBundle(ctx); err != nil {
		return n.fail(err)
	}
	return nil
}

// Down tears down the cache.
func (n *LiftedCombine) Down(ctx context.Context) error {
	if err := n.Combine.Down(ctx); err != nil {
		return err
	}
	n.cache = nil
	return nil
}

// MergeAccumulators is an executor for merging accumulators from a lifted combine.
type MergeAccumulators struct {
	*Combine
}

func (n *MergeAccumulators) String() string {
	return fmt.Sprintf("MergeAccumulators[%v] Keyed:%v Out:%v", path.Base(n.Fn.Name()), n.UsesKey, n.Out.ID())
}

// ProcessElement accepts a stream of accumulator values with the same key and
// runs the MergeAccumulatorsFn over them repeatedly.
func (n *MergeAccumulators) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for combine merge %v: %v", n.UID, n.status)
	}
	a, err := n.newAccum(ctx, value.Elm)
	if err != nil {
		return n.fail(err)
	}
	first := true

	stream, err := values[0].Open()
	if err != nil {
		return n.fail(err)
	}
	defer stream.Close()
	for {
		v, err := stream.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return n.fail(err)
		}
		if first {
			a = v.Elm
			first = false
			continue
		}
		a, err = n.mergeAccumulators(ctx, a, v.Elm)
		if err != nil {
			return err
		}
	}
	return n.Out.ProcessElement(ctx, FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: a, Timestamp: value.Timestamp})
}

// Up eagerly gets the optimized binary merge function.
func (n *MergeAccumulators) Up(ctx context.Context) error {
	if err := n.Combine.Up(ctx); err != nil {
		return err
	}
	n.optimizeMergeFn()
	return nil
}

// ExtractOutput is an executor for extracting output from a lifted combine.
type ExtractOutput struct {
	*Combine
}

func (n *ExtractOutput) String() string {
	return fmt.Sprintf("ExtractOutput[%v] Keyed:%v Out:%v", path.Base(n.Fn.Name()), n.UsesKey, n.Out.ID())
}

// ProcessElement accepts an accumulator value, and extracts the final return type from it.
func (n *ExtractOutput) ProcessElement(ctx context.Context, value FullValue, values ...ReStream) error {
	if n.status != Active {
		return fmt.Errorf("invalid status for combine extract %v: %v", n.UID, n.status)
	}
	out, err := n.extract(ctx, value.Elm2)
	if err != nil {
		return n.fail(err)
	}
	return n.Out.ProcessElement(ctx, FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: out, Timestamp: value.Timestamp})
}
