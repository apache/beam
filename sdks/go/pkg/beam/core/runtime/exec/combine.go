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

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/metrics"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/go/pkg/beam/util/errorx"
)

// Combine is a Combine executor. Combiners do not have side inputs (or output).
type Combine struct {
	UID     UnitID
	Fn      *graph.CombineFn
	UsesKey bool
	Out     Node

	PID string
	ctx context.Context

	binaryMergeFn reflectx.Func2x1 // optimized caller in the case of binary merge accumulators

	status Status
	err    errorx.GuardedError

	// reusable invokers
	createAccumInv, addInputInv, mergeInv, extractOutputInv *invoker
	// cached value converter for add input.
	aiValConvert func(interface{}) interface{}
}

// GetPID returns the PTransformID for this CombineFn.
func (n *Combine) GetPID() string {
	return n.PID
}

// ID returns the UnitID for this node.
func (n *Combine) ID() UnitID {
	return n.UID
}

// Up initializes this CombineFn and runs its SetupFn() method.
func (n *Combine) Up(ctx context.Context) error {
	if n.status != Initializing {
		return errors.Errorf("invalid status for combine %v: %v", n.UID, n.status)
	}
	n.status = Up

	if _, err := InvokeWithoutEventTime(ctx, n.Fn.SetupFn(), nil); err != nil {
		return n.fail(err)
	}

	if ca := n.Fn.CreateAccumulatorFn(); ca != nil {
		n.createAccumInv = newInvoker(ca)
	}
	if ai := n.Fn.AddInputFn(); ai != nil {
		n.addInputInv = newInvoker(ai)
	} else {
		n.optimizeMergeFn()
	}
	n.mergeInv = newInvoker(n.Fn.MergeAccumulatorsFn())
	if eo := n.Fn.ExtractOutputFn(); eo != nil {
		n.extractOutputInv = newInvoker(eo)
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
	val, err := n.mergeInv.InvokeWithoutEventTime(ctx, in, b)
	if err != nil {
		return nil, n.fail(errors.WithContext(err, "invoking MergeAccumulators"))
	}
	return val.Elm, nil
}

// StartBundle initializes processing this bundle for combines.
func (n *Combine) StartBundle(ctx context.Context, id string, data DataContext) error {
	if n.status != Up {
		return errors.Errorf("invalid status for combine %v: %v", n.UID, n.status)
	}
	n.status = Active

	// Allocating contexts all the time is expensive, but we seldom re-write them,
	// and never accept modified contexts from users, so we will cache them per-bundle
	// per-unit, to avoid the constant allocation overhead.
	n.ctx = metrics.SetPTransformID(ctx, n.PID)

	if err := n.Out.StartBundle(n.ctx, id, data); err != nil {
		return n.fail(err)
	}
	return nil
}

// ProcessElement combines elements grouped by key using the CombineFn's
// AddInput, MergeAccumulators, and ExtractOutput functions.
func (n *Combine) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	if n.status != Active {
		return errors.Errorf("invalid status for combine %v: %v", n.UID, n.status)
	}

	// Note that we do not explicitly call merge, although it may
	// be called implicitly when adding input.

	a, err := n.newAccum(n.ctx, value.Elm)
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

		a, err = n.addInput(n.ctx, a, value.Elm, v.Elm, value.Timestamp, first)
		if err != nil {
			return n.fail(err)
		}
		first = false
	}

	out, err := n.extract(n.ctx, a)
	if err != nil {
		return n.fail(err)
	}
	return n.Out.ProcessElement(n.ctx, &FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: out, Timestamp: value.Timestamp})
}

// FinishBundle completes this node's processing of a bundle.
func (n *Combine) FinishBundle(ctx context.Context) error {
	if n.status != Active {
		return errors.Errorf("invalid status for combine %v: %v", n.UID, n.status)
	}
	n.status = Up
	if n.createAccumInv != nil {
		n.createAccumInv.Reset()
	}
	if n.addInputInv != nil {
		n.addInputInv.Reset()
	}
	if n.mergeInv != nil {
		n.mergeInv.Reset()
	}
	if n.extractOutputInv != nil {
		n.extractOutputInv.Reset()
	}

	if err := n.Out.FinishBundle(n.ctx); err != nil {
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

	val, err := n.createAccumInv.InvokeWithoutEventTime(ctx, opt)
	if err != nil {
		return nil, n.fail(errors.WithContext(err, "invoking CreateAccumulator"))
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

	i := 1
	if n.UsesKey {
		//opt.Key.Elm2 = Convert(key, n.addInputInv.fn.Param[n.addInputInv.in[i]].T)
		opt.Key.Elm2 = key
		i++
	}
	if n.aiValConvert == nil {
		from := reflect.TypeOf(value)
		n.aiValConvert = ConvertFn(from, n.addInputInv.fn.Param[i].T)
	}
	v := n.aiValConvert(value)

	val, err := n.addInputInv.InvokeWithoutEventTime(ctx, opt, v)
	if err != nil {
		return nil, n.fail(errors.WithContext(err, "invoking AddInput"))
	}
	return val.Elm, err
}

func (n *Combine) extract(ctx context.Context, accum interface{}) (interface{}, error) {
	fn := n.Fn.ExtractOutputFn()
	if fn == nil {
		// Merge function only. Accumulator type is the output type.
		return accum, nil
	}

	val, err := n.extractOutputInv.InvokeWithoutEventTime(ctx, nil, accum)
	if err != nil {
		return nil, n.fail(errors.WithContext(err, "invoking ExtractOutput"))
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
	KeyCoder *coder.Coder

	keyHash elementHasher
	cache   map[uint64]FullValue
}

func (n *LiftedCombine) String() string {
	return fmt.Sprintf("LiftedCombine[%v] Keyed:%v Out:%v", path.Base(n.Fn.Name()), n.UsesKey, n.Out.ID())
}

// Up initializes the LiftedCombine.
func (n *LiftedCombine) Up(ctx context.Context) error {
	if err := n.Combine.Up(ctx); err != nil {
		return err
	}
	n.keyHash = makeElementHasher(n.KeyCoder)
	return nil
}

// StartBundle initializes the in memory cache of keys to accumulators.
func (n *LiftedCombine) StartBundle(ctx context.Context, id string, data DataContext) error {
	if err := n.Combine.StartBundle(ctx, id, data); err != nil {
		return err
	}
	n.cache = make(map[uint64]FullValue)
	return nil
}

// ProcessElement takes a KV pair and combines values with the same key into an accumulator,
// caching them until the bundle is complete. If the cache grows too large, a random eviction
// policy is used.
func (n *LiftedCombine) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	if n.status != Active {
		return errors.Errorf("invalid status for precombine %v: %v", n.UID, n.status)
	}

	key, err := n.keyHash.Hash(value.Elm)
	if err != nil {
		return n.fail(err)
	}
	// Value is a KV so Elm & Elm2 are populated.
	// Check the cache for an already present accumulator

	afv, notfirst := n.cache[key]
	var a interface{}
	if notfirst {
		a = afv.Elm2
	} else {
		b, err := n.newAccum(n.Combine.ctx, value.Elm)
		if err != nil {
			return n.fail(err)
		}
		a = b
	}

	a, err = n.addInput(n.Combine.ctx, a, value.Elm, value.Elm2, value.Timestamp, !notfirst)
	if err != nil {
		return n.fail(err)
	}

	// TODO(BEAM-4468): replace with some better implementation
	// once adding dependencies is easier.
	// Arbitrary limit until a broader improvement can be demonstrated.
	const cacheMax = 2000
	if len(n.cache) > cacheMax {
		// Use Go's random map iteration to have a basic
		// random eviction policy.
		for k, a := range n.cache {
			// Never evict and send out the current working key.
			// We've already combined this contribution with the
			// accumulator and we'd be repeating the contributions
			// of older elements.
			if k == key {
				continue
			}
			if err := n.Out.ProcessElement(n.Combine.ctx, &a); err != nil {
				return err
			}
			delete(n.cache, k)
			// Having the check be on strict greater than and
			// strict less than allows at least 2 keys to be
			// processed before evicting again.
			if len(n.cache) < cacheMax {
				break
			}
		}
	}

	// Cache the accumulator with the key
	n.cache[key] = FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: a, Timestamp: value.Timestamp}

	return nil
}

// FinishBundle iterates through the cached (key, accumulator) pairs, and then
// processes the value in the bundle as normal.
func (n *LiftedCombine) FinishBundle(ctx context.Context) error {
	// Need to run n.Out.ProcessElement for all the cached precombined KVs, and
	// then finally Finish bundle as normal.
	for _, a := range n.cache {
		if err := n.Out.ProcessElement(n.Combine.ctx, &a); err != nil {
			return err
		}
	}
	// Clear the cache now since all elements have been output.
	// Down isn't guaranteed to be called.
	n.cache = nil

	return n.Combine.FinishBundle(n.Combine.ctx)
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
func (n *MergeAccumulators) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	if n.status != Active {
		return errors.Errorf("invalid status for combine merge %v: %v", n.UID, n.status)
	}
	a, err := n.newAccum(n.Combine.ctx, value.Elm)
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
		a, err = n.mergeAccumulators(n.Combine.ctx, a, v.Elm)
		if err != nil {
			return err
		}
	}
	return n.Out.ProcessElement(n.Combine.ctx, &FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: a, Timestamp: value.Timestamp})
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
func (n *ExtractOutput) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	if n.status != Active {
		return errors.Errorf("invalid status for combine extract %v: %v", n.UID, n.status)
	}
	out, err := n.extract(n.Combine.ctx, value.Elm2)
	if err != nil {
		return n.fail(err)
	}
	return n.Out.ProcessElement(n.Combine.ctx, &FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: out, Timestamp: value.Timestamp})
}

// ConvertToAccumulators is an executor for converting an input value to an accumulator value.
type ConvertToAccumulators struct {
	*Combine
}

func (n *ConvertToAccumulators) String() string {
	return fmt.Sprintf("ConvertToAccumulators[%v] Keyed:%v Out:%v", path.Base(n.Fn.Name()), n.UsesKey, n.Out.ID())
}

// ProcessElement accepts an input value and returns an accumulator containing that one value.
func (n *ConvertToAccumulators) ProcessElement(ctx context.Context, value *FullValue, values ...ReStream) error {
	if n.status != Active {
		return errors.Errorf("invalid status for combine convert %v: %v", n.UID, n.status)
	}
	a, err := n.newAccum(n.Combine.ctx, value.Elm)
	if err != nil {
		return n.fail(err)
	}

	first := true
	a, err = n.addInput(n.Combine.ctx, a, value.Elm, value.Elm2, value.Timestamp, first)
	if err != nil {
		return n.fail(err)
	}
	return n.Out.ProcessElement(n.Combine.ctx, &FullValue{Windows: value.Windows, Elm: value.Elm, Elm2: a, Timestamp: value.Timestamp})
}
