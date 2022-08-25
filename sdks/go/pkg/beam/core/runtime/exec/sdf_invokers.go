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
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

// This file contains invokers for SDF methods. These invokers are based off
// exec.invoker which is used for regular DoFns. Since exec.invoker is
// specialized for DoFns it cannot be used for SDF methods. Instead, these
// invokers pare down the functionality to only what is essential for
// executing SDF methods, including per-element optimizations.
//
// Each SDF method invoker in this file is specific to a certain method, but
// they are all used the same way. Create an invoker with new[Method]Invoker
// in the Up method of an exec.Unit, and then invoke it with Invoke. Finally,
// call Reset on it when the bundle ends in FinishBundle.
//
// These invokers are not thread-safe.

// cirInvoker is an invoker for CreateInitialRestriction.
type cirInvoker struct {
	fn   *funcx.Fn
	args []interface{} // Cache to avoid allocating new slices per-element.
	call func(elms *FullValue) (rest interface{})
}

func newCreateInitialRestrictionInvoker(fn *funcx.Fn) (*cirInvoker, error) {
	n := &cirInvoker{
		fn:   fn,
		args: make([]interface{}, len(fn.Param)),
	}
	if err := n.initCallFn(); err != nil {
		return nil, errors.WithContext(err, "sdf CreateInitialRestriction invoker")
	}
	return n, nil
}

func (n *cirInvoker) initCallFn() error {
	// Expects a signature of the form:
	// (key?, value) restriction
	// TODO(BEAM-9643): Link to full documentation.
	switch fnT := n.fn.Fn.(type) {
	case reflectx.Func1x1:
		n.call = func(elms *FullValue) interface{} {
			return fnT.Call1x1(elms.Elm)
		}
	case reflectx.Func2x1:
		n.call = func(elms *FullValue) interface{} {
			return fnT.Call2x1(elms.Elm, elms.Elm2)
		}
	default:
		switch len(n.fn.Param) {
		case 1:
			n.call = func(elms *FullValue) interface{} {
				n.args[0] = elms.Elm
				return n.fn.Fn.Call(n.args)[0]
			}
		case 2:
			n.call = func(elms *FullValue) interface{} {
				n.args[0] = elms.Elm
				n.args[1] = elms.Elm2
				return n.fn.Fn.Call(n.args)[0]
			}
		default:
			return errors.Errorf("CreateInitialRestriction fn %v has unexpected number of parameters: %v",
				n.fn.Fn.Name(), len(n.fn.Param))
		}
	}

	return nil
}

// Invoke calls CreateInitialRestriction with the given FullValue as the element
// and returns the resulting restriction.
func (n *cirInvoker) Invoke(elms *FullValue) (rest interface{}) {
	return n.call(elms)
}

// Reset zeroes argument entries in the cached slice to allow values to be
// garbage collected after the bundle ends.
func (n *cirInvoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
}

// srInvoker is an invoker for SplitRestriction.
type srInvoker struct {
	fn   *funcx.Fn
	args []interface{} // Cache to avoid allocating new slices per-element.
	call func(elms *FullValue, rest interface{}) (splits interface{})
}

func newSplitRestrictionInvoker(fn *funcx.Fn) (*srInvoker, error) {
	n := &srInvoker{
		fn:   fn,
		args: make([]interface{}, len(fn.Param)),
	}
	if err := n.initCallFn(); err != nil {
		return nil, errors.WithContext(err, "sdf SplitRestriction invoker")
	}
	return n, nil
}

func (n *srInvoker) initCallFn() error {
	// Expects a signature of the form:
	// (key?, value, restriction) []restriction
	// TODO(BEAM-9643): Link to full documentation.
	switch fnT := n.fn.Fn.(type) {
	case reflectx.Func2x1:
		n.call = func(elms *FullValue, rest interface{}) interface{} {
			return fnT.Call2x1(elms.Elm, rest)
		}
	case reflectx.Func3x1:
		n.call = func(elms *FullValue, rest interface{}) interface{} {
			return fnT.Call3x1(elms.Elm, elms.Elm2, rest)
		}
	default:
		switch len(n.fn.Param) {
		case 2:
			n.call = func(elms *FullValue, rest interface{}) interface{} {
				n.args[0] = elms.Elm
				n.args[1] = rest
				return n.fn.Fn.Call(n.args)[0]
			}
		case 3:
			n.call = func(elms *FullValue, rest interface{}) interface{} {
				n.args[0] = elms.Elm
				n.args[1] = elms.Elm2
				n.args[2] = rest
				return n.fn.Fn.Call(n.args)[0]
			}
		default:
			return errors.Errorf("SplitRestriction fn %v has unexpected number of parameters: %v",
				n.fn.Fn.Name(), len(n.fn.Param))
		}
	}
	return nil
}

// Invoke calls SplitRestriction given a FullValue containing an element and
// the associated restriction, and returns a slice of split restrictions.
func (n *srInvoker) Invoke(elms *FullValue, rest interface{}) (splits []interface{}) {
	ret := n.call(elms, rest)

	// Return value is an interface{}, but we need to convert it to a []interface{}.
	val := reflect.ValueOf(ret)
	s := make([]interface{}, 0, val.Len())
	for i := 0; i < val.Len(); i++ {
		s = append(s, val.Index(i).Interface())
	}
	return s
}

// Reset zeroes argument entries in the cached slice to allow values to be
// garbage collected after the bundle ends.
func (n *srInvoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
}

// rsInvoker is an invoker for RestrictionSize.
type rsInvoker struct {
	fn   *funcx.Fn
	args []interface{} // Cache to avoid allocating new slices per-element.
	call func(elms *FullValue, rest interface{}) (size float64)
}

func newRestrictionSizeInvoker(fn *funcx.Fn) (*rsInvoker, error) {
	n := &rsInvoker{
		fn:   fn,
		args: make([]interface{}, len(fn.Param)),
	}
	if err := n.initCallFn(); err != nil {
		return nil, errors.WithContext(err, "sdf RestrictionSize invoker")
	}
	return n, nil
}

func (n *rsInvoker) initCallFn() error {
	// Expects a signature of the form:
	// (key?, value, restriction) float64
	// TODO(BEAM-9643): Link to full documentation.
	switch fnT := n.fn.Fn.(type) {
	case reflectx.Func2x1:
		n.call = func(elms *FullValue, rest interface{}) float64 {
			return fnT.Call2x1(elms.Elm, rest).(float64)
		}
	case reflectx.Func3x1:
		n.call = func(elms *FullValue, rest interface{}) float64 {
			return fnT.Call3x1(elms.Elm, elms.Elm2, rest).(float64)
		}
	default:
		switch len(n.fn.Param) {
		case 2:
			n.call = func(elms *FullValue, rest interface{}) float64 {
				n.args[0] = elms.Elm
				n.args[1] = rest
				return n.fn.Fn.Call(n.args)[0].(float64)
			}
		case 3:
			n.call = func(elms *FullValue, rest interface{}) float64 {
				n.args[0] = elms.Elm
				n.args[1] = elms.Elm2
				n.args[2] = rest
				return n.fn.Fn.Call(n.args)[0].(float64)
			}
		default:
			return errors.Errorf("RestrictionSize fn %v has unexpected number of parameters: %v",
				n.fn.Fn.Name(), len(n.fn.Param))
		}
	}
	return nil
}

// Invoke calls RestrictionSize given a FullValue containing an element and
// the associated restriction, and returns a size.
func (n *rsInvoker) Invoke(elms *FullValue, rest interface{}) (size float64) {
	return n.call(elms, rest)
}

// Reset zeroes argument entries in the cached slice to allow values to be
// garbage collected after the bundle ends.
func (n *rsInvoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
}

// ctInvoker is an invoker for CreateTracker.
type ctInvoker struct {
	fn   *funcx.Fn
	args []interface{} // Cache to avoid allocating new slices per-element.
	call func(rest interface{}) sdf.RTracker
}

func newCreateTrackerInvoker(fn *funcx.Fn) (*ctInvoker, error) {
	n := &ctInvoker{
		fn:   fn,
		args: make([]interface{}, len(fn.Param)),
	}
	if err := n.initCallFn(); err != nil {
		return nil, errors.WithContext(err, "sdf CreateTracker invoker")
	}
	return n, nil
}

func (n *ctInvoker) initCallFn() error {
	// Expects a signature of the form:
	// (restriction) sdf.RTracker
	// TODO(BEAM-9643): Link to full documentation.
	switch fnT := n.fn.Fn.(type) {
	case reflectx.Func1x1:
		n.call = func(rest interface{}) sdf.RTracker {
			return fnT.Call1x1(rest).(sdf.RTracker)
		}
	default:
		if len(n.fn.Param) != 1 {
			return errors.Errorf("CreateTracker fn %v has unexpected number of parameters: %v",
				n.fn.Fn.Name(), len(n.fn.Param))
		}
		n.call = func(rest interface{}) sdf.RTracker {
			n.args[0] = rest
			return n.fn.Fn.Call(n.args)[0].(sdf.RTracker)
		}
	}
	return nil
}

// Invoke calls CreateTracker given a restriction and returns an sdf.RTracker.
func (n *ctInvoker) Invoke(rest interface{}) sdf.RTracker {
	return n.call(rest)
}

// Reset zeroes argument entries in the cached slice to allow values to be
// garbage collected after the bundle ends.
func (n *ctInvoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
}

// trInvoker is an invoker for TruncateRestriction.
type trInvoker struct {
	fn   *funcx.Fn
	args []interface{}
	call func(rest interface{}, elms *FullValue) (pair interface{})
}

func defaultTruncateRestriction(restTracker interface{}) (newRest interface{}) {
	if tracker, ok := restTracker.(sdf.BoundableRTracker); ok && !tracker.IsBounded() {
		return nil
	}
	return restTracker.(sdf.RTracker).GetRestriction()
}

func newTruncateRestrictionInvoker(fn *funcx.Fn) (*trInvoker, error) {
	n := &trInvoker{
		fn:   fn,
		args: make([]interface{}, len(fn.Param)),
	}
	if err := n.initCallFn(); err != nil {
		return nil, errors.WithContext(err, "sdf TruncateRestriction invoker")
	}
	return n, nil
}

func newDefaultTruncateRestrictionInvoker() (*trInvoker, error) {
	n := &trInvoker{}
	n.call = func(rest interface{}, elms *FullValue) interface{} {
		return defaultTruncateRestriction(rest)
	}
	return n, nil
}

func (n *trInvoker) initCallFn() error {
	// Expects a signature of the form:
	// (key?, value, restriction) []restriction
	// TODO(BEAM-9643): Link to full documentation.
	switch fnT := n.fn.Fn.(type) {
	case reflectx.Func2x1:
		n.call = func(rest interface{}, elms *FullValue) interface{} {
			return fnT.Call2x1(rest, elms.Elm)
		}
	case reflectx.Func3x1:
		n.call = func(rest interface{}, elms *FullValue) interface{} {
			return fnT.Call3x1(rest, elms.Elm, elms.Elm2)
		}
	default:
		switch len(n.fn.Param) {
		case 2:
			n.call = func(rest interface{}, elms *FullValue) interface{} {
				n.args[0] = rest
				n.args[1] = elms.Elm
				return n.fn.Fn.Call(n.args)[0]
			}
		case 3:
			n.call = func(rest interface{}, elms *FullValue) interface{} {
				n.args[0] = rest
				n.args[1] = elms.Elm
				n.args[2] = elms.Elm2
				return n.fn.Fn.Call(n.args)[0]
			}
		default:
			return errors.Errorf("TruncateRestriction fn %v has unexpected number of parameters: %v",
				n.fn.Fn.Name(), len(n.fn.Param))
		}
	}
	return nil
}

// Invoke calls TruncateRestriction given a FullValue containing an element and
// the associated restriction tracker, and returns a truncated restriction.
func (n *trInvoker) Invoke(rt interface{}, elms *FullValue) (rest interface{}) {
	return n.call(rt, elms)
}

// Reset zeroes argument entries in the cached slice to allow values to be
// garbage collected after the bundle ends.
func (n *trInvoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
}

// cweInvoker is an invoker for CreateWatermarkEstimator.
type cweInvoker struct {
	fn   *funcx.Fn
	args []interface{} // Cache to avoid allocating new slices per-element.
	call func(rest interface{}) sdf.WatermarkEstimator
}

func newCreateWatermarkEstimatorInvoker(fn *funcx.Fn) (*cweInvoker, error) {
	n := &cweInvoker{
		fn:   fn,
		args: make([]interface{}, len(fn.Param)),
	}
	if err := n.initCallFn(); err != nil {
		return nil, errors.WithContext(err, "sdf CreateWatermarkEstimator invoker")
	}
	return n, nil
}

func (n *cweInvoker) initCallFn() error {
	// Expects a signature of the form:
	// (watermarkState?) sdf.WatermarkEstimator
	switch fnT := n.fn.Fn.(type) {
	case reflectx.Func0x1:
		n.call = func(rest interface{}) sdf.WatermarkEstimator {
			return fnT.Call0x1().(sdf.WatermarkEstimator)
		}
	case reflectx.Func1x1:
		n.call = func(rest interface{}) sdf.WatermarkEstimator {
			return fnT.Call1x1(rest).(sdf.WatermarkEstimator)
		}
	default:
		switch len(n.fn.Param) {
		case 0:
			n.call = func(rest interface{}) sdf.WatermarkEstimator {
				return n.fn.Fn.Call(n.args)[0].(sdf.WatermarkEstimator)
			}
		case 1:
			n.call = func(rest interface{}) sdf.WatermarkEstimator {
				n.args[0] = rest
				return n.fn.Fn.Call(n.args)[0].(sdf.WatermarkEstimator)
			}
		default:
			return errors.Errorf("CreateWatermarkEstimator fn %v has unexpected number of parameters: %v",
				n.fn.Fn.Name(), len(n.fn.Param))
		}
	}
	return nil
}

// Invoke calls CreateWatermarkEstimator given a restriction and returns an sdf.WatermarkEstimator.
func (n *cweInvoker) Invoke(rest interface{}) sdf.WatermarkEstimator {
	return n.call(rest)
}

// Reset zeroes argument entries in the cached slice to allow values to be
// garbage collected after the bundle ends.
func (n *cweInvoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
}

// iwesInvoker is an invoker for InitialWatermarkEstimatorState.
type iwesInvoker struct {
	fn   *funcx.Fn
	args []interface{} // Cache to avoid allocating new slices per-element.
	call func(rest interface{}, elms *FullValue) interface{}
}

func newInitialWatermarkEstimatorStateInvoker(fn *funcx.Fn) (*iwesInvoker, error) {
	args := []interface{}{}
	if fn != nil {
		args = make([]interface{}, len(fn.Param))
	}
	n := &iwesInvoker{
		fn:   fn,
		args: args,
	}
	if err := n.initCallFn(); err != nil {
		return nil, errors.WithContext(err, "sdf InitialWatermarkEstimatorState invoker")
	}
	return n, nil
}

func (n *iwesInvoker) initCallFn() error {
	// If no WatermarkEstimatorState function is defined, we'll use a default implementation that just returns false as the state.
	if n.fn == nil {
		n.call = func(rest interface{}, elms *FullValue) interface{} {
			return false
		}
		return nil
	}
	// Expects a signature of the form:
	// (typex.EventTime, restrictionTracker, key?, value) interface{}
	switch fnT := n.fn.Fn.(type) {
	case reflectx.Func3x1:
		n.call = func(rest interface{}, elms *FullValue) interface{} {
			return fnT.Call3x1(elms.Timestamp, rest, elms.Elm)
		}
	case reflectx.Func4x1:
		n.call = func(rest interface{}, elms *FullValue) interface{} {
			return fnT.Call4x1(elms.Timestamp, rest, elms.Elm, elms.Elm2)
		}
	default:
		switch len(n.fn.Param) {
		case 3:
			n.call = func(rest interface{}, elms *FullValue) interface{} {
				n.args[0] = elms.Timestamp
				n.args[1] = rest
				n.args[2] = elms.Elm
				return n.fn.Fn.Call(n.args)[0]
			}
		case 4:
			n.call = func(rest interface{}, elms *FullValue) interface{} {
				n.args[0] = elms.Timestamp
				n.args[1] = rest
				n.args[2] = elms.Elm
				n.args[3] = elms.Elm2
				return n.fn.Fn.Call(n.args)[0]
			}
		default:
			return errors.Errorf("InitialWatermarkEstimatorState fn %v has unexpected number of parameters: %v",
				n.fn.Fn.Name(), len(n.fn.Param))
		}
	}
	return nil
}

// Invoke calls InitialWatermarkEstimatorState given a restriction and returns an sdf.RTracker.
func (n *iwesInvoker) Invoke(rest interface{}, elms *FullValue) interface{} {
	return n.call(rest, elms)
}

// Reset zeroes argument entries in the cached slice to allow values to be
// garbage collected after the bundle ends.
func (n *iwesInvoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
}

// wesInvoker is an invoker for WatermarkEstimatorState.
type wesInvoker struct {
	fn   *funcx.Fn
	args []interface{} // Cache to avoid allocating new slices per-element.
	call func(we sdf.WatermarkEstimator) interface{}
}

func newWatermarkEstimatorStateInvoker(fn *funcx.Fn) (*wesInvoker, error) {
	args := []interface{}{}
	if fn != nil {
		args = make([]interface{}, len(fn.Param))
	}
	n := &wesInvoker{
		fn:   fn,
		args: args,
	}
	if err := n.initCallFn(); err != nil {
		return nil, errors.WithContext(err, "sdf WatermarkEstimatorState invoker")
	}
	return n, nil
}

func (n *wesInvoker) initCallFn() error {
	// If no WatermarkEstimatorState function is defined, we'll use a default implementation that just returns false as the state.
	if n.fn == nil {
		n.call = func(we sdf.WatermarkEstimator) interface{} {
			return false
		}
		return nil
	}
	// Expects a signature of the form:
	// (state) sdf.WatermarkEstimator
	switch fnT := n.fn.Fn.(type) {
	case reflectx.Func1x1:
		n.call = func(we sdf.WatermarkEstimator) interface{} {
			return fnT.Call1x1(we)
		}
	default:
		switch len(n.fn.Param) {
		case 1:
			n.call = func(we sdf.WatermarkEstimator) interface{} {
				n.args[0] = we
				return n.fn.Fn.Call(n.args)[0]
			}
		default:
			return errors.Errorf("WatermarkEstimatorState fn %v has unexpected number of parameters: %v",
				n.fn.Fn.Name(), len(n.fn.Param))
		}
	}
	return nil
}

// Invoke calls WatermarkEstimatorState given a restriction and returns an sdf.RTracker.
func (n *wesInvoker) Invoke(we sdf.WatermarkEstimator) interface{} {
	return n.call(we)
}

// Reset zeroes argument entries in the cached slice to allow values to be
// garbage collected after the bundle ends.
func (n *wesInvoker) Reset() {
	for i := range n.args {
		n.args[i] = nil
	}
}
