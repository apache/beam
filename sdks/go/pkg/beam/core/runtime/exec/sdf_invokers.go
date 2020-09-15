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
	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
	"reflect"
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
