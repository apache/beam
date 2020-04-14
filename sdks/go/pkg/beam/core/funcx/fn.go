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

package funcx

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// Note that we can't tell the difference between K, V and V, S before binding.

// FnParamKind represents the kinds of parameters a user function may take.
type FnParamKind int

const (
	// FnIllegal is an illegal function input parameter type.
	FnIllegal FnParamKind = 0x0
	// FnContext marks a function input parameter of type context.Context.
	FnContext FnParamKind = 0x1
	// FnEventTime indicates a function input parameter of type typex.EventTime.
	FnEventTime FnParamKind = 0x2
	// FnValue indicates a function input parameter of an ordinary Go type.
	FnValue FnParamKind = 0x4
	// FnIter indicates a function input parameter that is an iterator.
	// Examples of iterators:
	//       "func (*int) bool"
	//       "func (*string, *T) bool"
	// If there are 2 parameters, a KV input is implied.
	FnIter FnParamKind = 0x08
	// FnReIter indicates a function input parameter that is a reiterable
	// iterator.
	// The function signature is a function returning a function matching
	// the iterator signature.
	//   "func() func (*int) bool"
	//   "func() func (*string, *T) bool"
	// are reiterable versions of the FnIter examples.
	FnReIter FnParamKind = 0x10
	// FnEmit indicates a function input parameter that is an emitter.
	// Examples of emitters:
	//       "func (int)"
	//       "func (string, T)"
	//       "func (EventTime, int)"
	//       "func (EventTime, string, T)"
	// If there are 2 regular parameters, a KV output is implied. An optional
	// EventTime is allowed as well. Emitters cannot fail.
	FnEmit FnParamKind = 0x20
	// FnType indicates a function input parameter that is a type for a coder. It
	// is only valid for coders.
	FnType FnParamKind = 0x40
	// FnWindow indicates a function input parameter that implements typex.Window.
	FnWindow FnParamKind = 0x80
	// FnRTracker indicates a function input parameter that implements
	// sdf.RTracker.
	FnRTracker FnParamKind = 0x100
)

func (k FnParamKind) String() string {
	switch k {
	case FnContext:
		return "Context"
	case FnEventTime:
		return "EventTime"
	case FnValue:
		return "Value"
	case FnIter:
		return "Iter"
	case FnReIter:
		return "ReIter"
	case FnEmit:
		return "Emit"
	case FnType:
		return "Type"
	case FnWindow:
		return "Window"
	case FnRTracker:
		return "RTracker"
	default:
		return fmt.Sprintf("%v", int(k))
	}
}

// FnParam captures the kind and type of a single user function parameter.
type FnParam struct {
	Kind FnParamKind
	T    reflect.Type
}

// ReturnKind represents the kinds of return values a user function may provide.
type ReturnKind int

// The supported types of ReturnKind.
const (
	RetIllegal   ReturnKind = 0x0
	RetEventTime ReturnKind = 0x1
	RetValue     ReturnKind = 0x2
	RetError     ReturnKind = 0x4
	RetRTracker  ReturnKind = 0x8
)

func (k ReturnKind) String() string {
	switch k {
	case RetError:
		return "Error"
	case RetRTracker:
		return "RTracker"
	case RetEventTime:
		return "EventTime"
	case RetValue:
		return "Value"
	default:
		return fmt.Sprintf("%v", int(k))
	}
}

// ReturnParam captures the kind and type of a single user function return value.
type ReturnParam struct {
	Kind ReturnKind
	T    reflect.Type
}

// Fn is the reflected user function or method, preprocessed. This wrapper
// is useful both at graph construction time as well as execution time.
type Fn struct {
	Fn reflectx.Func

	Param []FnParam
	Ret   []ReturnParam
}

// Context returns (index, true) iff the function expects a context.Context.
// The context should be the first parameter by convention.
func (u *Fn) Context() (pos int, exists bool) {
	for i, p := range u.Param {
		if p.Kind == FnContext {
			return i, true
		}
	}
	return -1, false
}

// Emits returns (index, num, true) iff the function expects one or more
// emitters. The index returned is the index of the first emitter param in the
// signature. The num return value is the number of emitters in the signature.
// When there are multiple emitters in the signature, they will all be located
// contiguously, so the range of emitter params is [index, index+num).
func (u *Fn) Emits() (pos int, num int, exists bool) {
	pos = -1
	exists = false
	for i, p := range u.Param {
		if !exists && p.Kind == FnEmit {
			// This should execute when hitting the first emitter.
			pos = i
			num = 1
			exists = true
		} else if exists && p.Kind == FnEmit {
			// Subsequent emitters after the first.
			num++
		} else if exists {
			// Breaks out when no emitters are left.
			break
		}
	}
	return pos, num, exists
}

// Inputs returns (index, num, true) iff the function expects one or more
// inputs, consisting of the main input followed by any number of side inputs.
// The index returned is the index of the first input, which is always the main
// input. The num return value is the number of total inputs in the signature.
// The main input and all side inputs are located contiguously
func (u *Fn) Inputs() (pos int, num int, exists bool) {
	pos = -1
	exists = false
	for i, p := range u.Param {
		if !exists && (p.Kind == FnValue || p.Kind == FnIter || p.Kind == FnReIter) {
			// This executes on hitting the first input.
			pos = i
			num = 1
			exists = true
		} else if exists && (p.Kind == FnValue || p.Kind == FnIter || p.Kind == FnReIter) {
			// Subsequent inputs after the first.
			num++
		} else if exists {
			break
		}
	}
	return pos, num, exists
}

// Type returns (index, true) iff the function expects a reflect.FullType.
func (u *Fn) Type() (pos int, exists bool) {
	for i, p := range u.Param {
		if p.Kind == FnType {
			return i, true
		}
	}
	return -1, false
}

// EventTime returns (index, true) iff the function expects an event timestamp.
func (u *Fn) EventTime() (pos int, exists bool) {
	for i, p := range u.Param {
		if p.Kind == FnEventTime {
			return i, true
		}
	}
	return -1, false
}

// Window returns (index, true) iff the function expects a window.
func (u *Fn) Window() (pos int, exists bool) {
	for i, p := range u.Param {
		if p.Kind == FnWindow {
			return i, true
		}
	}
	return -1, false
}

// RTracker returns (index, true) iff the function expects an sdf.RTracker.
func (u *Fn) RTracker() (pos int, exists bool) {
	for i, p := range u.Param {
		if p.Kind == FnRTracker {
			return i, true
		}
	}
	return -1, false
}

// Error returns (index, true) iff the function returns an error.
func (u *Fn) Error() (pos int, exists bool) {
	for i, p := range u.Ret {
		if p.Kind == RetError {
			return i, true
		}
	}
	return -1, false
}

// OutEventTime returns (index, true) iff the function returns an event timestamp.
func (u *Fn) OutEventTime() (pos int, exists bool) {
	for i, p := range u.Ret {
		if p.Kind == RetEventTime {
			return i, true
		}
	}
	return -1, false
}

// Params returns the parameter indices that matches the given mask.
func (u *Fn) Params(mask FnParamKind) []int {
	var ret []int
	for i, p := range u.Param {
		if (p.Kind & mask) != 0 {
			ret = append(ret, i)
		}
	}
	return ret
}

// Returns returns the return indices that matches the given mask.
func (u *Fn) Returns(mask ReturnKind) []int {
	var ret []int
	for i, p := range u.Ret {
		if (p.Kind & mask) != 0 {
			ret = append(ret, i)
		}
	}
	return ret
}

func (u *Fn) String() string {
	return fmt.Sprintf("{Fn:{Name:%v Kind:%v} Param:%+v Ret:%+v}", u.Fn.Name(), u.Fn.Type(), u.Param, u.Ret)
}

// New returns a Fn from a user function, if valid. Closures and dynamically
// created functions are considered valid here, but will be rejected if they
// are attempted to be serialized.
func New(fn reflectx.Func) (*Fn, error) {
	var param []FnParam
	for i := 0; i < fn.Type().NumIn(); i++ {
		t := fn.Type().In(i)

		kind := FnIllegal
		switch {
		case t == reflectx.Context:
			kind = FnContext
		case t == typex.EventTimeType:
			kind = FnEventTime
		case t.Implements(typex.WindowType):
			kind = FnWindow
		case t == reflectx.Type:
			kind = FnType
		case t.Implements(reflect.TypeOf((*sdf.RTracker)(nil)).Elem()):
			kind = FnRTracker
		case typex.IsContainer(t), typex.IsConcrete(t), typex.IsUniversal(t):
			kind = FnValue
		case IsEmit(t):
			kind = FnEmit
		case IsIter(t):
			kind = FnIter
		case IsReIter(t):
			kind = FnReIter
		default:
			return nil, errors.Errorf("bad parameter type for %s: %v", fn.Name(), t)
		}

		param = append(param, FnParam{Kind: kind, T: t})
	}

	var ret []ReturnParam
	for i := 0; i < fn.Type().NumOut(); i++ {
		t := fn.Type().Out(i)

		kind := RetIllegal
		switch {
		case t == reflectx.Error:
			kind = RetError
		case t.Implements(reflect.TypeOf((*sdf.RTracker)(nil)).Elem()):
			kind = RetRTracker
		case t == typex.EventTimeType:
			kind = RetEventTime
		case typex.IsContainer(t), typex.IsConcrete(t), typex.IsUniversal(t):
			kind = RetValue
		default:
			return nil, errors.Errorf("bad return type for %s: %v", fn.Name(), t)
		}

		ret = append(ret, ReturnParam{Kind: kind, T: t})
	}

	u := &Fn{Fn: fn, Param: param, Ret: ret}

	if err := validateOrder(u); err != nil {
		return nil, err
	}
	return u, nil
}

// SubParams returns the subsequence of the given params with the given
// indices.
func SubParams(list []FnParam, indices ...int) []FnParam {
	var ret []FnParam
	for _, index := range indices {
		ret = append(ret, list[index])
	}
	return ret
}

// SubReturns returns the subsequence of the given return params with
// the given indices.
func SubReturns(list []ReturnParam, indices ...int) []ReturnParam {
	var ret []ReturnParam
	for _, index := range indices {
		ret = append(ret, list[index])
	}
	return ret
}

// The order of present parameters and return values must be as follows:
// func(FnContext?, FnWindow?, FnEventTime?, FnType?, FnRTracker?, (FnValue, SideInput*)?, FnEmit*) (RetEventTime?, RetOutput?, RetError?)
//     where ? indicates 0 or 1, and * indicates any number.
//     and  a SideInput is one of FnValue or FnIter or FnReIter
// Note: Fns with inputs must have at least one FnValue as the main input.
func validateOrder(u *Fn) error {
	paramState := psStart
	var err error
	// Validate the parameter ordering.
	for i, p := range u.Param {
		if paramState, err = nextParamState(paramState, p.Kind); err != nil {
			return errors.WithContextf(err, "validating parameter %d for %s", i, u.Fn.Name())
		}
	}
	// Validate the return value ordering.
	retState := rsStart
	for i, r := range u.Ret {
		if retState, err = nextRetState(retState, r.Kind); err != nil {
			return errors.WithContextf(err, "validating return value %d for %s", i, u.Fn.Name())
		}
	}
	return nil
}

var (
	errContextParam             = errors.New("may only have a single context.Context parameter and it must be the first parameter")
	errWindowParamPrecedence    = errors.New("may only have a single Window parameter and it must precede the EventTime and main input parameter")
	errEventTimeParamPrecedence = errors.New("may only have a single beam.EventTime parameter and it must precede the main input parameter")
	errReflectTypePrecedence    = errors.New("may only have a single reflect.Type parameter and it must precede the main input parameter")
	errRTrackerPrecedence       = errors.New("may only have a single sdf.RTracker parameter and it must precede the main input parameter")
	errInputPrecedence          = errors.New("inputs parameters must precede emit function parameters")
)

type paramState int

const (
	psStart paramState = iota
	psContext
	psWindow
	psEventTime
	psType
	psInput
	psOutput
	psRTracker
)

func nextParamState(cur paramState, transition FnParamKind) (paramState, error) {
	switch cur {
	case psStart:
		switch transition {
		case FnContext:
			return psContext, nil
		case FnWindow:
			return psWindow, nil
		case FnEventTime:
			return psEventTime, nil
		case FnType:
			return psType, nil
		case FnRTracker:
			return psRTracker, nil
		}
	case psContext:
		switch transition {
		case FnWindow:
			return psWindow, nil
		case FnEventTime:
			return psEventTime, nil
		case FnType:
			return psType, nil
		case FnRTracker:
			return psRTracker, nil
		}
	case psWindow:
		switch transition {
		case FnEventTime:
			return psEventTime, nil
		case FnType:
			return psType, nil
		case FnRTracker:
			return psRTracker, nil
		}
	case psEventTime:
		switch transition {
		case FnType:
			return psType, nil
		case FnRTracker:
			return psRTracker, nil
		}
	case psType:
		switch transition {
		case FnRTracker:
			return psRTracker, nil
		}
	case psRTracker:
		// Completely handled by the default clause
	case psInput:
		switch transition {
		case FnIter, FnReIter:
			return psInput, nil
		}
	case psOutput:
		switch transition {
		case FnValue, FnIter, FnReIter:
			return -1, errInputPrecedence
		}
	}
	// Default transition cases to reduce duplication above
	switch transition {
	case FnContext:
		return -1, errContextParam
	case FnWindow:
		return -1, errWindowParamPrecedence
	case FnEventTime:
		return -1, errEventTimeParamPrecedence
	case FnType:
		return -1, errReflectTypePrecedence
	case FnRTracker:
		return -1, errRTrackerPrecedence
	case FnIter, FnReIter, FnValue:
		return psInput, nil
	case FnEmit:
		return psOutput, nil
	default:
		panic(fmt.Sprintf("library error, unknown ParamKind: %v", transition))
	}
}

var (
	errEventTimeRetPrecedence = errors.New("beam.EventTime must be first return parameter")
	errErrorPrecedence        = errors.New("error must be the final return parameter")
)

type retState int

const (
	rsStart retState = iota
	rsEventTime
	rsOutput
	rsError
)

func nextRetState(cur retState, transition ReturnKind) (retState, error) {
	switch cur {
	case rsStart:
		switch transition {
		case RetEventTime:
			return rsEventTime, nil
		}
	case rsEventTime, rsOutput:
		// Identical to the default cases.
	case rsError:
		// This is a terminal state. No valid transitions. error must be the final return value.
		return -1, errErrorPrecedence
	}
	// The default cases to avoid repetition.
	switch transition {
	case RetEventTime:
		return -1, errEventTimeRetPrecedence
	case RetValue, RetRTracker:
		return rsOutput, nil
	case RetError:
		return rsError, nil
	default:
		panic(fmt.Sprintf("library error, unknown ReturnKind: %v", transition))
	}
}
