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
	"errors"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// TODO(herohde) 4/14/2017: various side input forms + aggregators/counters.
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
)

func (k ReturnKind) String() string {
	switch k {
	case RetError:
		return "Error"
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
	return fmt.Sprintf("%+v", *u)
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
		case t == reflectx.Type:
			kind = FnType
		case typex.IsContainer(t), typex.IsConcrete(t), typex.IsUniversal(t):
			kind = FnValue
		case IsEmit(t):
			kind = FnEmit
		case IsIter(t):
			kind = FnIter
		case IsReIter(t):
			kind = FnReIter
		default:
			return nil, fmt.Errorf("bad parameter type for %s: %v", fn.Name(), t)
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
		case t == typex.EventTimeType:
			kind = RetEventTime
		case typex.IsContainer(t), typex.IsConcrete(t), typex.IsUniversal(t):
			kind = RetValue
		default:
			return nil, fmt.Errorf("bad return type for %s: %v", fn.Name(), t)
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
// func(FnContext?, FnEventTime?, FnType?, (FnValue, SideInput*)?, FnEmit*) (RetEventTime?, RetEventTime?, RetError?)
//     where ? indicates 0 or 1, and * indicates any number.
//     and  a SideInput is one of FnValue or FnIter or FnReIter
// Note: Fns with inputs must have at least one FnValue as the main input.
func validateOrder(u *Fn) error {
	paramState := psStart
	var err error
	// Validate the parameter ordering.
	for i, p := range u.Param {
		if paramState, err = nextParamState(paramState, p.Kind); err != nil {
			return fmt.Errorf("%s at parameter %d for %s", err.Error(), i, u.Fn.Name())
		}
	}
	// Validate the return value ordering.
	retState := rsStart
	for i, r := range u.Ret {
		if retState, err = nextRetState(retState, r.Kind); err != nil {
			return fmt.Errorf("%s for return value %d for %s", err.Error(), i, u.Fn.Name())
		}
	}
	return nil
}

var (
	errContextParam             = errors.New("may only have a single context.Context parameter and it must be the first parameter")
	errEventTimeParamPrecedence = errors.New("may only have a single beam.EventTime parameter and it must preceed the main input parameter")
	errReflectTypePrecedence    = errors.New("may only have a single reflect.Type parameter and it must preceed the main input parameter")
	errSideInputPrecedence      = errors.New("side input parameters must follow main input parameter")
	errInputPrecedence          = errors.New("inputs parameters must preceed emit function parameters")
)

type paramState int

const (
	psStart paramState = iota
	psContext
	psEventTime
	psType
	psInput
	psOutput
)

func nextParamState(cur paramState, transition FnParamKind) (paramState, error) {
	switch cur {
	case psStart:
		switch transition {
		case FnContext:
			return psContext, nil
		case FnEventTime:
			return psEventTime, nil
		case FnType:
			return psType, nil
		}
	case psContext:
		switch transition {
		case FnEventTime:
			return psEventTime, nil
		case FnType:
			return psType, nil
		}
	case psEventTime:
		switch transition {
		case FnType:
			return psType, nil
		}
	case psType:
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
	case FnEventTime:
		return -1, errEventTimeParamPrecedence
	case FnType:
		return -1, errReflectTypePrecedence
	case FnValue:
		return psInput, nil
	case FnIter, FnReIter:
		return -1, errSideInputPrecedence
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
	case RetValue:
		return rsOutput, nil
	case RetError:
		return rsError, nil
	default:
		panic(fmt.Sprintf("library error, unknown ReturnKind: %v", transition))
	}
}
