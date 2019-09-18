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
	// FnInput is a placeholder enum only to be used in funcx.SignatureRules for
	// representing any of FnValue, FnIter, and FnReIter. In any other context,
	// this enum should behave like FnIllegal.
	FnInput FnParamKind = 0x100
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
	case FnInput:
		return "Input"
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

// Window returns (index, true) iff the function expects a window.
func (u *Fn) Window() (pos int, exists bool) {
	for i, p := range u.Param {
		if p.Kind == FnWindow {
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

// NewUnvalidated is identical to funcx.New except that the produced Fn has not
// had their signature validated for proper ordering or counts. This function is
// meant to be paired with funcx.ValidateSignature in order to create and
// validate Fns from the methods of structural DoFns, which may have different
// parameter order or count requirements than other DoFns.
func NewUnvalidated(fn reflectx.Func) (*Fn, error) {
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
	return u, nil
}

// New returns a Fn from a user function, if valid. Closures and dynamically
// created functions are considered valid here, but will be rejected if they
// are attempted to be serialized.
func New(fn reflectx.Func) (*Fn, error) {
	u, err := NewUnvalidated(fn)
	if err != nil {
		return nil, err
	}
	if err := validateSignatureDefault(u); err != nil {
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

type Count int

const (
	// CountNone indicates that the associated element kind should not be present.
	CountNone Count = iota
	// CountOpt indicates that the associated element kind is optional and can
	// be present either 0 or 1 times, but not more than 1.
	CountOpt
	// CountAny indicates that the associated element kind can be present in any
	// number, including 0.
	CountAny
)

// SignatureRules defines the rules to use when validating a Fn signature, which
// includes order and count of parameters and return values. The rules for order
// are a slice representing the order in which to expect elements The rules for
// count map elements kinds with Count enums representing the allowed count. In
// both of these rules, omitted kinds of elements are assumed to be invalid,
// and will cause validation errors if present in the signature.
type SignatureRules struct {
	Params  []ParamCount
	Returns []ReturnCount
}

type ParamCount struct {
	kind  FnParamKind
	count Count
}

type ReturnCount struct {
	kind  ReturnKind
	count Count
}

// func(FnContext?, FnWindow?, FnEventTime?, FnType?, (FnValue, SideInput*)?, FnEmit*) (RetEventTime?, RetEventTime?, RetError?)
func DefaultSignatureRules() SignatureRules {
	return SignatureRules{
		Params: []ParamCount{
			{FnContext, CountOpt},
			{FnWindow, CountOpt},
			{FnEventTime, CountOpt},
			{FnType, CountOpt},
			{FnInput, CountAny},
			{FnEmit, CountAny},
		},
		Returns: []ReturnCount{
			{RetEventTime, CountOpt},
			{RetValue, CountAny},
			{RetError, CountOpt},
		},
	}
}

// ValidateSignature validates the order and count of parameters and return
// values in a Fn's signature, based on the input rules.
func ValidateSignature(u *Fn, rules SignatureRules) error {
	err := validateOrder(u, rules)
	if err != nil {
		return err
	}
	err = validateCount(u, rules)
	if err != nil {
		return err
	}

	return nil
}

func validateOrder(u *Fn, rules SignatureRules) error {
	// Parse rules before delegating to implementation split by params/returns.
	var pOrder []FnParamKind
	for _, pc := range rules.Params {
		pOrder = append(pOrder, pc.kind)
	}
	err := validateOrderParams(u.Param, pOrder)
	if err != nil {
		return err
	}

	var rOrder []ReturnKind
	for _, rc := range rules.Returns {
		rOrder = append(rOrder, rc.kind)
	}
	err = validateOrderReturns(u.Ret, rOrder)
	if err != nil {
		return err
	}
	return nil
}

// kindMatches is a helper function that compares FnParamKind equality, but
// accounts for FnInput matching FnValue, FnIter, and FnReIter.
func kindMatches(got FnParamKind, want FnParamKind) bool {
	if want == FnInput {
		return got == FnValue || got == FnIter || got == FnReIter
	} else {
		return got == want
	}
}

func validateOrderParams(params []FnParam, order []FnParamKind) error {
	i := 0
	for _, kind := range order {
		for i < len(params) && kindMatches(params[i].Kind, kind) {
			i++
		}
		if i >= len(params) {
			return nil
		}
	}

	return &outOfOrderError{i: i, pKind: params[i].Kind, pOrder: order}
}

func validateOrderReturns(returns []ReturnParam, order []ReturnKind) error {
	i := 0
	for _, kind := range order {
		for i < len(returns) && returns[i].Kind == kind {
			i++
		}
		if i >= len(returns) {
			return nil
		}
	}

	return &outOfOrderError{i: i, rKind: returns[i].Kind, rOrder: order}
}

func validateCount(u *Fn, rules SignatureRules) error {
	// Parse rules before delegating to implementation split by params/returns.
	pCount := make(map[FnParamKind]Count)
	for _, pc := range rules.Params {
		pCount[pc.kind] = pc.count
	}
	err := validateCountParams(u.Param, pCount)
	if err != nil {
		return err
	}

	rCount := make(map[ReturnKind]Count)
	for _, rc := range rules.Returns {
		rCount[rc.kind] = rc.count
	}
	err = validateCountReturns(u.Ret, rCount)
	if err != nil {
		return err
	}
	return nil
}

func validateCountParams(params []FnParam, count map[FnParamKind]Count) error {
	paramCounts := make(map[FnParamKind]int)
	for _, param := range params {
		// To handle FnInputs in counts, we squash FnValues, FnIters, and FnReIters
		// all into FnInput
		kind := param.Kind
		if kind == FnValue || kind == FnIter || kind == FnReIter {
			kind = FnInput
		}
		paramCounts[kind]++
	}
	for kind, num := range paramCounts {
		switch expected := count[kind]; expected {
		case CountNone:
			if num > 0 {
				return &incorrectCountError{pKind: kind, expected: expected, actual: num}
			}
		case CountOpt:
			if num > 1 {
				return &incorrectCountError{pKind: kind, expected: expected, actual: num}
			}
		}
	}
	return nil
}

func validateCountReturns(returns []ReturnParam, count map[ReturnKind]Count) error {
	returnCounts := make(map[ReturnKind]int)
	for _, ret := range returns {
		returnCounts[ret.Kind]++
	}
	for kind, num := range returnCounts {
		switch expected := count[kind]; expected {
		case CountNone:
			if num > 0 {
				return &incorrectCountError{rKind: kind, expected: expected, actual: num}
			}
		case CountOpt:
			if num > 1 {
				return &incorrectCountError{rKind: kind, expected: expected, actual: num}
			}
		}
	}
	return nil
}

// validateSignatureDefault contains the original, default implementation for
// validating the order in a Fn's signature, compared to the newer
// ValidateSignature.
//
// The order of present parameters and return values must be as follows:
// func(FnContext?, FnWindow?, FnEventTime?, FnType?, (FnValue, SideInput*)?, FnEmit*) (RetEventTime?, RetValue*, RetError?)
//     where ? indicates 0 or 1, and * indicates any number.
//     and  a SideInput is one of FnValue or FnIter or FnReIter
// Note: Fns with inputs must have at least one FnValue as the main input.
func validateSignatureDefault(u *Fn) error {
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
	errSideInputPrecedence      = errors.New("side input parameters must follow main input parameter")
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
		}
	case psContext:
		switch transition {
		case FnWindow:
			return psWindow, nil
		case FnEventTime:
			return psEventTime, nil
		case FnType:
			return psType, nil
		}
	case psWindow:
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
	case FnWindow:
		return -1, errWindowParamPrecedence
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

// outOfOrderError is a helper struct for formatting errors that occur when
// validating order for parameters or return values. THe struct assumes that
// either pKind or rKind (but not both) are set to a non-illegal value,
// and that the corresponding pOrder or rOrder is present. Failing to fulfill
// those assumptions can lead to undefined behavior.
type outOfOrderError struct {
	pKind  FnParamKind   // The out of order param's kind.
	rKind  ReturnKind    // The out of order return value's kind.
	i      int           // The out of order value's index.
	pOrder []FnParamKind // The expected order of param kinds.
	rOrder []ReturnKind  // The expected order of return value kinds.
}

func (e *outOfOrderError) Error() string {
	var sigPart, kindStr, orderStr string
	if e.pKind != FnIllegal {
		sigPart = "parameter"
		kindStr = e.pKind.String()
		orderStr = fmt.Sprint(e.pOrder)
	} else {
		sigPart = "return value"
		kindStr = e.rKind.String()
		orderStr = fmt.Sprint(e.rOrder)
	}

	return fmt.Sprintf(
		"fn %s of kind %s at index %d is out of order. Expected order for %ss is: %s",
		sigPart, kindStr, e.i, sigPart, orderStr)
}

// incorrectCountError is a helper struct for formatting errors that occur
// when validating counts of parameters or return values. The struct assumes
// that either pKind or rKind (but not both) are set to a non-illegal value.
// Filling out both or neither leads to undefined behavior.
type incorrectCountError struct {
	pKind    FnParamKind // The incorrect param's kind.
	rKind    ReturnKind  // The incorrect return value's kind.
	expected Count       // The incorrect value's expected count.
	actual   int         // The incorrect value's actual count.
}

func (e *incorrectCountError) Error() string {
	var sigPart, kindStr string
	if e.pKind != FnIllegal {
		sigPart = "parameter"
		kindStr = e.pKind.String()
	} else {
		sigPart = "return value"
		kindStr = e.rKind.String()
	}

	switch e.expected {
	case CountNone:
		return fmt.Sprintf("fn may not have any %ss of kind %s", sigPart, kindStr)
	case CountOpt:
		return fmt.Sprintf("fn may not have more than one %s of kind %s, has %v", sigPart, kindStr, e.actual)
	}
	return "invalid incorrectCountError"
}
