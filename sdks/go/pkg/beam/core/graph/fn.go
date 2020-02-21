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

package graph

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// Fn holds either a function or struct receiver.
type Fn struct {
	// Fn holds the function, if present. If Fn is nil, Recv must be
	// non-nil.
	Fn *funcx.Fn
	// Recv hold the struct receiver, if present. If Recv is nil, Fn
	// must be non-nil.
	Recv interface{}
	// DynFn holds the function-generator, if dynamic. If not nil, Fn
	// holds the generated function.
	DynFn *DynFn

	// methods holds the public methods (or the function) by their beam
	// names.
	methods map[string]*funcx.Fn
}

// Name returns the name of the function or struct.
func (f *Fn) Name() string {
	if f.Fn != nil {
		return f.Fn.Fn.Name()
	}
	t := reflectx.SkipPtr(reflect.TypeOf(f.Recv))
	return fmt.Sprintf("%v.%v", t.PkgPath(), t.Name())
}

// DynFn is a generator for dynamically-created functions:
//
//    gen: (name string, t reflect.Type, []byte) -> func : T
//
// where the generated function, fn : T, is re-created at runtime. This concept
// allows serialization of dynamically-generated functions, which do not have a
// valid (unique) symbol such as one created via reflect.MakeFunc.
type DynFn struct {
	// Name is the name of the function. It does not have to be a valid symbol.
	Name string
	// T is the type of the generated function
	T reflect.Type
	// Data holds the data, if any, for the generator. Each function
	// generator typically needs some configuration data, which is
	// required by the DynFn to be encoded.
	Data []byte
	// Gen is the function generator. The function generator itself must be a
	// function with a unique symbol.
	Gen func(string, reflect.Type, []byte) reflectx.Func
}

// NewFn pre-processes a function, dynamic function or struct for graph
// construction.
func NewFn(fn interface{}) (*Fn, error) {
	if gen, ok := fn.(*DynFn); ok {
		f, err := funcx.New(gen.Gen(gen.Name, gen.T, gen.Data))
		if err != nil {
			return nil, err
		}
		return &Fn{Fn: f, DynFn: gen}, nil
	}

	val := reflect.ValueOf(fn)
	switch val.Type().Kind() {
	case reflect.Func:
		f, err := funcx.New(reflectx.MakeFunc(fn))
		if err != nil {
			return nil, err
		}
		return &Fn{Fn: f}, nil

	case reflect.Ptr:
		if val.Elem().Kind() != reflect.Struct {
			return nil, errors.Errorf("value %v must be ptr to struct", fn)
		}

		// Note that a ptr receiver is necessary if struct fields are updated in the
		// user code. Otherwise, updates are simply lost.
		fallthrough

	case reflect.Struct:
		methods := make(map[string]*funcx.Fn)
		if methodsFuncs, ok := reflectx.WrapMethods(fn); ok {
			for name, mfn := range methodsFuncs {
				f, err := funcx.New(mfn)
				if err != nil {
					return nil, errors.Wrapf(err, "method %v invalid", name)
				}
				methods[name] = f
			}
			return &Fn{Recv: fn, methods: methods}, nil
		}
		// TODO(lostluck): Consider moving this into the reflectx package.
		for i := 0; i < val.Type().NumMethod(); i++ {
			m := val.Type().Method(i)
			if m.PkgPath != "" {
				continue // skip: unexported
			}
			if m.Name == "String" {
				continue // skip: harmless
			}

			// CAVEAT(herohde) 5/22/2017: The type val.Type.Method.Type is not
			// the same as val.Method.Type: the former has the explicit receiver.
			// We'll use the receiver-less version.

			// TODO(herohde) 5/22/2017: Alternatively, it looks like we could
			// serialize each method, call them explicitly and avoid struct
			// registration.

			f, err := funcx.New(reflectx.MakeFunc(val.Method(i).Interface()))
			if err != nil {
				return nil, errors.Wrapf(err, "method %v invalid", m.Name)
			}
			methods[m.Name] = f
		}
		return &Fn{Recv: fn, methods: methods}, nil

	default:
		return nil, errors.Errorf("value %v must be function or (ptr to) struct", fn)
	}
}

// Signature method names.
const (
	setupName          = "Setup"
	startBundleName    = "StartBundle"
	processElementName = "ProcessElement"
	finishBundleName   = "FinishBundle"
	teardownName       = "Teardown"

	createAccumulatorName = "CreateAccumulator"
	addInputName          = "AddInput"
	mergeAccumulatorsName = "MergeAccumulators"
	extractOutputName     = "ExtractOutput"
	compactName           = "Compact"

	// TODO: ViewFn, etc.
)

// DoFn represents a DoFn.
type DoFn Fn

// SetupFn returns the "Setup" function, if present.
func (f *DoFn) SetupFn() *funcx.Fn {
	return f.methods[setupName]
}

// StartBundleFn returns the "StartBundle" function, if present.
func (f *DoFn) StartBundleFn() *funcx.Fn {
	return f.methods[startBundleName]
}

// ProcessElementFn returns the "ProcessElement" function.
func (f *DoFn) ProcessElementFn() *funcx.Fn {
	return f.methods[processElementName]
}

// FinishBundleFn returns the "FinishBundle" function, if present.
func (f *DoFn) FinishBundleFn() *funcx.Fn {
	return f.methods[finishBundleName]
}

// TeardownFn returns the "Teardown" function, if present.
func (f *DoFn) TeardownFn() *funcx.Fn {
	return f.methods[teardownName]
}

// Name returns the name of the function or struct.
func (f *DoFn) Name() string {
	return (*Fn)(f).Name()
}

// IsSplittable returns whether the DoFn is a valid Splittable DoFn.
func (f *DoFn) IsSplittable() bool {
	return false // TODO(BEAM-3301): Implement this when we add SDFs.
}

// RestrictionT returns the restriction type from the DoFn if it's splittable.
// Otherwise, returns nil.
func (f *DoFn) RestrictionT() *reflect.Type {
	return nil // TODO(BEAM-3301): Implement this when we add SDFs.
}

// TODO(herohde) 5/19/2017: we can sometimes detect whether the main input must be
// a KV or not based on the other signatures (unless we're more loose about which
// sideinputs are present). Bind should respect that.

// NewDoFn constructs a DoFn from the given value, if possible.
func NewDoFn(fn interface{}) (*DoFn, error) {
	ret, err := NewFn(fn)
	if err != nil {
		return nil, errors.WithContext(errors.Wrapf(err, "invalid DoFn"), "constructing DoFn")
	}
	return AsDoFn(ret)
}

// AsDoFn converts a Fn to a DoFn, if possible.
func AsDoFn(fn *Fn) (*DoFn, error) {
	addContext := func(err error, fn *Fn) error {
		return errors.WithContextf(err, "graph.AsDoFn: for Fn named %v", fn.Name())
	}

	if fn.methods == nil {
		fn.methods = make(map[string]*funcx.Fn)
	}
	if fn.Fn != nil {
		fn.methods[processElementName] = fn.Fn
	}
	if err := verifyValidNames("graph.AsDoFn", fn, setupName, startBundleName, processElementName, finishBundleName, teardownName); err != nil {
		return nil, err
	}

	if _, ok := fn.methods[processElementName]; !ok {
		err := errors.Errorf("failed to find %v method", processElementName)
		return nil, addContext(err, fn)
	}

	// Start validating DoFn. First, check that ProcessElement has a main input.
	processFn := fn.methods[processElementName]
	pos, num, ok := processFn.Inputs()
	if ok {
		first := processFn.Param[pos].Kind
		if first != funcx.FnValue {
			err := errors.New("side input parameters must follow main input parameter")
			err = errors.SetTopLevelMsgf(err,
				"Method %v of DoFns should always have a main input before side inputs, "+
					"but it has side inputs (as Iters or ReIters) first in DoFn %v.",
				processElementName, fn.Name())
			err = errors.WithContextf(err, "method %v", processElementName)
			return nil, addContext(err, fn)
		}
	}

	// If the ProcessElement function includes side inputs or emit functions those must also be
	// present in the signatures of startBundle and finishBundle.
	if ok && num > 1 {
		if startFn, ok := fn.methods[startBundleName]; ok {
			processFnInputs := processFn.Param[pos : pos+num]
			if err := validateMethodInputs(processFnInputs, startFn, startBundleName); err != nil {
				return nil, addContext(err, fn)
			}
		}
		if finishFn, ok := fn.methods[finishBundleName]; ok {
			processFnInputs := processFn.Param[pos : pos+num]
			if err := validateMethodInputs(processFnInputs, finishFn, finishBundleName); err != nil {
				return nil, addContext(err, fn)
			}
		}
	}

	pos, num, ok = processFn.Emits()
	if ok {
		if startFn, ok := fn.methods[startBundleName]; ok {
			processFnEmits := processFn.Param[pos : pos+num]
			if err := validateMethodEmits(processFnEmits, startFn, startBundleName); err != nil {
				return nil, addContext(err, fn)
			}
		}
		if finishFn, ok := fn.methods[finishBundleName]; ok {
			processFnEmits := processFn.Param[pos : pos+num]
			if err := validateMethodEmits(processFnEmits, finishFn, finishBundleName); err != nil {
				return nil, addContext(err, fn)
			}
		}
	}

	// Check that Setup and Teardown have no parameters other than Context.
	for _, name := range []string{setupName, teardownName} {
		if method, ok := fn.methods[name]; ok {
			params := method.Param
			if len(params) > 1 || (len(params) == 1 && params[0].Kind != funcx.FnContext) {
				err := errors.Errorf(
					"method %v has invalid parameters, "+
						"only allowed an optional context.Context", name)
				err = errors.SetTopLevelMsgf(err,
					"Method %v of DoFns should have no parameters other than "+
						"an optional context.Context, but invalid parameters are "+
						"present in DoFn %v.",
					name, fn.Name())
				return nil, addContext(err, fn)
			}
		}
	}

	// Check that none of the methods (except ProcessElement) have any return
	// values other than error.
	for _, name := range []string{setupName, startBundleName, finishBundleName, teardownName} {
		if method, ok := fn.methods[name]; ok {
			returns := method.Ret
			if len(returns) > 1 || (len(returns) == 1 && returns[0].Kind != funcx.RetError) {
				err := errors.Errorf(
					"method %v has invalid return values, "+
						"only allowed an optional error", name)
				err = errors.SetTopLevelMsgf(err,
					"Method %v of DoFns should have no return values other "+
						"than an optional error, but invalid return values are present "+
						"in DoFn %v.",
					name, fn.Name())
				return nil, addContext(err, fn)
			}
		}
	}

	return (*DoFn)(fn), nil
}

// validateMethodEmits compares the emits found in a DoFn method signature with the emits found in
// the signature for ProcessElement, and performs validation that those match. This function
// should only be used to validate methods that are expected to have the same emit parameters as
// ProcessElement.
func validateMethodEmits(processFnEmits []funcx.FnParam, method *funcx.Fn, methodName string) error {
	methodPos, methodNum, ok := method.Emits()
	if !ok {
		err := errors.Errorf("emit parameters expected in method %v", methodName)
		return errors.SetTopLevelMsgf(err,
			"Missing emit parameters in the %v method of a DoFn. "+
				"If emit parameters are present in %v those parameters must also be present in %v.",
			methodName, processElementName, methodName)
	}

	processFnNum := len(processFnEmits)
	if methodNum != processFnNum {
		err := errors.Errorf("number of emits in method %v does not match method %v: got %d, expected %d",
			methodName, processElementName, methodNum, processFnNum)
		return errors.SetTopLevelMsgf(err,
			"Incorrect number of emit parameters in the %v method of a DoFn. "+
				"The emit parameters should match those of the %v method.",
			methodName, processElementName)
	}

	methodEmits := method.Param[methodPos : methodPos+methodNum]
	for i := 0; i < processFnNum; i++ {
		if processFnEmits[i].T != methodEmits[i].T {
			var err error = &funcx.TypeMismatchError{Got: methodEmits[i].T, Want: processFnEmits[i].T}
			err = errors.Wrapf(err, "emit parameter in method %v does not match emit parameter in %v",
				methodName, processElementName)
			return errors.SetTopLevelMsgf(err,
				"Incorrect emit parameters in the %v method of a DoFn. "+
					"The emit parameters should match those of the %v method.",
				methodName, processElementName)
		}
	}

	return nil
}

// validateMethodInputs compares the inputs found in a DoFn method signature with the inputs found
// in the signature for ProcessElement, and performs validation to check that the side inputs
// match. This function should only be used to validate methods that are expected to have matching
// side inputs to ProcessElement.
func validateMethodInputs(processFnInputs []funcx.FnParam, method *funcx.Fn, methodName string) error {
	methodPos, methodNum, ok := method.Inputs()

	// Note: The second input to ProcessElements is not guaranteed to be a side input (it could be
	// the Value part of a KV main input). Since we can't know whether to interpret it as a main or
	// side input, some of these checks have to work around it in specific ways.
	if !ok {
		if len(processFnInputs) <= 2 {
			return nil // This case is fine, since both ProcessElement inputs may be main inputs.
		}
		err := errors.Errorf("side inputs expected in method %v", methodName)
		return errors.SetTopLevelMsgf(err,
			"Missing side inputs in the %v method of a DoFn. "+
				"If side inputs are present in %v those side inputs must also be present in %v.",
			methodName, processElementName, methodName)
	}

	processFnNum := len(processFnInputs)
	// The number of side inputs is the number of inputs minus 1 or 2 depending on whether the second
	// input is a main or side input, so that's what we expect in the method's parameters.
	// Ex. if ProcessElement has 7 inputs, method must have either 5 or 6 inputs.
	if (methodNum != processFnNum-1) && (methodNum != processFnNum-2) {
		err := errors.Errorf("number of side inputs in method %v does not match method %v: got %d, expected either %d or %d",
			methodName, processElementName, methodNum, processFnNum-1, processFnNum-2)
		return errors.SetTopLevelMsgf(err,
			"Incorrect number of side inputs in the %v method of a DoFn. "+
				"The side inputs should match those of the %v method.",
			methodName, processElementName)
	}

	methodInputs := method.Param[methodPos : methodPos+methodNum]
	offset := processFnNum - methodNum // We need an offset to skip the main inputs in ProcessFnInputs
	for i := 0; i < methodNum; i++ {
		if processFnInputs[i+offset].T != methodInputs[i].T {
			var err error = &funcx.TypeMismatchError{Got: methodInputs[i].T, Want: processFnInputs[i+offset].T}
			err = errors.Wrapf(err, "side input in method %v does not match side input in %v",
				methodName, processElementName)
			return errors.SetTopLevelMsgf(err,
				"Incorrect side inputs in the %v method of a DoFn. "+
					"The side inputs should match those of the %v method.",
				methodName, processElementName)
		}
	}

	return nil
}

// CombineFn represents a CombineFn.
type CombineFn Fn

// SetupFn returns the "Setup" function, if present.
func (f *CombineFn) SetupFn() *funcx.Fn {
	return f.methods[setupName]
}

// CreateAccumulatorFn returns the "CreateAccumulator" function, if present.
func (f *CombineFn) CreateAccumulatorFn() *funcx.Fn {
	return f.methods[createAccumulatorName]
}

// AddInputFn returns the "AddInput" function, if present.
func (f *CombineFn) AddInputFn() *funcx.Fn {
	return f.methods[addInputName]
}

// MergeAccumulatorsFn returns the "MergeAccumulators" function. If it is the only
// method present, then InputType == AccumulatorType == OutputType.
func (f *CombineFn) MergeAccumulatorsFn() *funcx.Fn {
	return f.methods[mergeAccumulatorsName]
}

// ExtractOutputFn returns the "ExtractOutput" function, if present.
func (f *CombineFn) ExtractOutputFn() *funcx.Fn {
	return f.methods[extractOutputName]
}

// CompactFn returns the "Compact" function, if present.
func (f *CombineFn) CompactFn() *funcx.Fn {
	return f.methods[compactName]
}

// TeardownFn returns the "Teardown" function, if present.
func (f *CombineFn) TeardownFn() *funcx.Fn {
	return f.methods[teardownName]
}

// Name returns the name of the function or struct.
func (f *CombineFn) Name() string {
	return (*Fn)(f).Name()
}

// NewCombineFn constructs a CombineFn from the given value, if possible.
func NewCombineFn(fn interface{}) (*CombineFn, error) {
	ret, err := NewFn(fn)
	if err != nil {
		return nil, errors.WithContext(errors.Wrapf(err, "invalid CombineFn"), "constructing CombineFn")
	}
	return AsCombineFn(ret)
}

// AsCombineFn converts a Fn to a CombineFn, if possible.
func AsCombineFn(fn *Fn) (*CombineFn, error) {
	const fnKind = "graph.AsCombineFn"
	if fn.methods == nil {
		fn.methods = make(map[string]*funcx.Fn)
	}
	if fn.Fn != nil {
		fn.methods[mergeAccumulatorsName] = fn.Fn
	}
	if err := verifyValidNames(fnKind, fn, setupName, createAccumulatorName, addInputName, mergeAccumulatorsName, extractOutputName, compactName, teardownName); err != nil {
		return nil, err
	}

	mergeFn, ok := fn.methods[mergeAccumulatorsName]
	if !ok {
		return nil, errors.Errorf("%v: failed to find required %v method on type: %v", fnKind, mergeAccumulatorsName, fn.Name())
	}

	// CombineFn methods must satisfy the following:
	// CreateAccumulator func() (A, error?)
	// AddInput func(A, I) (A, error?)
	// MergeAccumulators func(A, A) (A, error?)
	// ExtractOutput func(A) (O, error?)
	// This means that the other signatures *must* match the type used in MergeAccumulators.
	if len(mergeFn.Ret) <= 0 {
		return nil, errors.Errorf("%v: %v requires at least 1 return value. : %v", fnKind, mergeAccumulatorsName, mergeFn)
	}
	accumType := mergeFn.Ret[0].T

	for _, mthd := range []struct {
		name    string
		sigFunc func(fx *funcx.Fn, accumType reflect.Type) *funcx.Signature
	}{
		{mergeAccumulatorsName, func(fx *funcx.Fn, accumType reflect.Type) *funcx.Signature {
			return funcx.Replace(mergeAccumulatorsSig, typex.TType, accumType)
		}},
		{createAccumulatorName, func(fx *funcx.Fn, accumType reflect.Type) *funcx.Signature {
			return funcx.Replace(createAccumulatorSig, typex.TType, accumType)
		}},
		{addInputName, func(fx *funcx.Fn, accumType reflect.Type) *funcx.Signature {
			// AddInput needs the last parameter type substituted.
			p := fx.Param[len(fx.Param)-1]
			aiSig := funcx.Replace(addInputSig, typex.TType, accumType)
			return funcx.Replace(aiSig, typex.VType, p.T)
		}},
		{extractOutputName, func(fx *funcx.Fn, accumType reflect.Type) *funcx.Signature {
			// ExtractOutput needs the first Return type substituted.
			r := fx.Ret[0]
			eoSig := funcx.Replace(extractOutputSig, typex.TType, accumType)
			return funcx.Replace(eoSig, typex.WType, r.T)
		}},
	} {
		if err := validateSignature(fnKind, mthd.name, fn, accumType, mthd.sigFunc); err != nil {
			return nil, err
		}
	}

	return (*CombineFn)(fn), nil
}

func validateSignature(fnKind, methodName string, fn *Fn, accumType reflect.Type, sigFunc func(*funcx.Fn, reflect.Type) *funcx.Signature) error {
	if fx, ok := fn.methods[methodName]; ok {
		sig := sigFunc(fx, accumType)
		if err := funcx.Satisfy(fx, sig); err != nil {
			return &verifyMethodError{fnKind, methodName, err, fn, accumType, sig}
		}
	}
	return nil
}

func verifyValidNames(fnKind string, fn *Fn, names ...string) error {
	m := make(map[string]bool)
	for _, name := range names {
		m[name] = true
	}

	for key := range fn.methods {
		if !m[key] {
			return errors.Errorf("%s: unexpected exported method %v present. Valid methods are: %v", fnKind, key, names)
		}
	}
	return nil
}

type verifyMethodError struct {
	// Context for the error.
	fnKind, methodName string
	// The triggering error.
	err error

	fn        *Fn
	accumType reflect.Type
	sig       *funcx.Signature
}

func (e *verifyMethodError) Error() string {
	name := e.fn.methods[e.methodName].Fn.Name()
	if e.fn.Fn == nil {
		// Methods might be hidden behind reflect.methodValueCall, which is
		// not useful to the end user.
		name = fmt.Sprintf("%s.%s", e.fn.Name(), e.methodName)
	}
	typ := e.fn.methods[e.methodName].Fn.Type()
	switch e.methodName {
	case mergeAccumulatorsName:
		// Provide a clearer error for MergeAccumulators, since it's the root method
		// for CombineFns.
		// The root error doesn't matter here since we can't be certain what the accumulator
		// type is before mergeAccumulators is verified.
		return fmt.Sprintf("%v: %s must be a binary merge of accumulators to be a CombineFn. "+
			"It is of type \"%v\", but it must be of type func(context.Context?, A, A) (A, error?) "+
			"where A is the accumulator type",
			e.fnKind, name, typ)
	case createAccumulatorName, addInputName, extractOutputName:
		// Commonly the accumulator type won't match.
		if err, ok := e.err.(*funcx.TypeMismatchError); ok && err.Want == e.accumType {
			return fmt.Sprintf("%s invalid %v: %s has type \"%v\", but expected \"%v\" "+
				"to be the accumulator type \"%v\"; expected a signature like %v",
				e.fnKind, e.methodName, name, typ, err.Got, e.accumType, e.sig)
		}
	}
	return fmt.Sprintf("%s invalid %v %v: got type %v but "+
		"expected a signature like %v; original error: %v",
		e.fnKind, e.methodName, name, typ, e.sig, e.err)
}

var (
	mergeAccumulatorsSig = &funcx.Signature{
		OptArgs:   []reflect.Type{reflectx.Context},
		Args:      []reflect.Type{typex.TType, typex.TType},
		Return:    []reflect.Type{typex.TType},
		OptReturn: []reflect.Type{reflectx.Error},
	}
	createAccumulatorSig = &funcx.Signature{
		OptArgs:   []reflect.Type{reflectx.Context},
		Args:      []reflect.Type{},
		Return:    []reflect.Type{typex.TType},
		OptReturn: []reflect.Type{reflectx.Error},
	}
	addInputSig = &funcx.Signature{
		OptArgs:   []reflect.Type{reflectx.Context},
		Args:      []reflect.Type{typex.TType, typex.VType},
		Return:    []reflect.Type{typex.TType},
		OptReturn: []reflect.Type{reflectx.Error},
	}
	extractOutputSig = &funcx.Signature{
		OptArgs:   []reflect.Type{reflectx.Context},
		Args:      []reflect.Type{typex.TType},
		Return:    []reflect.Type{typex.WType},
		OptReturn: []reflect.Type{reflectx.Error},
	}
)
