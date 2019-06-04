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
		return nil, errors.Errorf("graph.AsDoFn: failed to find %v method: %v", processElementName, fn)
	}

	// TODO(herohde) 5/18/2017: validate the signatures, incl. consistency.

	return (*DoFn)(fn), nil
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
