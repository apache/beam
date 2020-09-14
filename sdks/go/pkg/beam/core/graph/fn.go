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
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
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

	createInitialRestrictionName = "CreateInitialRestriction"
	splitRestrictionName         = "SplitRestriction"
	restrictionSizeName          = "RestrictionSize"
	createTrackerName            = "CreateTracker"

	createAccumulatorName = "CreateAccumulator"
	addInputName          = "AddInput"
	mergeAccumulatorsName = "MergeAccumulators"
	extractOutputName     = "ExtractOutput"
	compactName           = "Compact"

	// TODO: ViewFn, etc.
)

var doFnNames = []string{
	setupName,
	startBundleName,
	processElementName,
	finishBundleName,
	teardownName,
	createInitialRestrictionName,
	splitRestrictionName,
	restrictionSizeName,
	createTrackerName,
}

var sdfNames = []string{
	createInitialRestrictionName,
	splitRestrictionName,
	restrictionSizeName,
	createTrackerName,
}

var combineFnNames = []string{
	createAccumulatorName,
	addInputName,
	mergeAccumulatorsName,
	extractOutputName,
	compactName,
}

var lifecycleMethods map[string]struct{}

func init() {
	lifecycleMethods = make(map[string]struct{})
	methods := append(doFnNames, combineFnNames...)
	for _, name := range methods {
		lifecycleMethods[name] = struct{}{}
	}
}

// lifecycleMethodName returns if the passed in string is one of the lifecycle
// method names used by the Go SDK as DoFn or CombineFn lifecycle methods. These
// are the only methods that need shims generated for them.
func IsLifecycleMethod(n string) bool {
	_, ok := lifecycleMethods[n]
	return ok
}

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
	// Validation already passed, so if one SDF method is present they should
	// all be present.
	_, ok := f.methods[createInitialRestrictionName]
	return ok
}

// SplittableDoFn represents a DoFn implementing SDF methods.
type SplittableDoFn DoFn

// CreateInitialRestrictionFn returns the "CreateInitialRestriction" function, if present.
func (f *SplittableDoFn) CreateInitialRestrictionFn() *funcx.Fn {
	return f.methods[createInitialRestrictionName]
}

// SplitRestrictionFn returns the "SplitRestriction" function, if present.
func (f *SplittableDoFn) SplitRestrictionFn() *funcx.Fn {
	return f.methods[splitRestrictionName]
}

// RestrictionSizeFn returns the "RestrictionSize" function, if present.
func (f *SplittableDoFn) RestrictionSizeFn() *funcx.Fn {
	return f.methods[restrictionSizeName]
}

// CreateTrackerFn returns the "CreateTracker" function, if present.
func (f *SplittableDoFn) CreateTrackerFn() *funcx.Fn {
	return f.methods[createTrackerName]
}

// Name returns the name of the function or struct.
func (f *SplittableDoFn) Name() string {
	return (*Fn)(f).Name()
}

// RestrictionT returns the restriction type from the SDF.
func (f *SplittableDoFn) RestrictionT() reflect.Type {
	return f.CreateInitialRestrictionFn().Ret[0].T
}

// TODO(herohde) 5/19/2017: we can sometimes detect whether the main input must be
// a KV or not based on the other signatures (unless we're more loose about which
// sideinputs are present). Bind should respect that.

type mainInputs int

// The following constants prefixed with "Main" represent valid numbers of DoFn
// main inputs for DoFn construction and validation.
const (
	MainUnknown mainInputs = -1 // Number of inputs is unknown for DoFn validation.
	MainSingle  mainInputs = 1  // Number of inputs for single value elements.
	MainKv      mainInputs = 2  // Number of inputs for KV elements.
)

// config stores the optional configuration parameters to NewDoFn.
type config struct {
	numMainIn mainInputs
}

func defaultConfig() *config {
	return &config{
		numMainIn: MainUnknown,
	}
}

// NumMainInputs is an optional config to NewDoFn which specifies the number
// of main inputs to the DoFn being created, allowing for more complete
// validation. Valid inputs are the package constants of type mainInputs.
//
// Example usage:
//   graph.NewDoFn(fn, graph.NumMainInputs(graph.MainKv))
func NumMainInputs(num mainInputs) func(*config) {
	return func(cfg *config) {
		cfg.numMainIn = num
	}
}

// CoGBKMainInput is an optional config to NewDoFn which specifies the number
// of components of a CoGBK input to the DoFn being created, allowing for more complete
// validation.
//
// Example usage:
//   var col beam.PCollection
//   graph.NewDoFn(fn, graph.CoGBKMainInput(len(col.Type().Components())))
func CoGBKMainInput(components int) func(*config) {
	return func(cfg *config) {
		cfg.numMainIn = mainInputs(components)
	}
}

// NewDoFn constructs a DoFn from the given value, if possible.
func NewDoFn(fn interface{}, options ...func(*config)) (*DoFn, error) {
	ret, err := NewFn(fn)
	if err != nil {
		return nil, errors.WithContext(errors.Wrapf(err, "invalid DoFn"), "constructing DoFn")
	}
	cfg := defaultConfig()
	for _, opt := range options {
		opt(cfg)
	}
	return AsDoFn(ret, cfg.numMainIn)
}

// AsDoFn converts a Fn to a DoFn, if possible. numMainIn specifies how many
// main inputs are expected in the DoFn's method signatures. Valid inputs are
// the package constants of type mainInputs. If that number is MainUnknown then
// validation is done by best effort and may miss some edge cases.
func AsDoFn(fn *Fn, numMainIn mainInputs) (*DoFn, error) {
	addContext := func(err error, fn *Fn) error {
		return errors.WithContextf(err, "graph.AsDoFn: for Fn named %v", fn.Name())
	}

	if fn.methods == nil {
		fn.methods = make(map[string]*funcx.Fn)
	}
	if fn.Fn != nil {
		fn.methods[processElementName] = fn.Fn
	}
	if err := verifyValidNames("graph.AsDoFn", fn, doFnNames...); err != nil {
		return nil, err
	}

	if _, ok := fn.methods[processElementName]; !ok {
		err := errors.Errorf("failed to find %v method", processElementName)
		return nil, addContext(err, fn)
	}

	// Validate ProcessElement has correct number of main inputs (as indicated by
	// numMainIn), and that main inputs are before side inputs.
	processFn := fn.methods[processElementName]
	if err := validateMainInputs(fn, processFn, processElementName, numMainIn); err != nil {
		return nil, addContext(err, fn)
	}

	// If numMainIn is unknown, we can try inferring it from the number of inputs in ProcessElement.
	pos, num, _ := processFn.Inputs()
	if numMainIn == MainUnknown && num == 1 {
		numMainIn = MainSingle
	}

	// If the ProcessElement function includes side inputs or emit functions those must also be
	// present in the signatures of startBundle and finishBundle.
	processFnInputs := processFn.Param[pos : pos+num]
	if startFn, ok := fn.methods[startBundleName]; ok {
		if err := validateSideInputs(processFnInputs, startFn, startBundleName, numMainIn); err != nil {
			return nil, addContext(err, fn)
		}
	}
	if finishFn, ok := fn.methods[finishBundleName]; ok {
		if err := validateSideInputs(processFnInputs, finishFn, finishBundleName, numMainIn); err != nil {
			return nil, addContext(err, fn)
		}
	}

	pos, num, ok := processFn.Emits()
	var processFnEmits []funcx.FnParam
	if ok {
		processFnEmits = processFn.Param[pos : pos+num]
	} else {
		processFnEmits = processFn.Param[0:0]
	}
	if startFn, ok := fn.methods[startBundleName]; ok {
		if err := validateEmits(processFnEmits, startFn, startBundleName); err != nil {
			return nil, addContext(err, fn)
		}
	}
	if finishFn, ok := fn.methods[finishBundleName]; ok {
		if err := validateEmits(processFnEmits, finishFn, finishBundleName); err != nil {
			return nil, addContext(err, fn)
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

	// Check whether to perform SDF validation.
	isSdf, err := validateIsSdf(fn)
	if err != nil {
		return nil, addContext(err, fn)
	}

	// Perform validation on the SDF method signatures to ensure they're valid.
	if isSdf {
		err := validateSdfSignatures(fn, numMainIn)
		if err != nil {
			return nil, addContext(err, fn)
		}
	}

	return (*DoFn)(fn), nil
}

// validateMainInputs checks that a method has the given number of main inputs
// and that main inputs are before any side inputs.
func validateMainInputs(fn *Fn, method *funcx.Fn, methodName string, numMainIn mainInputs) error {
	if numMainIn == MainUnknown {
		numMainIn = MainSingle // If unknown, validate for minimum number of inputs.
	}

	// Make sure there are enough inputs (at least numMainIn)
	pos, num, ok := method.Inputs()
	if !ok {
		err := errors.Errorf("%v method has no main inputs", methodName)
		err = errors.SetTopLevelMsgf(err,
			"Method %v in DoFn %v is missing all inputs. A main input is required.",
			methodName, fn.Name())
		return err
	}
	if num < int(numMainIn) {
		err := errors.Errorf("%v method has too few main inputs", methodName)
		err = errors.SetTopLevelMsgf(err,
			"Method %v in DoFn %v does not have enough main inputs. "+
				"%v main inputs were expected, but only %v inputs were found.",
			methodName, fn.Name(), numMainIn, num)
		return err
	}

	// Check that the first input is not an Iter or ReIter (those aren't valid
	// as the first main input).
	first := method.Param[pos].Kind
	if first != funcx.FnValue {
		err := errors.New("first main input parameter must be a value type")
		err = errors.SetTopLevelMsgf(err,
			"Method %v of DoFns should always have the first input be a value type, "+
				"but it has an Iter or ReIter first in DoFn %v.",
			processElementName, fn.Name())
		return errors.WithContextf(err, "method %v", processElementName)
	}
	return nil
}

// validateEmits compares the emits found in a DoFn method signature with the emits found in
// the signature for ProcessElement, and performs validation that those match. This function
// should only be used to validate methods that are expected to have the same emit parameters as
// ProcessElement.
func validateEmits(processFnEmits []funcx.FnParam, method *funcx.Fn, methodName string) error {
	posMethodEmits, numMethodEmits, ok := method.Emits()
	numProcessEmits := len(processFnEmits)

	// Handle cases where method has no emits.
	if !ok {
		if numProcessEmits == 0 { // We're good, expected no emits.
			return nil
		}
		// Error, missing emits.
		err := errors.Errorf("emit parameters expected in method %v", methodName)
		return errors.SetTopLevelMsgf(err,
			"Missing emit parameters in the %v method of a DoFn. "+
				"If emit parameters are present in %v those parameters must also be present in %v.",
			methodName, processElementName, methodName)
	}

	// Error if number of emits doesn't match.
	if numMethodEmits != numProcessEmits {
		err := errors.Errorf("number of emits in method %v does not match method %v: got %d, expected %d",
			methodName, processElementName, numMethodEmits, numProcessEmits)
		return errors.SetTopLevelMsgf(err,
			"Incorrect number of emit parameters in the %v method of a DoFn. "+
				"The emit parameters should match those of the %v method.",
			methodName, processElementName)
	}

	// Error if there's a type mismatch.
	methodEmits := method.Param[posMethodEmits : posMethodEmits+numMethodEmits]
	for i := 0; i < numProcessEmits; i++ {
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

// validateSideInputs compares the inputs found in a DoFn method signature with the inputs found
// in the signature for ProcessElement, and performs validation to check that the side inputs
// match. This function should only be used to validate methods that are expected to have matching
// side inputs to ProcessElement.
func validateSideInputs(processFnInputs []funcx.FnParam, method *funcx.Fn, methodName string, numMainIn mainInputs) error {
	if numMainIn == MainUnknown {
		return validateSideInputsNumUnknown(processFnInputs, method, methodName)
	}

	numProcessIn := len(processFnInputs)
	numSideIn := numProcessIn - int(numMainIn)
	posMethodIn, numMethodIn, ok := method.Inputs()

	// Handle cases where method has no inputs.
	if !ok {
		if numSideIn == 0 { // We're good, expected no side inputs.
			return nil
		}
		// Error, missing side inputs.
		err := errors.Errorf("side inputs expected in method %v", methodName)
		return errors.SetTopLevelMsgf(err,
			"Missing side inputs in the %v method of a DoFn. "+
				"If side inputs are present in %v those side inputs must also be present in %v.",
			methodName, processElementName, methodName)
	}

	// Error if number of side inputs doesn't match.
	if numMethodIn != numSideIn {
		err := errors.Errorf("number of side inputs in method %v does not match method %v: got %d, expected %d",
			methodName, processElementName, numMethodIn, numSideIn)
		return errors.SetTopLevelMsgf(err,
			"Incorrect number of side inputs in the %v method of a DoFn. "+
				"The side inputs should match those of the %v method.",
			methodName, processElementName)
	}

	// Error if there's a type mismatch.
	methodInputs := method.Param[posMethodIn : posMethodIn+numMethodIn]
	sideInputs := processFnInputs[numMainIn:] // Skip main inputs in ProcessFn
	for i := 0; i < len(sideInputs); i++ {
		if sideInputs[i].T != methodInputs[i].T {
			var err error = &funcx.TypeMismatchError{Got: methodInputs[i].T, Want: sideInputs[i].T}
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

// validateSideInputsNumUnknown does similar validation as validateSideInputs, but for an unknown
// number of main inputs.
func validateSideInputsNumUnknown(processFnInputs []funcx.FnParam, method *funcx.Fn, methodName string) error {
	// Note: By the time this is called, we should have already know that ProcessElement has at
	// least two inputs, and the second input is ambiguous (could be either a main input or side
	// input). Since we don't know how to interpret the second input, these checks will be more
	// permissive than they would be otherwise.
	posMethodIn, numMethodIn, ok := method.Inputs()
	numProcessIn := len(processFnInputs)

	// Handle cases where method has no inputs.
	if !ok {
		// If there's no inputs, this is fine, as the ProcessElement method could be a
		// CoGBK, and not have side inputs.
		return nil
	}

	// Error if number of side inputs doesn't match any of the possible numbers of side inputs,
	// defined below.
	numSideInSingle := numProcessIn - int(MainSingle)
	numSideInKv := numProcessIn - int(MainKv)
	if numMethodIn != numSideInSingle && numMethodIn != numSideInKv {
		err := errors.Errorf("number of side inputs in method %v does not match method %v: got %d, expected either %d or %d",
			methodName, processElementName, numMethodIn, numSideInSingle, numSideInKv)
		return errors.SetTopLevelMsgf(err,
			"Incorrect number of side inputs in the %v method of a DoFn. "+
				"The side inputs should match those of the %v method.",
			methodName, processElementName)
	}

	// Error if there's a type mismatch.
	methodInputs := method.Param[posMethodIn : posMethodIn+numMethodIn]
	// If there's N inputs in the method, then we compare with the last N inputs to processElement.
	offset := numProcessIn - numMethodIn
	sideInputs := processFnInputs[offset:]
	for i := 0; i < numMethodIn; i++ {
		if sideInputs[i].T != methodInputs[i].T {
			var err error = &funcx.TypeMismatchError{Got: methodInputs[i].T, Want: sideInputs[i].T}
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

// validateIsSdf checks whether a Fn either is or is not an SDF, and returns
// true if it is, false if it isn't, or an error if it doesn't fulfill the
// requirements for either case.
//
// For a Fn to be an SDF it must:
//   * Implement all the SDF methods.
//   * Include an RTracker parameter in ProcessElement.
// For a Fn to not be an SDF, it must:
//   * Implement none of the SDF methods.
//   * Not include an RTracker parameter in ProcessElement.
func validateIsSdf(fn *Fn) (bool, error) {
	// Store missing method names so we can output them to the user if validation fails.
	var missing []string
	for _, name := range sdfNames {
		_, ok := fn.methods[name]
		if !ok {
			missing = append(missing, name)
		}
	}

	var isSdf bool
	switch len(missing) {
	case 0: // All SDF methods present.
		isSdf = true
	case len(sdfNames): // No SDF methods.
		isSdf = false
	default: // Anything else means an invalid # of SDF methods.
		err := errors.Errorf("not all SplittableDoFn methods are present. Missing methods: %v", missing)
		return false, err
	}

	processFn := fn.methods[processElementName]
	if pos, ok := processFn.RTracker(); ok != isSdf {
		if ok {
			err := errors.Errorf("method %v has sdf.RTracker as param %v, expected none",
				processElementName, pos)
			return false, errors.SetTopLevelMsgf(err, "Method %v has an sdf.RTracker parameter at index %v, "+
				"but is not part of a splittable DoFn. sdf.RTracker is invalid in %v in non-splittable DoFns.",
				processElementName, pos, processElementName)
		}
		pos, _, _ = processFn.Inputs()
		err := errors.Errorf("method %v missing sdf.RTracker, expected one at index %v",
			processElementName, pos)
		return false, errors.SetTopLevelMsgf(err, "Method %v is missing an sdf.RTracker "+
			"parameter despite being part of a splittable DoFn. %v in splittable DoFns requires an "+
			"sdf.RTracker parameter before main inputs (in this case, at index %v).",
			processElementName, processElementName, pos)
	}
	return isSdf, nil
}

// validateSdfSignatures validates that types in the SDF methods of a Fn are
// consistent with each other (for example, element and restriction types should
// match with each other). Returns an error if one is found, or nil if the
// types are all valid.
// TODO(BEAM-3301): Once SDF documentation is added to ParDo, add a comment
// here to refer to that for specific details about what needs to be consistent.
func validateSdfSignatures(fn *Fn, numMainIn mainInputs) error {
	num := int(numMainIn)

	// If number of main inputs is ambiguous, we check for consistency against
	// CreateInitialRestriction.
	if numMainIn == MainUnknown {
		initialRestFn := fn.methods[createInitialRestrictionName]
		paramNum := len(initialRestFn.Param)
		switch paramNum {
		case int(MainSingle), int(MainKv):
			num = paramNum
		default: // Can't infer because method has invalid # of main inputs.
			err := errors.Errorf("invalid number of params in method %v. got: %v, want: %v or %v",
				createInitialRestrictionName, paramNum, int(MainSingle), int(MainKv))
			return errors.SetTopLevelMsgf(err, "Invalid number of parameters in method %v. "+
				"Got: %v, Want: %v or %v. Check that the signature conforms to the expected signature for %v, "+
				"and that elements in SDF method parameters match elements in %v.",
				createInitialRestrictionName, paramNum, int(MainSingle), int(MainKv), createInitialRestrictionName, processElementName)
		}
	}

	if err := validateSdfSigNumbers(fn, num); err != nil {
		return err
	}
	if err := validateSdfSigTypes(fn, num); err != nil {
		return err
	}

	return nil
}

// validateSdfSigNumbers validates the number of parameters and return values
// in each SDF method in the given Fn, and returns an error if a method has an
// invalid/unexpected number.
func validateSdfSigNumbers(fn *Fn, num int) error {
	paramNums := map[string]int{
		createInitialRestrictionName: num,
		splitRestrictionName:         num + 1,
		restrictionSizeName:          num + 1,
		createTrackerName:            1,
	}
	returnNum := 1 // TODO(BEAM-3301): Enable optional error params in SDF methods.

	for _, name := range sdfNames {
		method := fn.methods[name]
		if len(method.Param) != paramNums[name] {
			err := errors.Errorf("unexpected number of params in method %v. got: %v, want: %v",
				name, len(method.Param), paramNums[name])
			return errors.SetTopLevelMsgf(err, "Unexpected number of parameters in method %v. "+
				"Got: %v, Want: %v. Check that the signature conforms to the expected signature for %v, "+
				"and that elements in SDF method parameters match elements in %v.",
				name, len(method.Param), paramNums[name], name, processElementName)
		}
		if len(method.Ret) != returnNum {
			err := errors.Errorf("unexpected number of returns in method %v. got: %v, want: %v",
				name, len(method.Ret), returnNum)
			return errors.SetTopLevelMsgf(err, "Unexpected number of return values in method %v. "+
				"Got: %v, Want: %v. Check that the signature conforms to the expected signature for %v.",
				name, len(method.Ret), returnNum, name)
		}
	}
	return nil
}

// validateSdfSigTypes validates the types of the parameters and return values
// in each SDF method in the given Fn, and returns an error if a method has an
// invalid/mismatched type. Assumes that the number of parameters and return
// values has already been validated.
func validateSdfSigTypes(fn *Fn, num int) error {
	restrictionT := fn.methods[createInitialRestrictionName].Ret[0].T
	rTrackerT := reflect.TypeOf((*sdf.RTracker)(nil)).Elem()

	for _, name := range sdfNames {
		method := fn.methods[name]
		switch name {
		case createInitialRestrictionName:
			if err := validateSdfElementT(fn, createInitialRestrictionName, method, num); err != nil {
				return err
			}
		case splitRestrictionName:
			if err := validateSdfElementT(fn, splitRestrictionName, method, num); err != nil {
				return err
			}
			if method.Param[num].T != restrictionT {
				err := errors.Errorf("mismatched restriction type in method %v, param %v. got: %v, want: %v",
					splitRestrictionName, num, method.Param[num].T, restrictionT)
				return errors.SetTopLevelMsgf(err, "Mismatched restriction type in method %v, "+
					"parameter at index %v. Got: %v, Want: %v (from method %v). "+
					"Ensure that all restrictions in an SDF are the same type.",
					splitRestrictionName, num, method.Param[num].T, restrictionT, createInitialRestrictionName)
			}
			if method.Ret[0].T.Kind() != reflect.Slice ||
				method.Ret[0].T.Elem() != restrictionT {
				err := errors.Errorf("invalid output type in method %v, return %v. got: %v, want: %v",
					splitRestrictionName, 0, method.Ret[0].T, reflect.SliceOf(restrictionT))
				return errors.SetTopLevelMsgf(err, "Invalid output type in method %v, "+
					"return value at index %v. Got: %v, Want: %v (from method %v). "+
					"Ensure that all restrictions in an SDF are the same type, and that %v returns a slice.",
					splitRestrictionName, 0, method.Ret[0].T, reflect.SliceOf(restrictionT), createInitialRestrictionName, splitRestrictionName)
			}
		case restrictionSizeName:
			if err := validateSdfElementT(fn, restrictionSizeName, method, num); err != nil {
				return err
			}
			if method.Param[num].T != restrictionT {
				err := errors.Errorf("mismatched restriction type in method %v, param %v. got: %v, want: %v",
					restrictionSizeName, num, method.Param[num].T, restrictionT)
				return errors.SetTopLevelMsgf(err, "Mismatched restriction type in method %v, "+
					"parameter at index %v. Got: %v, Want: %v (from method %v). "+
					"Ensure that all restrictions in an SDF are the same type.",
					restrictionSizeName, num, method.Param[num].T, restrictionT, createInitialRestrictionName)
			}
			if method.Ret[0].T != reflectx.Float64 {
				err := errors.Errorf("invalid output type in method %v, return %v. got: %v, want: %v",
					restrictionSizeName, 0, method.Ret[0].T, reflectx.Float64)
				return errors.SetTopLevelMsgf(err, "Invalid output type in method %v, "+
					"return value at index %v. Got: %v, Want: %v. Sizing information in SDF methods must be in float64.",
					restrictionSizeName, 0, method.Ret[0].T, reflectx.Float64)
			}
		case createTrackerName:
			if method.Param[0].T != restrictionT {
				err := errors.Errorf("mismatched restriction type in method %v, param %v. got: %v, want: %v",
					createTrackerName, 0, method.Param[0].T, restrictionT)
				return errors.SetTopLevelMsgf(err, "Mismatched restriction type in method %v, "+
					"parameter at index %v. Got: %v, Want: %v (from method %v). "+
					"Ensure that all restrictions in an SDF are the same type.",
					createTrackerName, 0, method.Param[0].T, restrictionT, createInitialRestrictionName)
			}
			if method.Ret[0].T.Implements(rTrackerT) == false {
				err := errors.Errorf("invalid output type in method %v, return %v: %v does not implement sdf.RTracker",
					createTrackerName, 0, method.Ret[0].T)
				return errors.SetTopLevelMsgf(err, "Invalid output type in method %v, "+
					"return value at index %v (type: %v). Output of method %v must implement sdf.RTracker.",
					createTrackerName, 0, method.Ret[0].T, createTrackerName)
			}
			processFn := fn.methods[processElementName]
			pos, _ := processFn.RTracker()
			if method.Ret[0].T != processFn.Param[pos].T {
				err := errors.Errorf("mismatched output type in method %v, return %v: got: %v, want: %v",
					createTrackerName, 0, method.Ret[0].T, processFn.Param[pos].T)
				return errors.SetTopLevelMsgf(err, "Mismatched output type in method %v, "+
					"return value at index %v. Got: %v, Want: %v (from method %v).",
					createTrackerName, 0, method.Ret[0].T, processFn.Param[pos].T, processElementName)
			}
		}
	}

	return nil
}

// validateSdfElementT validates that element types in an SDF method are
// consistent with the ProcessElement method. This method assumes that the
// first 'num' parameters to the SDF method are the elements.
func validateSdfElementT(fn *Fn, name string, method *funcx.Fn, num int) error {
	// ProcessElement is the most canonical source of the element type. We can
	// processFn is valid by this point and skip unnecessary validation.
	processFn := fn.methods[processElementName]
	pos, _, _ := processFn.Inputs()

	for i := 0; i < num; i++ {
		if method.Param[i].T != processFn.Param[pos+i].T {
			err := errors.Errorf("mismatched element type in method %v, param %v. got: %v, want: %v",
				name, i, method.Param[i].T, processFn.Param[pos+i].T)
			return errors.SetTopLevelMsgf(err, "Mismatched element type in method %v, "+
				"parameter at index %v. Got: %v, Want: %v (from method %v). "+
				"Ensure that element parameters in SDF methods have consistent types with element parameters in %v.",
				name, i, method.Param[i].T, processFn.Param[pos+i].T, processElementName, processElementName)
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
