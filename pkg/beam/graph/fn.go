package graph

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
)

// Fn holds either a function or struct receiver.
type Fn struct {
	// Fn holds the function, if present. If Fn is nil, Recv must be
	// non-nil.
	Fn *userfn.UserFn
	// Recv hold the struct receiver, if present. If Recv is nil, Fn
	// must be non-nil.
	Recv interface{}

	// methods holds the public methods (or the function) by their beam
	// names.
	methods map[string]*userfn.UserFn
}

// Name returns the name of the function or struct.
func (f *Fn) Name() string {
	if f.Fn != nil {
		return f.Fn.Name
	}
	t := reflectx.SkipPtr(reflect.TypeOf(f.Recv))
	return fmt.Sprintf("%v.%v", t.PkgPath(), t.Name())
}

// NewFn pre-processes a function or struct for graph construction.
func NewFn(fn interface{}) (*Fn, error) {
	val := reflect.ValueOf(fn)
	switch val.Type().Kind() {
	case reflect.Func:
		f, err := userfn.New(fn)
		if err != nil {
			return nil, err
		}
		return &Fn{Fn: f}, nil

	case reflect.Ptr:
		if val.Elem().Kind() != reflect.Struct {
			return nil, fmt.Errorf("value %v must be ptr to struct", fn)
		}

		// Note that a ptr receiver is necessary if struct fields are updated in the
		// user code. Otherwise, updates are simply lost.
		fallthrough

	case reflect.Struct:
		methods := make(map[string]*userfn.UserFn)
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

			f, err := userfn.New(val.Method(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("method %v invalid: %v", m.Name, err)
			}
			methods[m.Name] = f
		}
		return &Fn{Recv: fn, methods: methods}, nil

	default:
		return nil, fmt.Errorf("value %v must be function or (ptr to) struct", fn)
	}
}

// Signature method names.
const (
	setupName          = "Setup"
	startBundleName    = "StartBundle"
	processElementName = "ProcessElement"
	finishBundleName   = "FinishBundle"
	teardownName       = "Teardown"

	createAccumulator = "CreateAccumulator"
	addInput          = "AddInput"
	mergeAccumulators = "MergeAccumulators"
	extractOutput     = "ExtractOutput"
	compact           = "Compact"

	// TODO: ViewFn, etc.
)

// DoFn represents a DoFn.
type DoFn Fn

// SetupFn returns the "Setup" function, if present.
func (f *DoFn) SetupFn() *userfn.UserFn {
	return f.methods[setupName]
}

// StartBundleFn returns the "StartBundle" function, if present.
func (f *DoFn) StartBundleFn() *userfn.UserFn {
	return f.methods[startBundleName]
}

// ProcessElementFn returns the "ProcessElement" function.
func (f *DoFn) ProcessElementFn() *userfn.UserFn {
	return f.methods[processElementName]
}

// FinishBundleFn returns the "FinishBundle" function, if present.
func (f *DoFn) FinishBundleFn() *userfn.UserFn {
	return f.methods[finishBundleName]
}

// TeardownFn returns the "Teardown" function, if present.
func (f *DoFn) TeardownFn() *userfn.UserFn {
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
		return nil, err
	}
	return AsDoFn(ret)
}

// AsDoFn converts a Fn to a DoFn, if possible.
func AsDoFn(fn *Fn) (*DoFn, error) {
	if fn.methods == nil {
		fn.methods = make(map[string]*userfn.UserFn)
	}
	if fn.Fn != nil {
		fn.methods[processElementName] = fn.Fn
	}
	if err := verifyValidNames(fn, setupName, startBundleName, processElementName, finishBundleName, teardownName); err != nil {
		return nil, err
	}

	if _, ok := fn.methods[processElementName]; !ok {
		return nil, fmt.Errorf("failed to find ProcessElement method: %v", fn)
	}

	// TODO(herohde) 5/18/2017: validate the signatures, incl. consistency.

	return (*DoFn)(fn), nil
}

// CombineFn represents a CombineFn.
type CombineFn Fn

// CreateAccumulatorFn returns the "CreateAccumulator" function, if present.
func (f *CombineFn) CreateAccumulatorFn() *userfn.UserFn {
	return f.methods[createAccumulator]
}

// AddInputFn returns the "AddInput" function, if present.
func (f *CombineFn) AddInputFn() *userfn.UserFn {
	return f.methods[addInput]
}

// MergeAccumulatorsFn returns the "MergeAccumulators" function. If it is the only
// method present, then InputType == AccumulatorType == OutputType.
func (f *CombineFn) MergeAccumulatorsFn() *userfn.UserFn {
	return f.methods[mergeAccumulators]
}

// ExtractOutputFn returns the "ExtractOutput" function, if present.
func (f *CombineFn) ExtractOutputFn() *userfn.UserFn {
	return f.methods[extractOutput]
}

// CompactFn returns the "Compact" function, if present.
func (f *CombineFn) CompactFn() *userfn.UserFn {
	return f.methods[compact]
}

// Name returns the name of the function or struct.
func (f *CombineFn) Name() string {
	return (*Fn)(f).Name()
}

// NewCombineFn constructs a CombineFn from the given value, if possible.
func NewCombineFn(fn interface{}) (*CombineFn, error) {
	ret, err := NewFn(fn)
	if err != nil {
		return nil, err
	}
	return AsCombineFn(ret)
}

// AsCombineFn converts a Fn to a CombineFn, if possible.
func AsCombineFn(fn *Fn) (*CombineFn, error) {
	if fn.methods == nil {
		fn.methods = make(map[string]*userfn.UserFn)
	}
	if fn.Fn != nil {
		fn.methods[mergeAccumulators] = fn.Fn
	}
	if err := verifyValidNames(fn, createAccumulator, addInput, mergeAccumulators, extractOutput, compact); err != nil {
		return nil, err
	}

	if _, ok := fn.methods[mergeAccumulators]; !ok {
		return nil, fmt.Errorf("failed to find MergeAccumulators method: %v", fn)
	}

	// TODO(herohde) 5/24/2017: validate the signatures, incl. consistency.

	return (*CombineFn)(fn), nil
}

func verifyValidNames(fn *Fn, names ...string) error {
	m := make(map[string]bool)
	for _, name := range names {
		m[name] = true
	}

	for key, _ := range fn.methods {
		if !m[key] {
			return fmt.Errorf("unexpected method %v present. Valid methods are: %v", key, names)
		}
	}
	return nil
}
