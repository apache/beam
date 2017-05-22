package graph

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"log"
	"reflect"
)

// Fn holds either a function or struct receiver.
type Fn struct {
	Fn      *userfn.UserFn
	Recv    interface{}
	Methods map[string]*userfn.UserFn
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
			log.Printf("M: %+v", m)

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
		return &Fn{Recv: fn, Methods: methods}, nil

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

	// TODO: CombineFn, ViewFn, etc.
)

// DoFn represents a DoFn.
type DoFn Fn

// Setup returns the "Setup" function, if present.
func (f *DoFn) Setup() *userfn.UserFn {
	return f.Methods[setupName]
}

// StartBundle returns the "StartBundle" function, if present.
func (f *DoFn) StartBundle() *userfn.UserFn {
	return f.Methods[startBundleName]
}

// ProcessElement returns the "ProcessElement" function.
func (f *DoFn) ProcessElement() *userfn.UserFn {
	return f.Methods[processElementName]
}

// FinishBundle returns the "FinishBundle" function, if present.
func (f *DoFn) FinishBundle() *userfn.UserFn {
	return f.Methods[finishBundleName]
}

// Teardown returns the "Teardown" function, if present.
func (f *DoFn) Teardown() *userfn.UserFn {
	return f.Methods[teardownName]
}

// Name returns the name of the function or struct.
func (f *DoFn) Name() string {
	if f.Fn != nil {
		return f.Fn.Name
	}
	return reflect.TypeOf(f.Recv).Name()
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
	if fn.Methods == nil {
		fn.Methods = make(map[string]*userfn.UserFn)
	}
	if fn.Fn != nil {
		fn.Methods[processElementName] = fn.Fn
	}
	if err := verifyValidNames(fn, setupName, startBundleName, processElementName, finishBundleName, teardownName); err != nil {
		return nil, err
	}

	if _, ok := fn.Methods[processElementName]; !ok {
		return nil, fmt.Errorf("failed to find ProcessElement method: %v", fn)
	}

	// TODO(herohde) 5/18/2017: validate the signatures, incl. consistency.

	return (*DoFn)(fn), nil
}

func verifyValidNames(fn *Fn, names ...string) error {
	m := make(map[string]bool)
	for _, name := range names {
		m[name] = true
	}

	for key, _ := range fn.Methods {
		if !m[key] {
			return fmt.Errorf("unexpected method %v present. Valid methods are: %v", key, names)
		}
	}
	return nil
}
