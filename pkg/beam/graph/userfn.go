package graph

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"reflect"
	"runtime"
)

type FnParamKind int

const (
	FnValue FnParamKind = iota
	FnInChan
	FnOutChan
	FnContext
	FnOptions
)

type FnParam struct {
	Kind FnParamKind
	T    reflect.Type
}

type ReturnKind int

const (
	RetValue ReturnKind = iota
	RetError
)

type ReturnParam struct {
	Kind ReturnKind
	T    reflect.Type
}

// UserFn is the reflected user function, preprocessed. This wrapper is useful
// both at graph construction time as well as execution time.
type UserFn struct {
	Fn   reflect.Value
	Name string // robust name

	Param []FnParam
	Ret   []ReturnParam
}

func (u *UserFn) HasDirectInput() bool {
	for _, p := range u.Param {
		if p.Kind == FnValue {
			return true
		}
	}
	return false
}

func (u *UserFn) HasDirectOutput() bool {
	for _, p := range u.Ret {
		if p.Kind == RetValue {
			return true
		}
	}
	return false
}

func (u *UserFn) HasError() (int, bool) {
	for i, p := range u.Ret {
		if p.Kind == RetError {
			return i, true
		}
	}
	return -1, false
}

// TODO(herohde): perhaps separate "Context" (runtime) and "Options" (pipeline
// construction time)?

func (u *UserFn) Context() (reflect.Type, bool) {
	for _, p := range u.Param {
		if p.Kind == FnContext {
			return p.T, true
		}
	}
	return nil, false
}

// Input returns the main input and side inputs, if any. The main input is
// either the type components (if exploded form) or a single type. Side
// inputs must be presented as views during execution.
func (u *UserFn) Input() ([]reflect.Type, []reflect.Type) {
	var ret []reflect.Type
	var side []reflect.Type

	for _, p := range u.Param {
		if p.Kind == FnValue {
			ret = append(ret, p.T)
		}
	}
	for _, p := range u.Param {
		if p.Kind == FnInChan {
			if len(ret) == 0 {
				ret = append(ret, p.T)
			} else {
				side = append(side, p.T)
			}
		}
	}
	return ret, side
}

// Output returns the outputs, if any.
func (u *UserFn) Output() []reflect.Type {
	var ret []reflect.Type

	for _, p := range u.Ret {
		if p.Kind == RetValue {
			ret = append(ret, p.T)
		}
	}
	for _, p := range u.Param {
		if p.Kind == FnOutChan {
			ret = append(ret, p.T)
		}
	}
	return ret
}

func (u *UserFn) String() string {
	return fmt.Sprintf("%+v", *u)
}

// ReflectFn classifies a user function.
func ReflectFn(dofn interface{}) (*UserFn, error) {
	fn := reflect.ValueOf(dofn)
	if fn.Kind() != reflect.Func {
		return nil, fmt.Errorf("Not a function: %v", fn.Kind())
	}

	name := runtime.FuncForPC(fn.Pointer()).Name() // + "/" + fn.String()
	fntype := fn.Type()

	var param []FnParam
	for i := 0; i < fntype.NumIn(); i++ {
		t := fntype.In(i)
		kind := FnValue

		switch t.Kind() {
		case reflect.Chan:
			switch t.ChanDir() {
			case reflect.RecvDir:
				kind = FnInChan
				t = t.Elem()

			case reflect.SendDir:
				kind = FnOutChan
				t = t.Elem()

			default:
				return nil, fmt.Errorf("Channels cannot be bidirectional: %v", t)
			}

			// case reflect.Ptr, reflect.Struct: TBD to detect Contexts
		}
		if !IsValidDataType(t) {
			return nil, fmt.Errorf("Parameter %v for %v has unsupported type: %v", i, name, t)
		}

		param = append(param, FnParam{Kind: kind, T: t})
	}

	var ret []ReturnParam
	for i := 0; i < fntype.NumOut(); i++ {
		t := fntype.Out(i)
		kind := RetValue

		if reflectx.Error == t {
			kind = RetError
		} else if !IsValidDataType(t) {
			return nil, fmt.Errorf("Return value %v for %v has unsupported type: %v", i, name, t)
		}

		ret = append(ret, ReturnParam{Kind: kind, T: t})
	}

	// TODO(herohde): validate parameter order?

	// Validate that we have at most one RetValue and RetError. The former
	// restriction avoids synthetic output collection types from returning
	// (string, int, error), say.

	seen := make(map[ReturnKind]bool)
	for _, p := range ret {
		if _, ok := seen[p.Kind]; ok {
			return nil, fmt.Errorf("Too many return values: %v", ret)
		}
		seen[p.Kind] = true
	}

	return &UserFn{Fn: fn, Name: name, Param: param, Ret: ret}, nil
}

// IsValidDataType returns true iff the type can be used as data in the
// pipeline. Such data must be fully serializable and . Functions and channels
// are examples of invalid types.
func IsValidDataType(t reflect.Type) bool {
	return isValidDataType(t, true)
}

func isValidDataType(t reflect.Type, ptr bool) bool {
	switch t.Kind() {
	case reflect.Invalid, reflect.UnsafePointer, reflect.Uintptr, reflect.Interface:
		return false // no unmanageable types

	case reflect.Chan, reflect.Func:
		return false // no unserializable types

	case reflect.Map, reflect.Array:
		return false // TBD

	case reflect.Complex64, reflect.Complex128:
		return false // TBD

	case reflect.Slice:
		return IsValidDataType(t.Elem())

	case reflect.Ptr:
		if !ptr {
			return false // no nested pointers
		}
		return isValidDataType(t.Elem(), false)

	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !IsValidDataType(t.Field(i).Type) {
				return false
			}
		}
		return true

	case reflect.Bool:
		return true

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true

	case reflect.Float32, reflect.Float64:
		return true

	case reflect.String:
		return true

	default:
		panic(fmt.Sprintf("Unexpected type: %v", t))
	}
}

// Bind returns true if the from type can be matched to the (possibly exploded)
// to type and remainder. Is is assumed that all types involved are valid data
// types, modulo GBK results.
//
// There are 3 possibilities:
//   (1) T              -> [T'], X         (simple)
//   (2) KV(T,T2)       -> [T',T2'], X     (exploded)
//   (3) KV(T, chan T2) -> [T'], [T2']::X  (GBK result)
//
// The return value is (X, true) if the binding is valid.
func Bind(from reflect.Type, to []reflect.Type, rest []reflect.Type) ([]reflect.Type, bool) {
	switch from.Kind() {
	case reflect.Ptr:
		return Bind(from.Elem(), to, rest)
	case reflect.Struct:
		switch len(to) {
		case 1:
			// Try to match a GBK result first.

			if k, v, ok := IsGBKResult(from); ok {
				match := k.AssignableTo(to[0]) && len(rest) > 0 && v.AssignableTo(reflectx.SkipPtr(rest[0]))
				if match {
					return rest[1:], true
				} else {
					return rest, false
				}
			}

			// Then try to match an explicit KV. We line up the key and
			// value fields based on tags.

			if fromKey, fromValue, ok := IsKV(from); ok {
				if toKey, toValue, ok := IsKV(to[0]); ok {
					return rest, reflectx.SkipPtr(fromKey).AssignableTo(reflectx.SkipPtr(toKey)) && reflectx.SkipPtr(fromValue).AssignableTo(reflectx.SkipPtr(toValue))
				}
			}

			return rest, from.AssignableTo(reflectx.SkipPtr(to[0]))
		case 2:
			// Try to match a KV to exploded key and value. We use
			// tags to identify the field role. For the target, the
			// order is [key, value].

			key, value, ok := IsKV(from)
			if !ok {
				return rest, false
			}
			return rest, key.AssignableTo(reflectx.SkipPtr(to[0])) && value.AssignableTo(reflectx.SkipPtr(to[1]))
		default:
			return rest, false
		}
	default:
		return rest, len(to) == 1 && from.AssignableTo(reflectx.SkipPtr(to[0]))
	}
}

// BindSide returns true iff the side input can be bound.
func BindSide(from reflect.Type, to reflect.Type) bool {
	return reflectx.SkipPtr(from).AssignableTo(reflectx.SkipPtr(to))
}

// IsKV returns (T', T'', true) if the type is of the form:
//
//    type T struct {
//          K T'  `beam:"key"`
//          V T'' `beam:"value"`
//    }
func IsKV(t reflect.Type) (reflect.Type, reflect.Type, bool) {
	if t.Kind() != reflect.Struct {
		return nil, nil, false
	}
	if t.NumField() != 2 {
		return nil, nil, false
	}

	key, ok := FindTaggedField(t, KeyTag)
	if !ok {
		return nil, nil, false
	}
	value, ok := FindTaggedField(t, ValueTag)
	if !ok {
		return nil, nil, false
	}
	return key.Type, value.Type, true
}

// IsGBKResult returns (T', T'', true) if the type is of the form:
//
//    type T struct {
//          K T'       `beam:"key"`
//          V chan T'' `beam:"values"`
//    }
func IsGBKResult(t reflect.Type) (reflect.Type, reflect.Type, bool) {
	if t.Kind() != reflect.Struct {
		return nil, nil, false
	}
	if t.NumField() != 2 {
		return nil, nil, false
	}

	key, ok := FindTaggedField(t, KeyTag)
	if !ok {
		return nil, nil, false
	}
	values, ok := FindTaggedField(t, ValuesTag)
	if !ok {
		return nil, nil, false
	}

	if values.Type.Kind() != reflect.Chan {
		return nil, nil, false
	}
	return key.Type, values.Type.Elem(), true
}
