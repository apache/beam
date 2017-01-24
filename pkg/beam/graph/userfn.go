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
// construction time)? [no - hard to recognize the difference without wrappers.]

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
		validate := true

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

		// TODO(herohde): function types for re-iterables?

		case reflect.Struct:
			if _, ok := reflectx.FindTaggedField(t, reflectx.DataTag, reflectx.TypeTag /* ... */); ok {
				kind = FnContext
				validate = false
			}
		}

		if validate && reflectx.ClassOf(t) == reflectx.Invalid {
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
		} else if reflectx.ClassOf(t) == reflectx.Invalid {
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

// Bind returns true if the from type can be matched to the (possibly exploded)
// to type and remainder. Is is assumed that all types involved are valid data
// types, modulo GBK results.
//
// There are 3 possibilities:
//   (1) T              -> [T'], X         (simple)
//   (2) KV(T,T2)       -> [T',T2'], X     (exploded)
//   (3) KV(T, chan T2) -> [T'], [T2']::X  (GBK result)
//
// The return value is (T'', X, true), T'; is the receiving bound type, if the
// binding is valid. T'' is synthetic if exploded or GBK result.
func Bind(from reflect.Type, to []reflect.Type, rest []reflect.Type) (reflect.Type, []reflect.Type, bool) {
	switch from.Kind() {
	// case reflect.Ptr:
	// 	return Bind(from.Elem(), to, rest)
	case reflect.Struct:
		switch len(to) {
		case 1:
			// Try to match a GBK result first.

			if k, v, ok := reflectx.UnfoldGBK(from); ok {
				match := reflectx.IsAssignable(k, to[0]) && len(rest) > 0 && reflectx.IsAssignable(v, rest[0])
				if match {
					recv, _ := reflectx.MakeGBK(to[0], rest[0])
					return recv, rest[1:], true
				}
				return nil, nil, false
			}

			// Then try to match an explicit KV. We line up the key and
			// value fields based on tags.

			if fromKey, fromValue, ok := reflectx.UnfoldKV(from); ok {
				if toKey, toValue, ok := reflectx.UnfoldKV(to[0]); ok {
					match := reflectx.IsAssignable(fromKey, toKey) && reflectx.IsAssignable(fromValue, toValue)
					if match {
						return to[0], rest, true
					}
					return nil, nil, false
				}
			}

			// Otherwise, we require possibly converted assignment.

			if reflectx.IsAssignable(from, to[0]) {
				return to[0], rest, true
			}
			return nil, nil, false
		case 2:
			// Try to match a KV to exploded key and value. We use
			// tags to identify the field role. For the target, the
			// order is [key, value].

			key, value, ok := reflectx.UnfoldKV(from)
			if !ok {
				return nil, nil, false
			}

			match := reflectx.IsAssignable(key, to[0]) && reflectx.IsAssignable(value, to[1])
			if match {
				recv, _ := reflectx.MakeKV(to[0], to[1])
				return recv, rest, true
			}
			return nil, nil, false
		default:
			return nil, nil, false
		}
	default:
		if len(to) == 1 && reflectx.IsAssignable(from, to[0]) {
			return to[0], rest, true
		}
		return nil, nil, false
	}
}

// TODO(herohde): use side input kind in BindSide to generalize.

// BindSide returns true iff the side input can be bound. The given types are
// assumed to be from a Chan context.
func BindSide(from reflect.Type, to reflect.Type) bool {
	return reflectx.IsAssignable(from, to)
}
