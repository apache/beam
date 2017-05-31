package typex

import (
	"fmt"
	"reflect"
	"strings"
)

// FullType represents the tree structure of data types processed by the graph.
// It allows representation of composite types, such as KV<int, string> or
// W<GBK<int, int>>, as well as "generic" such types, KV<int,T> or GBK<X,Y>,
// where the free "type variables" are the fixed universal types: T, X, etc.
type FullType interface {
	// Class returns the class of the FullType. It is never Illegal.
	Class() Class
	// Type returns the Go type at the root of the tree, such as KV, X
	// or int.
	Type() reflect.Type
	// Components returns the real components of the root type, if Composite.
	Components() []FullType
}

// TODO(herohde) 5/16/2017: maybe rename FullType to DataType to match
// DataClass, if we go that way?

type tree struct {
	class      Class
	t          reflect.Type
	components []FullType
}

func (t *tree) Class() Class {
	return t.class
}

func (t *tree) Type() reflect.Type {
	return t.t
}

func (t *tree) Components() []FullType {
	return t.components
}

func (t *tree) String() string {
	switch t.class {
	case Concrete:
		return t.t.String()
	case Universal:
		return t.t.Name()
	case Container:
		if IsList(t.t) {
			return fmt.Sprintf("[]%v", t.components[0])
		}
		return fmt.Sprintf("<invalid: %v>", t.t)
	case Composite:
		var args []string
		for _, c := range t.components {
			args = append(args, fmt.Sprintf("%v", c))
		}
		return fmt.Sprintf("%v<%v>", t.t.Name(), strings.Join(args, ","))
	default:
		return fmt.Sprintf("<invalid: %v>", t.t)
	}
}

// TODO(herohde) 4/20/2017: internalize types?
// TODO(herohde) 4/24/2017: fully validate types.

// New constructs a new full type with the given elements. It panics
// if not valid.
func New(t reflect.Type, components ...FullType) FullType {
	class := ClassOf(t)
	switch class {
	case Concrete, Universal:
		return &tree{class, t, nil}
	case Container:
		switch t.Kind() {
		case reflect.Slice:
			// We include the child type as a component for convenience.
			return &tree{class, t, []FullType{New(t.Elem())}}
		default:
			panic(fmt.Sprintf("Unexpected aggregate type: %v", t))
		}
	case Composite:
		switch t {
		case KVType, GBKType:
			if len(components) != 2 {
				panic("Invalid number of components for KV/GBK")
			}
			return &tree{class, t, components}
		case WindowedValueType:
			if len(components) != 1 {
				panic("Invalid number of components for WindowedValue")
			}
			if components[0].Type() == WindowedValueType {
				panic("Invalid to nest WindowedValue")
			}
			return &tree{class, t, components}
		case CoGBKType:
			if len(components) < 2 {
				panic("Invalid number of components for CoGBK")
			}
			return &tree{class, t, components}
		default:
			panic(fmt.Sprintf("Unexpected composite type: %v", t))
		}
	default:
		panic(fmt.Sprintf("Invalid underlying type: %v", t))
	}
}

// Convenience methods to operate through the top-level WindowedValue.

func IsW(t FullType) bool {
	return t.Type() == WindowedValueType
}

func NewW(t FullType) FullType {
	return New(WindowedValueType, t)
}

func IsWKV(t FullType) bool {
	return IsW(t) && SkipW(t).Type() == KVType
}

func NewWKV(components ...FullType) FullType {
	return NewW(New(KVType, components...))
}

func IsWGBK(t FullType) bool {
	return IsW(t) && SkipW(t).Type() == GBKType
}

func NewWGBK(components ...FullType) FullType {
	return NewW(New(GBKType, components...))
}

func IsWCoGBK(t FullType) bool {
	return IsW(t) && SkipW(t).Type() == CoGBKType
}

func NewWCoGBK(components ...FullType) FullType {
	return NewW(New(CoGBKType, components...))
}

func SkipW(t FullType) FullType {
	if t.Type() == WindowedValueType {
		return t.Components()[0]
	}
	return t
}

// TODO(herohde) 4/20/2017: Disallow bad universal substitutions that produce
// inconsistent bindings: KV<T', U'> bind to KV<T, T>, where the substitution
// would be an invalid mapping {T -> T', T -> U'}. However, KV<X',Y'> -> KV<Y,X>
// or KV<T',T'> -> KV<T,U> are ok.

// IsAssignable returns true iff a from value can be assigned to the to value of
// the given Types.
func IsAssignable(from, to FullType) bool {
	switch from.Class() {
	case Concrete:
		if to.Class() == Concrete {
			return from.Type().AssignableTo(to.Type())
		}
		return to.Class() == Universal
	case Universal:
		return to.Class() == Universal || to.Class() == Concrete || to.Class() == Container
	case Container:
		if to.Class() == Container {
			return IsList(from.Type()) && IsList(to.Type()) && IsAssignable(from.Components()[0], to.Components()[1])
		}
		return to.Class() == Universal
	case Composite:
		if from.Type() != to.Type() {
			return false
		}
		if len(from.Components()) != len(to.Components()) {
			return false
		}
		for i, elm := range from.Components() {
			if !IsAssignable(elm, to.Components()[i]) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// IsEqual returns true iff the types are equal.
func IsEqual(from, to FullType) bool {
	if from.Type() != to.Type() {
		return false
	}
	if len(from.Components()) != len(to.Components()) {
		return false
	}
	for i, elm := range from.Components() {
		if !IsEqual(elm, to.Components()[i]) {
			return false
		}
	}
	return true
}

// IsBound returns true iff the type has no universal type components.
// Nodes and coders need bound types.
func IsBound(t FullType) bool {
	if t.Class() == Universal {
		return false
	}
	for _, elm := range t.Components() {
		if !IsBound(elm) {
			return false
		}
	}
	return true
}

// Bind returns a substitution from universals to types in the given models,
// such as {"T" -> X, "X" -> int}. Each model must be assignable to the
// corresponding type. For example, Bind(KV<T,int>, KV<string, int>) would
// produce {"T" -> string}.
func Bind(types, models []FullType) (map[string]reflect.Type, error) {
	if len(types) != len(models) {
		return nil, fmt.Errorf("invalid number of modes: %v, want %v", len(models), len(types))
	}

	m := make(map[string]reflect.Type)
	for i := 0; i < len(types); i++ {
		t := types[i]
		model := models[i]

		if !IsAssignable(model, t) {
			return nil, fmt.Errorf("%v is not assignable to %v", model, t)
		}
		if err := walk(t, model, m); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func walk(t, model FullType, m map[string]reflect.Type) error {
	switch t.Class() {
	case Universal:
		// By checking that the model is assignable to t, we know that they are
		// structurally compatible. We rely on the exact reflect.Type in the
		// Aggregate case to pick the correct binding, i.e., we do not need to
		// construct such a type.

		name := t.Type().Name()
		if current, ok := m[name]; ok && current != model.Type() {
			return fmt.Errorf("bind conflict for %v: %v != %v", name, current, model.Type())
		}
		m[name] = model.Type()
		return nil
	case Composite, Container:
		for i, elm := range t.Components() {
			if err := walk(elm, model.Components()[i], m); err != nil {
				return err
			}
		}
		return nil
	default:
		return nil
	}
}

// Substitute returns types identical to the given types, but with all
// universals substituted. All free type variables must be present in the
// substitution.
func Substitute(list []FullType, m map[string]reflect.Type) ([]FullType, error) {
	var ret []FullType
	for _, t := range list {
		repl, err := substitute(t, m)
		if err != nil {
			return nil, err
		}
		ret = append(ret, repl)
	}
	return ret, nil
}

func substitute(t FullType, m map[string]reflect.Type) (FullType, error) {
	switch t.Class() {
	case Universal:
		name := t.Type().Name()
		repl, ok := m[name]
		if !ok {
			return nil, fmt.Errorf("type variable not bound: %v", name)
		}
		return New(repl), nil
	case Container:
		comp, err := substituteList(t.Components(), m)
		if err != nil {
			return nil, err
		}
		if IsList(t.Type()) {
			return New(reflect.SliceOf(comp[0].Type()), comp...), nil
		}
		panic(fmt.Sprintf("Unexpected aggregate: %v", t))
	case Composite:
		comp, err := substituteList(t.Components(), m)
		if err != nil {
			return nil, err
		}
		return New(t.Type(), comp...), nil

	default:
		return t, nil
	}
}

func substituteList(list []FullType, m map[string]reflect.Type) ([]FullType, error) {
	var ret []FullType
	for _, elm := range list {
		repl, err := substitute(elm, m)
		if err != nil {
			return nil, err
		}
		ret = append(ret, repl)
	}
	return ret, nil
}
