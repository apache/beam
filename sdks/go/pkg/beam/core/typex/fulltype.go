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

// Package typex contains full type representation and utilities for type checking.
package typex

import (
	"fmt"
	"reflect"
	"strings"
)

// FullType represents the tree structure of data types processed by the graph.
// It allows representation of composite types, such as KV<int, string> or
// W<CoGBK<int, int>>, as well as "generic" such types, KV<int,T> or CoGBK<X,Y>,
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
		return fmt.Sprintf("%v<%v>", printShortComposite(t.t), strings.Join(args, ","))
	default:
		return fmt.Sprintf("<invalid: %v>", t.t)
	}
}

func printShortComposite(t reflect.Type) string {
	switch t {
	case WindowedValueType:
		return "W"
	case CoGBKType:
		return "CoGBK"
	case KVType:
		return "KV"
	default:
		return fmt.Sprintf("invalid(%v)", t)
	}
}

// TODO(herohde) 4/20/2017: internalize fulltypes?

// New constructs a new full type with the given elements. It panics
// if not valid.
func New(t reflect.Type, components ...FullType) FullType {
	checkTypesNotNil(components)

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
		case KVType:
			if len(components) != 2 {
				panic("Invalid number of components for KV")
			}
			if isAnyNonKVComposite(components) {
				panic("Invalid to nest composites inside KV")
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
			if isAnyNonKVComposite(components) {
				panic("Invalid to nest composites inside CoGBK")
			}
			return &tree{class, t, components}
		default:
			panic(fmt.Sprintf("Unexpected composite type: %v", t))
		}
	default:
		panic(fmt.Sprintf("Invalid underlying type: %v", t))
	}
}

// NOTE(herohde) 1/26/2018: we allow nested KV types and coders to support the
// CoGBK translation (using KV<K,KV<int,[]byte>> to encode keyed raw union values)
// and potentially other uses. We do not have a reasonable way to emit nested KV
// values, so user functions are still limited to non-nested KVs. Universally-typed
// KV-values might be simple to allow, for example.

func isAnyNonKVComposite(list []FullType) bool {
	for _, t := range list {
		if t.Class() == Composite && t.Type() != KVType {
			return true
		}
	}
	return false
}

// Convenience functions.

// IsW returns true iff the type is a WindowedValue.
func IsW(t FullType) bool {
	return t.Type() == WindowedValueType
}

// NewW constructs a new WindowedValue of the given type.
func NewW(t FullType) FullType {
	return New(WindowedValueType, t)
}

// SkipW skips a WindowedValue layer, if present. If no, returns the input.
func SkipW(t FullType) FullType {
	if t.Type() == WindowedValueType {
		return t.Components()[0]
	}
	return t
}

// IsKV returns true iff the type is a KV.
func IsKV(t FullType) bool {
	return t.Type() == KVType
}

// NewKV constructs a new KV of the given key and value types.
func NewKV(components ...FullType) FullType {
	return New(KVType, components...)
}

// IsCoGBK returns true iff the type is a CoGBK.
func IsCoGBK(t FullType) bool {
	return t.Type() == CoGBKType
}

// NewCoGBK constructs a new CoGBK of the given component types.
func NewCoGBK(components ...FullType) FullType {
	return New(CoGBKType, components...)
}

// IsStructurallyAssignable returns true iff a from value is structurally
// assignable to the to value of the given types. Types that are
// "structurally assignable" (SA) are assignable if type variables are
// disregarded. In other words, depending on the bindings of universal
// type variables, types may or may not be assignable. However, types that
// are not SA are not assignable under any bindings.
//
// For example:
//
//   SA:  KV<int,int>    := KV<int,int>
//   SA:  KV<int,X>      := KV<int,string>  // X bound to string by assignment
//   SA:  KV<int,string> := KV<int,X>       // Assignable only if X is already bound to string
//   SA:  KV<int,string> := KV<X,X>         // Not assignable under any binding
//
//   Not SA:  KV<int,string> := KV<string,X>
//   Not SA:  X              := KV<int,string>
//   Not SA:  GBK(X,Y)       := KV<int,string>
//
func IsStructurallyAssignable(from, to FullType) bool {
	switch from.Class() {
	case Concrete:
		if to.Class() == Concrete {
			return from.Type().AssignableTo(to.Type())
		}
		return to.Class() == Universal
	case Universal:
		// Universals are not structurally assignable to Composites, such as KV or GBK.
		return to.Class() == Universal || to.Class() == Concrete || to.Class() == Container
	case Container:
		if to.Class() == Container {
			return IsList(from.Type()) && IsList(to.Type()) && IsStructurallyAssignable(from.Components()[0], to.Components()[0])
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
			if !IsStructurallyAssignable(elm, to.Components()[i]) {
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

// IsEqualList returns true iff the lists of types are equal.
func IsEqualList(from, to []FullType) bool {
	if len(from) != len(to) {
		return false
	}
	for i, t := range from {
		if !IsEqual(t, to[i]) {
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

		if !IsStructurallyAssignable(model, t) {
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

func checkTypesNotNil(list []FullType) {
	for i, t := range list {
		if t == nil {
			panic(fmt.Sprintf("nil type at index: %v", i))
		}
	}
}
