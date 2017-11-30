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
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// Signature is a concise representation of a group of function types. The
// function types in a group can differ in optional leading arguments and
// trailing returns only. For example, a signature can represent:
//
//     (context.Context?, int, string) -> (bool, error?)
//
// where the context arguments and error return are optional. The int and
// string parameters as well as the bool return are mandatory.
type Signature struct {
	// OptArgs is the optional arguments allowed in order, if any, before any
	// required arguments. Must be concrete types.
	OptArgs []reflect.Type
	// Args is the required arguments allowed in order, if any.
	Args []reflect.Type
	// Return is the required returns allowed in order, if any.
	Return []reflect.Type
	// OptReturn is the optional returns allowed in order, if any, after any
	// required returns. Must be concrete types.
	OptReturn []reflect.Type
}

func (sig *Signature) String() string {
	var args, ret []string
	for _, a := range sig.OptArgs {
		args = append(args, fmt.Sprintf("%v?", a))
	}
	for _, a := range sig.Args {
		args = append(args, fmt.Sprintf("%v", a))
	}

	for _, r := range sig.Return {
		ret = append(ret, fmt.Sprintf("%v", r))
	}
	for _, r := range sig.OptReturn {
		ret = append(ret, fmt.Sprintf("%v?", r))
	}

	return fmt.Sprintf("%v -> %v", printArgList(args), printArgList(ret))
}

func printArgList(list []string) string {
	if len(list) == 1 {
		return list[0]
	}
	return fmt.Sprintf("(%v)", strings.Join(list, ", "))
}

// MakePredicate creates a simple N-ary predicate: <args> -> bool.
func MakePredicate(args ...reflect.Type) *Signature {
	return &Signature{Args: args, Return: []reflect.Type{reflectx.Bool}}
}

// Replace substitutes the old top-level type for the new one. It is intended
// to specialize generic signatures to concrete ones.
func Replace(sig *Signature, old, new reflect.Type) *Signature {
	return &Signature{
		OptArgs:   replace(sig.OptArgs, old, new),
		Args:      replace(sig.Args, old, new),
		Return:    replace(sig.Return, old, new),
		OptReturn: replace(sig.OptReturn, old, new),
	}
}

func replace(list []reflect.Type, old, new reflect.Type) []reflect.Type {
	var ret []reflect.Type
	for _, elm := range list {
		if elm == old {
			elm = new
		}
		ret = append(ret, elm)
	}
	return ret
}

// Satisfy returns nil iff the fn can satisfy the signature, respecting
// generics. For example, for
//
//    foo : (context.Context, X) -> bool
//    bar : (int) -> bool
//
// both would satisfy a signature of (context.Context?, int) -> bool. Only
// "foo" would satisfy (context.Context, string) -> bool and only "bar" would
// satisfy (int) -> bool.
func Satisfy(fn interface{}, sig *Signature) error {
	value := reflect.ValueOf(fn)
	if value.Kind() != reflect.Func {
		return fmt.Errorf("not a function: %v", value)
	}

	var in, out []reflect.Type
	for i := 0; i < value.Type().NumIn(); i++ {
		in = append(in, value.Type().In(i))
	}
	for i := 0; i < value.Type().NumOut(); i++ {
		out = append(out, value.Type().Out(i))
	}
	if len(in) < len(sig.Args) || len(out) < len(sig.Return) {
		return fmt.Errorf("not enough required parameters: %v", value)
	}
	if len(in) > len(sig.Args)+len(sig.OptArgs) || len(out) > len(sig.Return)+len(sig.OptReturn) {
		return fmt.Errorf("too many parameters: %v", value)
	}

	// (1) Create generic binding. If inconsistent, reject fn. We do not allow
	// optional parameters to be _defining_ generic to avoid ambiguity here.

	m := make(map[string]reflect.Type)
	off := len(in) - len(sig.Args)
	if err := bind(in[off:], sig.Args, m); err != nil {
		return err
	}
	if err := bind(out[:len(sig.Return)], sig.Return, m); err != nil {
		return err
	}

	// (2) Check satisfiability under binding.

	if err := matchReq(in[off:], sig.Args); err != nil {
		return err
	}
	if err := matchReq(out[:len(sig.Return)], sig.Return); err != nil {
		return err
	}
	if err := matchOpt(in[:off], sig.OptArgs, m); err != nil {
		return err
	}
	return matchOpt(out[len(sig.Return):], sig.OptReturn, m)
}

func bind(list, models []reflect.Type, m map[string]reflect.Type) error {
	for i, t := range models {
		if !typex.IsUniversal(list[i]) {
			continue
		}

		name := list[i].Name()
		if current, ok := m[name]; ok && current != t {
			return fmt.Errorf("bind conflict for %v: %v != %v", name, current, t)
		}
		m[name] = t
	}
	return nil
}

func matchReq(list, models []reflect.Type) error {
	for i, t := range list {
		if typex.IsUniversal(t) {
			continue // ok: if this was bad, there would be a bind conflict
		}

		model := models[i]
		if model != t {
			return fmt.Errorf("type mismatch: %v, want %v", t, model)
		}
	}
	return nil
}

func matchOpt(list, models []reflect.Type, m map[string]reflect.Type) error {
	i := 0
	for _, t := range list {
		if typex.IsUniversal(t) {
			// Substitute optional types, if bound.
			subst, ok := m[t.Name()]
			if !ok {
				return fmt.Errorf("optional generic parameter not bound %v", t.Name())
			}
			t = subst
		}
		for i < len(models) && models[i] != t {
			i++
		}

		if i == len(models) {
			return fmt.Errorf("failed to match optional parameter %v", t)
		}
	}
	return nil
}

// MustSatisfy panics if the given fn does not satisfy the signature.
func MustSatisfy(fn interface{}, sig *Signature) {
	if err := Satisfy(fn, sig); err != nil {
		panic(fmt.Sprintf("fn does not satisfy signature %v: %v", sig, err))
	}
}
