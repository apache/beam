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

// Package genx is a convenience package to better support the code
// generator. It can be depended on by the user facing beam package
// and be refered to by generated code.
//
// Similarly, it can depend on beam internals and access the canonical
// method list in the graph package, or other packages to filter out
// types that aren't necessary for registration (like context.Context).
package genx

import (
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
)

// RegisterDoFn is a convenience function for registering DoFns.
// Differs from RegisterFunction and RegisterType by introspecting
// all parameters and returns of Lifecycle methods on the dofn,
// and registers those types for you.
//
// Panics if not passed a dofn.
func RegisterDoFn(dofn interface{}) {
	f, ts, err := registerDoFn(dofn)
	if err != nil {
		panic(err)
	}
	if f != nil {
		runtime.RegisterFunction(f)
	}
	for _, t := range ts {
		runtime.RegisterType(t)
	}
}

// registerDoFn returns all types associated with the provided DoFn.
// If passed a functional DoFn, the first return is a Function to
// register with runtime.RegisterFunction.
// The second return is all types to register with runtime.RegisterType.
// Returns an error if the passed in values are not DoFns.
func registerDoFn(dofn interface{}) (interface{}, []reflect.Type, error) {
	if rt, ok := dofn.(reflect.Type); ok {
		if rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
		dofn = reflect.New(rt).Interface()
	}
	fn, err := graph.NewFn(dofn)
	if err != nil {
		return nil, nil, err
	}
	c := cache{}
	var valid bool
	// Validates that this is a DoFn or combineFn.
	do, err := graph.AsDoFn(fn, graph.MainUnknown)
	if err == nil {
		valid = true
		handleDoFn(do, c)
	} else if cmb, err2 := graph.AsCombineFn(fn); err2 == nil {
		valid = true
		handleCombineFn(cmb, c)
	}
	if !valid {
		// Return the DoFn specific error as that's more common.
		return nil, nil, err
	}

	var retFunc interface{}
	rt := reflect.TypeOf(dofn)
	switch rt.Kind() {
	case reflect.Func:
		retFunc = dofn
		c.regFuncTypes(rt)
	default:
		c.regType(rt)
	}
	var retTypes []reflect.Type
	for _, t := range c {
		retTypes = append(retTypes, t)
	}
	return retFunc, retTypes, nil
}

func handleDoFn(fn *graph.DoFn, c cache) {
	c.pullMethod(fn.SetupFn())
	c.pullMethod(fn.StartBundleFn())
	c.pullMethod(fn.ProcessElementFn())
	c.pullMethod(fn.FinishBundleFn())
	c.pullMethod(fn.TeardownFn())
	if !fn.IsSplittable() {
		return
	}
	sdf := (*graph.SplittableDoFn)(fn)
	c.pullMethod(sdf.CreateInitialRestrictionFn())
	c.pullMethod(sdf.CreateTrackerFn())
	c.pullMethod(sdf.RestrictionSizeFn())
	c.pullMethod(sdf.SplitRestrictionFn())
	c.regType(sdf.RestrictionT())
}

func handleCombineFn(fn *graph.CombineFn, c cache) {
	c.pullMethod(fn.SetupFn())
	c.pullMethod(fn.AddInputFn())
	c.pullMethod(fn.CreateAccumulatorFn())
	c.pullMethod(fn.MergeAccumulatorsFn())
	c.pullMethod(fn.CompactFn())
	c.pullMethod(fn.ExtractOutputFn())
	c.pullMethod(fn.TeardownFn())
}

type cache map[string]reflect.Type

func (c cache) pullMethod(fn *funcx.Fn) {
	if fn == nil {
		return
	}
	c.regFuncTypes(fn.Fn.Type())
}

// regFuncTypes registers the non-derived types of parameters that appear in
// the signatures.
func (c cache) regFuncTypes(ft reflect.Type) {
	for i := 0; i < ft.NumIn(); i++ {
		c.regType(ft.In(i))
	}
	for i := 0; i < ft.NumOut(); i++ {
		c.regType(ft.Out(i))
	}
}

// excludedType filters out types we don't need to register.
func excludedType(name string) bool {
	switch name {
	case "context.Context":
		return true
	}
	return false
}

func (c cache) regType(rt reflect.Type) {
	switch rt.Kind() {
	case reflect.Func:
		// Pull types from function parameters types.
		if funcx.IsEmit(rt) || funcx.IsIter(rt) || funcx.IsReIter(rt) {
			c.regFuncTypes(rt)
		}
	case reflect.Map:
		c.regType(rt.Key())
		fallthrough
	case reflect.Ptr, reflect.Array, reflect.Slice:
		c.regType(rt.Elem())
	}
	if name, ok := runtime.TypeKey(rt); ok && !excludedType(name) {
		c[name] = rt
	}
}
