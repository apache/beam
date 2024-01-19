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

// Package vet is a Beam runner that "runs" a pipeline by producing
// generated code to avoid symbol table lookups and reflection in pipeline
// execution.
//
// This runner isn't necessarily intended to be run by itself. Other runners
// can use this as a sanity check on whether a given pipeline avoids known
// performance bottlenecks.
//
// TODO(https://github.com/apache/beam/issues/19402): Add usage documentation.
package vet

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/util/shimx"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

func init() {
	beam.RegisterRunner("vet", Execute)
}

// We want clear failures when looking up symbols so we can tell if something has been
// registered properly or not.
type disabledResolver bool

func (p disabledResolver) Sym2Addr(name string) (uintptr, error) {
	return 0, errors.Errorf("%v not found. Register DoFns and functions with the beam/register package.", name)
}

// Execute evaluates the pipeline on whether it can run without reflection.
func Execute(ctx context.Context, p *beam.Pipeline) (beam.PipelineResult, error) {
	e, err := Evaluate(ctx, p)
	if err != nil {
		return nil, errors.WithContext(err, "validating pipeline with vet runner")
	}
	if !e.Performant() {
		e.summary()
		e.Generate("main")
		e.diag("*/\n")
		err := errors.Errorf("pipeline is not performant, see diagnostic summary:\n%s\n%s", e.d.String(), string(e.Bytes()))
		err = errors.WithContext(err, "validating pipeline with vet runner")
		return nil, errors.SetTopLevelMsg(err, "pipeline is not performant")
	}
	// Pipeline nas no further tasks.
	return nil, nil
}

// Evaluate returns an object that can generate necessary shims and inits.
func Evaluate(_ context.Context, p *beam.Pipeline) (*Eval, error) {
	// Disable the resolver so we can see functions that are that are already registered.
	r := runtime.Resolver
	runtime.Resolver = disabledResolver(false)
	// Reinstate the resolver when we're through.
	defer func() { runtime.Resolver = r }()

	edges, _, err := p.Build()
	if err != nil {
		return nil, errors.New("can't get data to generate")
	}

	e := newEval()

	e.diag("/**\n")
	e.extractFromMultiEdges(edges)
	return e, nil
}

func newEval() *Eval {
	return &Eval{
		functions:   make(map[string]*funcx.Fn),
		types:       make(map[string]reflect.Type),
		funcs:       make(map[string]reflect.Type),
		emits:       make(map[string]reflect.Type),
		iters:       make(map[string]reflect.Type),
		imports:     make(map[string]struct{}),
		allExported: true,
	}
}

// Eval contains and uniquifies the cache of types and things that need to be generated.
type Eval struct {
	// d is a buffer for the diagnostic information produced when evaluating the pipeline.
	// w is the primary output buffer.
	d, w bytes.Buffer

	// Register and uniquify the needed shims for each kind.
	// Functions to Register
	functions map[string]*funcx.Fn
	// Types to Register (structs, essentially)
	types map[string]reflect.Type
	// FuncShims needed
	funcs map[string]reflect.Type
	// Emitter Shims needed
	emits map[string]reflect.Type
	// Iterator Shims needed
	iters map[string]reflect.Type

	// list of packages we need to import.
	imports map[string]struct{}

	allExported bool // Marks if all ptransforms are exported and available in main.
}

// extractFromMultiEdges audits the given pipeline edges so we can determine if
// this pipeline will run without reflection.
func (e *Eval) extractFromMultiEdges(edges []*graph.MultiEdge) {
	e.diag("PTransform Audit:\n")
	for _, edge := range edges {
		switch edge.Op {
		case graph.ParDo:
			// Gets the ParDo's identifier
			e.diagf("pardo %s", edge.Name())
			e.extractGraphFn((*graph.Fn)(edge.DoFn))
		case graph.Combine:
			e.diagf("combine %s", edge.Name())
			e.extractGraphFn((*graph.Fn)(edge.CombineFn))
		default:
			continue
		}
		e.diag("\n")
	}
}

// Performant returns whether this pipeline needs additional registrations
// to avoid reflection, or symbol lookups at runtime.
func (e *Eval) Performant() bool {
	return !e.RequiresRegistrations() && !e.UsesDefaultReflectionShims()
}

// RequiresRegistrations returns if there are any types or functions that require
// registrations.
func (e *Eval) RequiresRegistrations() bool {
	return (len(e.functions) + len(e.types)) > 0
}

// UsesDefaultReflectionShims returns whether the default reflection shims are going
// to be used by the pipeline.
func (e *Eval) UsesDefaultReflectionShims() bool {
	return (len(e.funcs) + len(e.emits) + len(e.iters)) > 0
}

// AllExported returns whether all values in the pipeline are exported,
// and thus it may be possible to patch the pipeline's package with
// generated shims.
// Using exported vs unexported identifiers does not affect pipeline performance
// but does matter on if the pipeline package can do anything about it.
func (e *Eval) AllExported() bool {
	return e.allExported
}

func (e *Eval) summary() {
	e.diag("\n")
	e.diag("Summary\n")
	e.diagf("All exported?: %v\n", e.AllExported())
	e.diagf("%d\t Imports\n", len(e.imports))
	e.diagf("%d\t Functions\n", len(e.functions))
	e.diagf("%d\t Types\n", len(e.types))
	e.diagf("%d\t Shims\n", len(e.funcs))
	e.diagf("%d\t Emits\n", len(e.emits))
	e.diagf("%d\t Inputs\n", len(e.iters))

	if e.Performant() {
		e.diag("Pipeline is performant!\n")
	} else {
		e.diag("Pipeline is not performant:\n")
		if e.RequiresRegistrations() {
			e.diag("\trequires additional type or function registration\n")
		}
		if e.UsesDefaultReflectionShims() {
			e.diag("\trequires additional shim generation\n")
		}
		if e.AllExported() {
			e.diag("\tGood News! All identifiers are exported; the pipeline's package can be patched with generated output.\n")
		}
	}
}

// NameType turns a reflect.Type into a string based on its name.
// It prefixes Emit or Iter if the function satisfies the constraints of those types.
func NameType(t reflect.Type) string {
	if emt, ok := makeEmitter(t); ok {
		return "Emit" + emt.Name
	}
	if ipt, ok := makeInput(t); ok {
		return "Iter" + ipt.Name
	}
	return shimx.Name(t.String())
}

// Generate produces a go file under the given package.
func (e *Eval) Generate(packageName string) {
	// Here's where we shove everything into the Top template type.
	// Need to swap in typex.* for beam.* where appropriate.
	e.diag("Diagnostic output pre-amble for the code generator\n")

	e.diag("Functions\n")
	var functions []string
	for fn, t := range e.functions {
		e.diagf("%s, %v\n", fn, t)
		n := strings.Split(fn, ".")
		// If this is the main package, we don't need the package qualifier
		if n[0] == "main" {
			functions = append(functions, n[1])
		} else {
			functions = append(functions, fn)
		}
	}
	e.diag("Types\n")
	var types []string
	for fn, t := range e.types {
		e.diagf("%s, %v\n", fn, t)
		n := strings.Split(fn, ".")
		// If this is the main package, we don't need the package qualifier
		if n[0] == "main" {
			types = append(types, n[1])
		} else {
			types = append(types, fn)
		}
	}
	e.diag("Shims\n")
	var shims []shimx.Func
	for fn, t := range e.funcs {
		e.diagf("%s, %v\n", fn, t)
		shim := shimx.Func{Type: t.String()}
		var inNames []string
		for i := 0; i < t.NumIn(); i++ {
			s := t.In(i)
			shim.In = append(shim.In, s.String())
			inNames = append(inNames, NameType(s))
		}
		var outNames []string
		for i := 0; i < t.NumOut(); i++ {
			s := t.Out(i)
			shim.Out = append(shim.Out, s.String())
			outNames = append(outNames, NameType(s))
		}
		shim.Name = shimx.FuncName(inNames, outNames)
		shims = append(shims, shim)
	}
	e.diag("Emitters\n")
	var emitters []shimx.Emitter
	for k, t := range e.emits {
		e.diagf("%s, %v\n", k, t)
		emt, ok := makeEmitter(t)
		if !ok {
			panic(fmt.Sprintf("%v is not an emit, but we expected it to be one.", t))
		}
		emitters = append(emitters, emt)
	}
	e.diag("Iterators \n")
	var inputs []shimx.Input
	for ipt, t := range e.iters {
		e.diagf("%s, %v\n", ipt, t)
		itr, ok := makeInput(t)
		if !ok {
			panic(fmt.Sprintf("%v is not an emit, but we expected it to be one.", t))
		}
		inputs = append(inputs, itr)
	}
	var imports []string
	for k := range e.imports {
		if k == "" {
			continue
		}
		imports = append(imports, k)
	}

	top := shimx.Top{
		Package:   packageName,
		Imports:   imports,
		Functions: functions,
		Types:     types,
		Shims:     shims,
		Emitters:  emitters,
		Inputs:    inputs,
	}
	shimx.File(&e.w, &top)
}

func makeEmitter(t reflect.Type) (shimx.Emitter, bool) {
	types, isEmit := funcx.UnfoldEmit(t)
	if !isEmit {
		return shimx.Emitter{}, false
	}
	emt := shimx.Emitter{Type: t.String()}
	switch len(types) {
	case 1:
		emt.Time = false
		emt.Val = types[0].String()
	case 2:
		if types[0] == typex.EventTimeType {
			emt.Time = true
		} else {
			emt.Key = types[0].String()
		}
		emt.Val = types[1].String()
	case 3:
		// If there's 3, the first one must be typex.EvalentTime.
		emt.Time = true
		emt.Key = types[1].String()
		emt.Val = types[2].String()
	}
	if emt.Time {
		emt.Name = fmt.Sprintf("ET%s%s", shimx.Name(emt.Key), shimx.Name(emt.Val))
	} else {
		emt.Name = fmt.Sprintf("%s%s", shimx.Name(emt.Key), shimx.Name(emt.Val))
	}
	return emt, true
}

func makeInput(t reflect.Type) (shimx.Input, bool) {
	itr := shimx.Input{Type: t.String()}
	types, isIter := funcx.UnfoldIter(t)
	if !isIter {
		return shimx.Input{}, false
	}
	switch len(types) {
	case 1:
		itr.Time = false
		itr.Val = types[0].String()
	case 2:
		if types[0] == typex.EventTimeType {
			itr.Time = true
		} else {
			itr.Key = types[0].String()
		}
		itr.Val = types[1].String()
	case 3:
		// If there's 3, the first one must be typex.EventTime.
		itr.Time = true
		itr.Key = types[1].String()
		itr.Val = types[2].String()
	}
	if itr.Time {
		itr.Name = fmt.Sprintf("ET%s%s", shimx.Name(itr.Key), shimx.Name(itr.Val))
	} else {
		itr.Name = fmt.Sprintf("%s%s", shimx.Name(itr.Key), shimx.Name(itr.Val))
	}
	return itr, true
}

// needFunction marks the function itself needs to be registered
func (e *Eval) needFunction(fn *funcx.Fn) {
	k := fn.Fn.Name()
	if _, ok := e.functions[k]; ok {
		e.diag(" FUNCTION_COVERED")
	} else {
		e.diag(" NEED_FUNCTION") // Needs a RegisterFunction
		e.functions[k] = fn
		e.needImport(fn.Fn.Name())
	}
}

// needImport registers the given identifier's import for including in generation.
func (e *Eval) needImport(p string) {
	// If this is a reflect.methodValueCall, this is covered by the type
	// check already, so we don't need to do anything.
	if p == "reflect.methodValueCall" {
		return
	}
	// Split at last '.' to get full package name and identifier name.
	splitInd := strings.LastIndexByte(p, '.')
	pp := p[:splitInd]

	// If it's ad-hoc, or in main we can't/won't import it.
	if pp == "main" || pp == "" {
		return
	}

	// Check if the identifier is exported
	r, _ := utf8.DecodeRuneInString(p[splitInd+1:])
	if !unicode.IsUpper(r) {
		e.allExported = false
		return
	}
	e.imports[pp] = struct{}{}
	e.diagf("\n%s\n", pp)
}

// needShim marks the function's type signature as needing to be specialized.
func (e *Eval) needShim(fn *funcx.Fn) {
	k := fn.Fn.Type().String()
	if _, ok := e.funcs[k]; ok {
		e.diag(" SHIM_COVERED")
	} else {
		e.diag(" NEED_SHIM") // Needs a RegisterFunc
		e.funcs[k] = fn.Fn.Type()
		e.needImport(fn.Fn.Name())
	}
}

// needType marks the struct's type signature as needing to be specialized.
func (e *Eval) needType(k string, rt reflect.Type) {
	if _, ok := e.types[k]; ok {
		e.diag(" OK")
	} else {
		e.diag(" NEED_TYPE") // Needs a RegisterType
		e.types[k] = rt
		e.needImport(k)
	}
}

// needEmit marks the emit parameter as needed specialization
func (e *Eval) needEmit(rt reflect.Type) {
	k := fmt.Sprintf("%v", rt)
	if exec.IsEmitterRegistered(rt) {
		e.diag(" OK")
		return
	}
	if _, ok := e.emits[k]; ok {
		e.diag(" EMIT_COVERED")
	} else {
		e.diagf(" NEED_EMIT[%v]", rt) // Needs a RegisterEmit
		e.emits[k] = rt
	}
}

// needInput marks the iterator parameter as needed specialization
func (e *Eval) needInput(rt reflect.Type) {
	k := fmt.Sprintf("%v", rt)
	if exec.IsInputRegistered(rt) {
		e.diag(" OK")
		return
	}
	if _, ok := e.iters[k]; ok {
		e.diag(" INPUT_COVERED")
	} else {
		e.diagf(" NEED_INPUT[%v]", rt) // Needs a RegisterInput
		e.iters[k] = rt
	}
}

// diag invokes fmt.Fprint on the diagnostic buffer.
func (e *Eval) diag(s string) {
	fmt.Fprint(&e.d, s)
}

// diag invokes fmt.Fprintf on the diagnostic buffer.
func (e *Eval) diagf(f string, args ...any) {
	fmt.Fprintf(&e.d, f, args...)
}

// Print invokes fmt.Fprint on the Eval buffer.
func (e *Eval) Print(s string) {
	fmt.Fprint(&e.w, s)
}

// Printf invokes fmt.Fprintf on the Eval buffer.
func (e *Eval) Printf(f string, args ...any) {
	fmt.Fprintf(&e.w, f, args...)
}

// Bytes returns the Eval buffer's bytes for file writing.
func (e *Eval) Bytes() []byte {
	return e.w.Bytes()
}

// We need to take graph.Fns (which can be created from any from graph.NewFn)
// and convert them to all needed function caller signatures,
// and emitters.
//
// The type assertion shim Funcs need to be registered with reflectx.RegisterFunc
// Emitters need to be registered with exec.RegisterEmitter
// Iterators with exec.RegisterInput
// The types need to be registered with beam.RegisterType
// The user functions need to be registered with beam.RegisterFunction
//
// Registrations are all on the concrete element type, rather than the
// pointer type.

// extractGraphFn does the analysis of the function and determines what things need generating.
// A single line is used, unless it's a struct, at which point one line per implemented method
// is used.
func (e *Eval) extractGraphFn(fn *graph.Fn) {
	if fn.DynFn != nil {
		// TODO(https://github.com/apache/beam/issues/19401) handle dynamics if necessary (probably not since it's got general function handling)
		e.diag(" dynamic function")
		return
	}
	if fn.Recv != nil {
		e.diagf(" struct[[%T]]", fn.Recv)

		rt := reflectx.SkipPtr(reflect.TypeOf(fn.Recv)) // We need the value not the pointer that's used.
		if tk, ok := runtime.TypeKey(rt); ok {
			if t, found := runtime.LookupType(tk); !found {
				e.needType(tk, rt)
			} else {
				e.diagf(" FOUND %v", t) // Doesn't need a RegisterType
			}
		} else {
			e.diagf(" CANT REGISTER %v %v %v", rt, rt.PkgPath(), rt.Name())
		}
		e.extractFromDoFn((*graph.DoFn)(fn))
		e.extractFromCombineFn((*graph.CombineFn)(fn))
	}

	if fn.Fn != nil {
		// This goes here since methods don't need registering. That's handled by the type.
		f := fn.Fn.Fn
		if _, err := runtime.ResolveFunction(f.Name(), f.Type()); err != nil {
			e.needFunction(fn.Fn) // Need a RegisterFunction
		}
		e.extractFuncxFn(fn.Fn)
	}
}

type mthd struct {
	m    func() *funcx.Fn
	name string
}

func (e *Eval) extractFromCombineFn(cmbfn *graph.CombineFn) {
	methods := []mthd{
		{cmbfn.SetupFn, "SetupFn"},
		{cmbfn.CreateAccumulatorFn, "CreateAccumulatorFn"},
		{cmbfn.AddInputFn, "AddInputFn"},
		{cmbfn.MergeAccumulatorsFn, "MergeAccumulatorsFn"},
		{cmbfn.ExtractOutputFn, "ExtractOutputFn"},
		{cmbfn.CompactFn, "CompactFn"},
		{cmbfn.TeardownFn, "TeardownFn"},
	}
	e.extractMethods(methods)
}

func (e *Eval) extractFromDoFn(dofn *graph.DoFn) {
	methods := []mthd{
		{dofn.SetupFn, "SetupFn"},
		{dofn.StartBundleFn, "StartBundleFn"},
		{dofn.ProcessElementFn, "ProcessElementFn"},
		{dofn.FinishBundleFn, "FinishBundleFn"},
		{dofn.TeardownFn, "TeardownFn"},
	}
	e.extractMethods(methods)
}

func (e *Eval) extractMethods(methods []mthd) {
	for _, m := range methods {
		if mfn := m.m(); mfn != nil {
			e.diag("\n\t- ")
			e.diag(m.name)
			e.extractFuncxFn(mfn)
		}
	}
}

// extractFuncxFn writes everything to the same line marking things as registered or not as needed.
func (e *Eval) extractFuncxFn(fn *funcx.Fn) {
	t := fn.Fn.Type()
	e.diagf(" function[[%v]]", t)
	// We don't have access to the maps directly, so we can sanity check if we need
	// a shim by checking against this type.
	if shim := fmt.Sprintf("%T", fn.Fn); shim == "*reflectx.reflectFunc" {
		e.needShim(fn) // Need a generated Shim and RegisterFunc
	}
	// Need to extract emitter types and iterator types for specialization.
	// We're "stuck" always generating these all the time, since we
	// can't tell what's already registered at this level.
	for _, p := range fn.Param {
		switch p.Kind {
		case funcx.FnEmit:
			e.needEmit(p.T) // Need a generated emitter and RegisterEmitter
		case funcx.FnIter:
			e.needInput(p.T) // Need a generated iter and RegisterInput
		case funcx.FnReIter:
			e.needInput(p.T) // ???? Might be unnecessary?
		}
	}
}
