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

// Package shimx specifies the templates for generating type assertion shims for
// Apache Beam Go SDK pipelines.
//
// In particular, the shims are used by the Beam Go SDK to avoid reflection at runtime,
// which is the default mode of operation. The shims are specialized for the code
// in question, using type assertion to convert arguments as required, and invoke the
// user code.
//
// Similar shims are required for emitters, and iterators in order to propagate values
// out of, and in to user functions respectively without reflection overhead.
//
// Registering user types is required to support user types as PCollection
// types, while registering functions is required to avoid possibly expensive function
// resolution at worker start up, which defaults to using DWARF Symbol tables.
//
// The generator largely relies on basic types and strings to ensure that it's usable
// by dynamic processes via reflection, or by any static analysis approach that is
// used in the future.
package shimx

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/template"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Beam imports that the generated code requires.
var (
	ExecImport     = "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/exec"
	TypexImport    = "github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	ReflectxImport = "github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	RuntimeImport  = "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime"
	SchemaImport   = "github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
	SdfImport      = "github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
)

func validateBeamImports() {
	checkImportSuffix(ExecImport, "exec")
	checkImportSuffix(TypexImport, "typex")
	checkImportSuffix(ReflectxImport, "reflectx")
	checkImportSuffix(RuntimeImport, "runtime")
	checkImportSuffix(SchemaImport, "schema")
	checkImportSuffix(SdfImport, "sdf")
}

func checkImportSuffix(path, suffix string) {
	if !strings.HasSuffix(path, suffix) {
		panic(fmt.Sprintf("expected %v to end with %v. can't generate valid code", path, suffix))
	}
}

// Top is the top level inputs into the template file for generating shims.
type Top struct {
	FileName, ToolName, Package string

	Imports   []string // the full import paths
	Functions []string // the plain names of the functions to be registered.
	Types     []string // the plain names of the types to be registered.
	Wraps     []Wrap
	Emitters  []Emitter
	Inputs    []Input
	Shims     []Func
}

// sort orders the shims consistently to minimize diffs in the generated code.
func (t *Top) sort() {
	sort.Strings(t.Imports)
	sort.Strings(t.Functions)
	sort.Strings(t.Types)
	sort.SliceStable(t.Emitters, func(i, j int) bool {
		return t.Emitters[i].Name < t.Emitters[j].Name
	})
	sort.SliceStable(t.Inputs, func(i, j int) bool {
		return t.Inputs[i].Name < t.Inputs[j].Name
	})
	sort.SliceStable(t.Shims, func(i, j int) bool {
		return t.Shims[i].Name < t.Shims[j].Name
	})
	sort.SliceStable(t.Wraps, func(i, j int) bool {
		return t.Wraps[i].Name < t.Wraps[j].Name
	})
	for _, w := range t.Wraps {
		sort.SliceStable(w.Methods, func(i, j int) bool {
			return w.Methods[i].Name < w.Methods[j].Name
		})
	}
}

// processImports removes imports that are otherwise handled by the template
// This method is on the value to shallow copy the Field references to avoid
// mutating the user provided instance.
func (t Top) processImports() *Top {
	pred := map[string]bool{"reflect": true}
	var filtered []string
	if len(t.Emitters) > 0 {
		pred["context"] = true
		filtered = append(filtered, SdfImport)
		pred[SdfImport] = true
	}
	if len(t.Inputs) > 0 {
		pred["fmt"] = true
		pred["io"] = true
	}
	// This should definitley be happening earlier though.
	var filteredTypes []string
	for _, t := range t.Types {
		if !strings.HasPrefix(t, "beam.") {
			filteredTypes = append(filteredTypes, t)
		}
	}
	t.Types = filteredTypes
	if len(t.Types) > 0 {
		filtered = append(filtered, SchemaImport)
		pred[SchemaImport] = true
	}
	if len(t.Types) > 0 || len(t.Functions) > 0 {
		filtered = append(filtered, RuntimeImport)
		pred[RuntimeImport] = true
	}
	if len(t.Shims) > 0 {
		filtered = append(filtered, ReflectxImport)
		pred[ReflectxImport] = true
	}
	if len(t.Emitters) > 0 || len(t.Inputs) > 0 {
		filtered = append(filtered, ExecImport)
		pred[ExecImport] = true
	}
	needTypexImport := len(t.Emitters) > 0
	for _, i := range t.Inputs {
		if i.Time {
			needTypexImport = true
			break
		}
	}
	if needTypexImport {
		filtered = append(filtered, TypexImport)
		pred[TypexImport] = true
	}
	for _, imp := range t.Imports {
		if !pred[imp] {
			filtered = append(filtered, imp)
		}
	}
	t.Imports = filtered
	return &t
}

// Emitter represents an emitter shim to be generated.
type Emitter struct {
	Name, Type string // The user name of the function, the type of the emit.
	Time       bool   // if this uses event time.
	Key, Val   string // The type of the emits.
}

// Input represents an iterator shim to be generated.
type Input struct {
	Name, Type string // The user name of the function, the type of the iterator (including the bool).
	Time       bool   // if this uses event time.
	Key, Val   string // The type of the inputs, pointers removed.
}

// Func represents a type assertion shim for function invocation to be generated.
type Func struct {
	Name, Type string
	In, Out    []string
}

// Wrap represents a type assertion shim for Structural DoFn method
// invocation to be generated.
type Wrap struct {
	Name, Type string
	Methods    []Func
}

// Name creates a capitalized identifier from a type string. The identifier
// follows the rules of go identifiers and should be compileable.
// See https://golang.org/ref/spec#Identifiers for details.
func Name(t string) string {
	if strings.HasPrefix(t, "[]") {
		return "SliceOf" + Name(t[2:])
	}
	if strings.HasPrefix(t, "map[") {
		if i := strings.Index(t, "]"); i >= 0 {
			// It should read MapOfKeyTypeName_ValueTypeName.
			return "MapOf" + Name(t[4:i]) + "_" + Name(t[i+1:])
		}
	}
	// Handle arrays.
	if strings.HasPrefix(t, "[") {
		if i := strings.Index(t, "]"); i >= 0 {
			// It should read ArrayOfNTypeName.
			return "ArrayOf" + t[1:i] + Name(t[i+1:])
		}
	}
	if strings.HasPrefix(t, "*") {
		return "Ꮨ" + Name(t[1:])
	}

	t = strings.Replace(t, "beam.", "typex.", -1)
	t = strings.Replace(t, ".", "۰", -1) // For packages
	return cases.Title(language.Und, cases.NoLower).String(t)
}

// FuncName returns a compilable Go identifier for a function, given valid
// type names as generated by Name.
// See https://golang.org/ref/spec#Identifiers for details.
func FuncName(inNames, outNames []string) string {
	return fmt.Sprintf("%sГ%s", strings.Join(inNames, ""), strings.Join(outNames, ""))
}

// File writes go code to the given writer.
func File(w io.Writer, top *Top) {
	validateBeamImports()
	top = top.processImports()
	top.sort()
	vampireTemplate.Funcs(funcMap).Execute(w, top)
}

var vampireTemplate = template.Must(template.New("vampire").Funcs(funcMap).Parse(`// Code generated by {{.ToolName}}. DO NOT EDIT.
// File: {{.FileName}}

package {{.Package}}

import (

{{- if .Emitters}}
	"context"
{{- end}}
{{- if .Inputs}}
	"fmt"
	"io"
{{- end}}
	"reflect"
{{- if .Imports}}

	// Library imports
{{- end}}
{{- range $import := .Imports}}
	"{{$import}}"
{{- end}}
)

func init() {
{{- range $x := .Functions}}
	runtime.RegisterFunction({{$x}})
{{- end}}
{{- range $x := .Types}}
	runtime.RegisterType(reflect.TypeOf((*{{$x}})(nil)).Elem())
	schema.RegisterType(reflect.TypeOf((*{{$x}})(nil)).Elem())
{{- end}}
{{- range $x := .Wraps}}
	reflectx.RegisterStructWrapper(reflect.TypeOf((*{{$x.Type}})(nil)).Elem(), wrapMaker{{$x.Name}})
{{- end}}
{{- range $x  := .Shims}}
	reflectx.RegisterFunc(reflect.TypeOf((*{{$x.Type}})(nil)).Elem(), funcMaker{{$x.Name}})
{{- end}}
{{- range $x := .Emitters}}
	exec.RegisterEmitter(reflect.TypeOf((*{{$x.Type}})(nil)).Elem(), emitMaker{{$x.Name}})
{{- end}}
{{- range $x := .Inputs}}
	exec.RegisterInput(reflect.TypeOf((*{{$x.Type}})(nil)).Elem(), iterMaker{{$x.Name}})
{{- end}}
}

{{range $x := .Wraps -}}
func wrapMaker{{$x.Name}}(fn any) map[string]reflectx.Func {
	dfn := fn.(*{{$x.Type}})
	return map[string]reflectx.Func{
	{{- range $y := .Methods}}
		"{{$y.Name}}": reflectx.MakeFunc(func({{mkparams "a%d %v" $y.In}}) {{if $y.Out}}({{mkrets "%v" $y.Out}}) { return {{else -}} { {{end -}} dfn.{{$y.Name}}({{mktuplef (len $y.In) "a%d" }}) }),
	{{- end}}
	}
}

{{end}}
{{- range $x  := .Shims -}}
type caller{{$x.Name}} struct {
	fn {{$x.Type}}
}

func funcMaker{{$x.Name}}(fn any) reflectx.Func {
	f := fn.({{$x.Type}})
	return &caller{{$x.Name}}{fn: f}
}

func (c *caller{{$x.Name}}) Name() string {
	return reflectx.FunctionName(c.fn)
}

func (c *caller{{$x.Name}}) Type() reflect.Type {
	return reflect.TypeOf(c.fn)
}

func (c *caller{{$x.Name}}) Call(args []any) []any {
	{{mktuplef (len $x.Out) "out%d"}}{{- if len $x.Out}} := {{end -}}c.fn({{mkparams "args[%d].(%v)" $x.In}})
	return []any{ {{- mktuplef (len $x.Out) "out%d" -}} }
}

func (c *caller{{$x.Name}}) Call{{len $x.In}}x{{len $x.Out}}({{mkargs (len $x.In) "arg%v" "any"}}) ({{- mktuple (len $x.Out) "any"}}) {
	{{if len $x.Out}}return {{end}}c.fn({{mkparams "arg%d.(%v)" $x.In}})
}

{{end}}
{{- if .Emitters -}}
type emitNative struct {
	n     exec.ElementProcessor
	fn    any
	est   *sdf.WatermarkEstimator

	ctx context.Context
	ws  []typex.Window
	et  typex.EventTime
	value exec.FullValue
}

func (e *emitNative) Init(ctx context.Context, ws []typex.Window, et typex.EventTime) error {
	e.ctx = ctx
	e.ws = ws
	e.et = et
	return nil
}

func (e *emitNative) Value() any {
	return e.fn
}

func (e *emitNative) AttachEstimator(est *sdf.WatermarkEstimator) {
	e.est = est
}

{{end}}
{{- range $x := .Emitters -}}
func emitMaker{{$x.Name}}(n exec.ElementProcessor) exec.ReusableEmitter {
	ret := &emitNative{n: n}
	ret.fn = ret.invoke{{.Name}}
	return ret
}

func (e *emitNative) invoke{{$x.Name}}({{if $x.Time -}} t typex.EventTime, {{end}}{{if $x.Key}}key {{$x.Key}}, {{end}}val {{$x.Val}}) {
	e.value = exec.FullValue{Windows: e.ws, Timestamp: {{- if $x.Time}} t{{else}} e.et{{end}}, {{- if $x.Key}} Elm: key, Elm2: val {{else}} Elm: val{{end -}} }
	if e.est != nil {
		(*e.est).(sdf.TimestampObservingEstimator).ObserveTimestamp({{- if $x.Time}} t.ToTime(){{else}} e.et.ToTime(){{end}})
	}
	if err := e.n.ProcessElement(e.ctx, &e.value); err != nil {
		panic(err)
	}
}

{{end}}
{{- if .Inputs -}}
type iterNative struct {
	s     exec.ReStream
	fn    any

	// cur is the "current" stream, if any.
	cur exec.Stream
}

func (v *iterNative) Init() error {
	cur, err := v.s.Open()
	if err != nil {
		return err
	}
	v.cur = cur
	return nil
}

func (v *iterNative) Value() any {
	return v.fn
}

func (v *iterNative) Reset() error {
	if err := v.cur.Close(); err != nil {
		return err
	}
	v.cur = nil
	return nil
}

{{end}}
{{- range $x := .Inputs -}}
func iterMaker{{$x.Name}}(s exec.ReStream) exec.ReusableInput {
	ret := &iterNative{s: s}
	ret.fn = ret.read{{$x.Name}}
	return ret
}

func (v *iterNative) read{{$x.Name}}({{if $x.Time -}} et *typex.EventTime, {{end}}{{if $x.Key}}key *{{$x.Key}}, {{end}}value *{{$x.Val}}) bool {
	elm, err := v.cur.Read()
	if err != nil {
		if err == io.EOF {
			return false
		}
		panic(fmt.Sprintf("broken stream: %v", err))
	}

{{- if $x.Time}}
	*et = elm.Timestamp
{{- end}}
{{- if $x.Key}}
	*key = elm.Elm.({{$x.Key}})
{{- end}}
	*value = elm.Elm{{- if $x.Key -}} 2 {{- end -}}.({{$x.Val}})
	return true
}

{{end}}
// DO NOT MODIFY: GENERATED CODE
`))

// funcMap contains useful functions for use in the template.
var funcMap template.FuncMap = map[string]any{
	"mkargs":   mkargs,
	"mkparams": mkparams,
	"mkrets":   mkrets,
	"mktuple":  mktuple,
	"mktuplef": mktuplef,
}

// mkargs(n, type) returns "<fmt.Sprintf(format, 0)>, .., <fmt.Sprintf(format, n-1)> type".
// If n is 0, it returns the empty string.
func mkargs(n int, format, typ string) string {
	if n == 0 {
		return ""
	}
	return fmt.Sprintf("%v %v", mktuplef(n, format), typ)
}

// mkparams(format, []type) returns "<fmt.Sprintf(format, 0, type[0])>, .., <fmt.Sprintf(format, n-1, type[n-1])>".
func mkparams(format string, types []string) string {
	var ret []string
	for i, t := range types {
		ret = append(ret, fmt.Sprintf(format, i, t))
	}
	return strings.Join(ret, ", ")
}

// mkrets(format, []type) returns "<fmt.Sprintf(format, type[0])>, .., <fmt.Sprintf(format, type[n-1])>".
func mkrets(format string, types []string) string {
	var ret []string
	for _, t := range types {
		ret = append(ret, fmt.Sprintf(format, t))
	}
	return strings.Join(ret, ", ")
}

// mktuple(n, v) returns "v, v, ..., v".
func mktuple(n int, v string) string {
	var ret []string
	for i := 0; i < n; i++ {
		ret = append(ret, v)
	}
	return strings.Join(ret, ", ")
}

// mktuplef(n, format) returns "<fmt.Sprintf(format, 0)>, .., <fmt.Sprintf(format, n-1)>"
func mktuplef(n int, format string) string {
	var ret []string
	for i := 0; i < n; i++ {
		ret = append(ret, fmt.Sprintf(format, i))
	}
	return strings.Join(ret, ", ")
}
