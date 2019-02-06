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

package shimx

import (
	"bytes"
	"sort"
	"testing"
)

func TestTop_Sort(t *testing.T) {
	top := Top{
		Imports:   []string{"z", "a", "r"},
		Functions: []string{"z", "a", "r"},
		Types:     []string{"z", "a", "r"},
		Emitters:  []Emitter{{Name: "z"}, {Name: "a"}, {Name: "r"}},
		Inputs:    []Input{{Name: "z"}, {Name: "a"}, {Name: "r"}},
		Shims:     []Func{{Name: "z"}, {Name: "a"}, {Name: "r"}},
		Wraps:     []Wrap{{Name: "z"}, {Name: "a", Methods: []Func{{Name: "z"}, {Name: "a"}, {Name: "r"}}}, {Name: "r"}},
	}

	top.sort()
	if !sort.SliceIsSorted(top.Imports, func(i, j int) bool { return top.Imports[i] < top.Imports[j] }) {
		t.Errorf("top.Imports not sorted: got %v, want it sorted", top.Imports)
	}
	if !sort.SliceIsSorted(top.Functions, func(i, j int) bool { return top.Functions[i] < top.Functions[j] }) {
		t.Errorf("top.Types not sorted: got %v, want it sorted", top.Functions)
	}
	if !sort.SliceIsSorted(top.Types, func(i, j int) bool { return top.Types[i] < top.Types[j] }) {
		t.Errorf("top.Types not sorted: got %v, want it sorted", top.Types)
	}
	if !sort.SliceIsSorted(top.Emitters, func(i, j int) bool { return top.Emitters[i].Name < top.Emitters[j].Name }) {
		t.Errorf("top.Emitters not sorted by name: got %v, want it sorted", top.Emitters)
	}
	if !sort.SliceIsSorted(top.Inputs, func(i, j int) bool { return top.Inputs[i].Name < top.Inputs[j].Name }) {
		t.Errorf("top.Inputs not sorted by name: got %v, want it sorted", top.Inputs)
	}
	if !sort.SliceIsSorted(top.Shims, func(i, j int) bool { return top.Shims[i].Name < top.Shims[j].Name }) {
		t.Errorf("top.Shims not sorted: got %v, want it sorted", top.Shims)
	}
	if !sort.SliceIsSorted(top.Wraps, func(i, j int) bool { return top.Wraps[i].Name < top.Wraps[j].Name }) {
		t.Errorf("top.Wraps not sorted: got %v, want it sorted", top.Wraps)
	}
	for i, w := range top.Wraps {
		if !sort.SliceIsSorted(w.Methods, func(i, j int) bool { return w.Methods[i].Name < w.Methods[j].Name }) {
			t.Errorf("top.Wraps[%d] not sorted: got %v, want it sorted", i, w.Methods)
		}
	}
}

func TestTop_ProcessImports(t *testing.T) {
	needsFiltering := []string{"context", "keepit", "fmt", "io", "reflect", "unrelated"}

	tests := []struct {
		name string
		got  *Top
		want []string
	}{
		{name: "default", got: &Top{}, want: []string{"context", "keepit", "fmt", "io", "unrelated"}},
		{name: "emit", got: &Top{Emitters: []Emitter{{Name: "emit"}}}, want: []string{ExecImport, TypexImport, "keepit", "fmt", "io", "unrelated"}},
		{name: "iter", got: &Top{Inputs: []Input{{Name: "iter"}}}, want: []string{ExecImport, "context", "keepit", "unrelated"}},
		{name: "iterWTime", got: &Top{Inputs: []Input{{Name: "iterWTime", Time: true}}}, want: []string{ExecImport, TypexImport, "context", "keepit", "unrelated"}},
		{name: "shim", got: &Top{Shims: []Func{{Name: "emit"}}}, want: []string{ReflectxImport, "context", "keepit", "fmt", "io", "unrelated"}},
		{name: "iter&emit", got: &Top{Emitters: []Emitter{{Name: "emit"}}, Inputs: []Input{{Name: "iter"}}}, want: []string{ExecImport, TypexImport, "keepit", "unrelated"}},
		{name: "functions", got: &Top{Functions: []string{"func1"}}, want: []string{RuntimeImport, "context", "keepit", "fmt", "io", "unrelated"}},
		{name: "types", got: &Top{Types: []string{"func1"}}, want: []string{RuntimeImport, "context", "keepit", "fmt", "io", "unrelated"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			top := test.got
			top.Imports = needsFiltering
			top = top.processImports()
			for i := range top.Imports {
				if top.Imports[i] != test.want[i] {
					t.Fatalf("want %v, got %v", test.want, top.Imports)
				}
			}
		})
	}
}

func TestName(t *testing.T) {
	tests := []struct {
		have, want string
	}{
		{"int", "Int"},
		{"foo.MyInt", "Foo۰MyInt"},
		{"*foo.MyInt", "ᏘFoo۰MyInt"},
		{"[]beam.X", "SliceOfTypex۰X"},
		{"[4]beam.X", "ArrayOf4Typex۰X"},
		{"[53][]beam.X", "ArrayOf53SliceOfTypex۰X"},
		{"[][123]beam.X", "SliceOfArrayOf123Typex۰X"},
		{"[][123]*beam.X", "SliceOfArrayOf123ᏘTypex۰X"},
		{"*[][123]beam.X", "ᏘSliceOfArrayOf123Typex۰X"},
		{"*[][123]*beam.X", "ᏘSliceOfArrayOf123ᏘTypex۰X"},
		{"map[int]beam.X", "MapOfInt_Typex۰X"},
		{"map[string]*beam.X", "MapOfString_ᏘTypex۰X"},
	}
	for _, test := range tests {
		if got := Name(test.have); got != test.want {
			t.Errorf("Name(%v) = %v, want %v", test.have, got, test.want)
		}
	}
}

func TestFuncName(t *testing.T) {
	tests := []struct {
		in, out []string
		want    string
	}{
		{in: []string{"Int"}, out: []string{"Int"}, want: "IntГInt"},
		{in: []string{"Int"}, out: []string{}, want: "IntГ"},
		{in: []string{}, out: []string{"Bool"}, want: "ГBool"},
		{in: []string{"Bool", "String"}, out: []string{"Int", "Bool"}, want: "BoolStringГIntBool"},
		{in: []string{"String", "Map_int_typex۰X"}, out: []string{"Int", "Typex۰XSlice"}, want: "StringMap_int_typex۰XГIntTypex۰XSlice"},
	}
	for _, test := range tests {
		if got := FuncName(test.in, test.out); got != test.want {
			t.Errorf("FuncName(%v,%v) = %v, want %v", test.in, test.out, got, test.want)
		}
	}
}

func TestFile(t *testing.T) {
	top := Top{
		Package:   "gentest",
		Imports:   []string{"z", "a", "r"},
		Functions: []string{"z", "a", "r"},
		Types:     []string{"z", "a", "r"},
		Emitters: []Emitter{
			{Name: "z", Type: "func(int)", Val: "Int"},
			{Name: "a", Type: "func(bool, int) bool", Key: "bool", Val: "int"},
			{Name: "r", Type: "func(typex.EventTime, string) bool", Time: true, Val: "string"},
		},
		Inputs: []Input{
			{Name: "z", Type: "func(*int) bool"},
			{Name: "a", Type: "func(*bool, *int) bool", Key: "bool", Val: "int"},
			{Name: "r", Type: "func(*typex.EventTime, *string) bool", Time: true, Val: "string"},
		},
		Shims: []Func{
			{Name: "z", Type: "func(string, func(int))", In: []string{"string", "func(int)"}},
			{Name: "a", Type: "func(float64) (int, int)", In: []string{"float64"}, Out: []string{"int", "int"}},
			{Name: "r", Type: "func(string, func(int))", In: []string{"string", "func(int)"}},
		},
	}
	top.sort()

	var b bytes.Buffer
	if err := vampireTemplate.Funcs(funcMap).Execute(&b, top); err != nil {
		t.Errorf("error generating template: %v", err)
	}
}

func TestMkargs(t *testing.T) {
	tests := []struct {
		n           int
		format, typ string
		want        string
	}{
		{n: 0, format: "Foo", typ: "Bar", want: ""},
		{n: 1, format: "arg%d", typ: "Bar", want: "arg0 Bar"},
		{n: 4, format: "a%d", typ: "Baz", want: "a0, a1, a2, a3 Baz"},
	}
	for _, test := range tests {
		if got := mkargs(test.n, test.format, test.typ); got != test.want {
			t.Errorf("mkargs(%v,%v,%v) = %v, want %v", test.n, test.format, test.typ, got, test.want)
		}
	}
}

func TestMkparams(t *testing.T) {
	tests := []struct {
		format string
		types  []string
		want   string
	}{
		{format: "Foo", types: []string{}, want: ""},
		{format: "arg%d %v", types: []string{"Bar"}, want: "arg0 Bar"},
		{format: "a%d %v", types: []string{"Foo", "Baz", "interface{}"}, want: "a0 Foo, a1 Baz, a2 interface{}"},
	}
	for _, test := range tests {
		if got := mkparams(test.format, test.types); got != test.want {
			t.Errorf("mkparams(%v,%v) = %v, want %v", test.format, test.types, got, test.want)
		}
	}
}

func TestMktuple(t *testing.T) {
	tests := []struct {
		n    int
		v    string
		want string
	}{
		{n: 0, v: "Foo", want: ""},
		{n: 1, v: "Bar", want: "Bar"},
		{n: 4, v: "Baz", want: "Baz, Baz, Baz, Baz"},
	}
	for _, test := range tests {
		if got := mktuple(test.n, test.v); got != test.want {
			t.Errorf("mktuple(%v,%v) = %v, want %v", test.n, test.v, got, test.want)
		}
	}
}

func TestMktuplef(t *testing.T) {
	tests := []struct {
		n           int
		format, typ string
		want        string
	}{
		{n: 0, format: "Foo%d", want: ""},
		{n: 1, format: "arg%d", want: "arg0"},
		{n: 4, format: "a%d", want: "a0, a1, a2, a3"},
	}
	for _, test := range tests {
		if got := mktuplef(test.n, test.format); got != test.want {
			t.Errorf("mktuplef(%v,%v) = %v, want %v", test.n, test.format, got, test.want)
		}
	}
}

func TestMkrets(t *testing.T) {
	tests := []struct {
		types  []string
		format string
		want   string
	}{
		{types: nil, format: "%v", want: ""},
		{types: []string{}, format: "%v", want: ""},
		{types: []string{"Foo", "baz", "*imp.Bar"}, format: "%v", want: "Foo, baz, *imp.Bar"},
	}
	for _, test := range tests {
		if got := mkrets(test.format, test.types); got != test.want {
			t.Errorf("mkrets(%v,%v) = %v, want %v", test.format, test.types, got, test.want)
		}
	}
}
