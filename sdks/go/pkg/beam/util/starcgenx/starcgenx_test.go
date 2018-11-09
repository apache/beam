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

package starcgenx

import (
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestExtractor(t *testing.T) {
	tests := []struct {
		name     string
		pkg      string
		files    []string
		ids      []string
		expected []string
		excluded []string
	}{
		{name: "pardo1", files: []string{pardo}, pkg: "pardo",
			expected: []string{"runtime.RegisterFunction(MyIdent)", "runtime.RegisterFunction(MyDropVal)", "runtime.RegisterFunction(MyOtherDoFn)", "runtime.RegisterType(reflect.TypeOf((*foo)(nil)).Elem())", "funcMakerStringГString", "funcMakerIntStringГInt", "funcMakerFooГStringFoo"},
		},
		{name: "emits1", files: []string{emits}, pkg: "emits",
			expected: []string{"runtime.RegisterFunction(anotherFn)", "runtime.RegisterFunction(emitFn)", "runtime.RegisterType(reflect.TypeOf((*reInt)(nil)).Elem())", "funcMakerEmitIntIntГ", "emitMakerIntInt", "funcMakerIntIntEmitIntIntГError"},
		},
		{name: "iters1", files: []string{iters}, pkg: "iters",
			expected: []string{"runtime.RegisterFunction(iterFn)", "funcMakerStringIterIntГ", "iterMakerInt"},
		},
		{name: "structs1", files: []string{structs}, pkg: "structs", ids: []string{"myDoFn"},
			expected: []string{"runtime.RegisterType(reflect.TypeOf((*myDoFn)(nil)).Elem())", "funcMakerEmitIntГ", "emitMakerInt", "funcMakerValTypeValTypeEmitIntГ", "runtime.RegisterType(reflect.TypeOf((*valType)(nil)).Elem())"},
			excluded: []string{"funcMakerStringГ", "emitMakerString", "nonPipelineType"},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			fset := token.NewFileSet()
			var fs []*ast.File
			for i, f := range test.files {
				n, err := parser.ParseFile(fset, "", f, 0)
				if err != nil {
					t.Fatalf("couldn't parse test.files[%d]: %v", i, err)
				}
				fs = append(fs, n)
			}
			e := NewExtractor(test.pkg)
			e.Ids = test.ids
			if err := e.FromAsts(importer.Default(), fset, fs); err != nil {
				t.Fatal(err)
			}
			data := e.Generate("test_shims.go")
			s := string(data)
			for _, i := range test.expected {
				if !strings.Contains(s, i) {
					t.Errorf("expected %q in generated file", i)
				}
			}
			for _, i := range test.excluded {
				if strings.Contains(s, i) {
					t.Errorf("found %q in generated file", i)
				}
			}
			t.Log(s)
		})
	}
}

const pardo = `
package pardo

func MyIdent(v string) string {
	return v
}

func MyDropVal(k int,v string) int {
	return k
}

// A user defined type
type foo struct{}

func MyOtherDoFn(v foo) (string,foo) {
	return "constant"
}
`

const emits = `
package emits

type reInt int

func anotherFn(emit func(int,int)) {
	emit(0, 0)
}

func emitFn(k,v int, emit func(int,int)) error {
	for i := 0; i < v; i++ { emit(k, i) }
	return nil
}
`
const iters = `
package iters

func iterFn(k string, iters func(*int) bool) {}
`

const structs = `
package structs

type myDoFn struct{}

// valType should be picked up via processElement
type valType int

func (f *myDoFn) ProcessElement(k, v valType, emit func(int)) {}

func (f *myDoFn) Setup(emit func(int)) {}
func (f *myDoFn) StartBundle(emit func(int)) {}
func (f *myDoFn) FinishBundle(emit func(int)) error {}
func (f *myDoFn) Teardown(emit func(int)) {}

type nonPipelineType int

// UnrelatedMethods shouldn't have shims or tangents generated for them
func (f *myDoFn) UnrelatedMethod1(v string) {}
func (f *myDoFn) UnrelatedMethod2(notEmit func(string)) {}

func (f *myDoFn) UnrelatedMethod3(notEmit func(nonPipelineType)) {}
`
