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

package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

func TestGenerate(t *testing.T) {
	tests := []struct {
		name     string
		pkg      string
		files    []string
		ids      []string
		expected []string
		excluded []string
	}{
		{name: "genAllSingleFile", files: []string{hello1}, pkg: "hello", ids: []string{},
			expected: []string{"runtime.RegisterFunction(MyTitle)", "runtime.RegisterFunction(MyOtherDoFn)", "runtime.RegisterType(reflect.TypeOf((*foo)(nil)).Elem())", "funcMakerContext۰ContextStringГString", "funcMakerFooГString"},
		},
		{name: "genSpecificSingleFile", files: []string{hello1}, pkg: "hello", ids: []string{"MyTitle"},
			expected: []string{"runtime.RegisterFunction(MyTitle)", "funcMakerContext۰ContextStringГString"},
			excluded: []string{"MyOtherDoFn", "runtime.RegisterType(reflect.TypeOf((*foo)(nil)).Elem())", "funcMakerFooГString"},
		},
		{name: "genAllMultiFile", files: []string{hello1, hello2}, pkg: "hello", ids: []string{},
			expected: []string{"runtime.RegisterFunction(MyTitle)", "runtime.RegisterFunction(MyOtherDoFn)", "runtime.RegisterFunction(anotherFn)", "runtime.RegisterType(reflect.TypeOf((*foo)(nil)).Elem())", "funcMakerContext۰ContextStringГString", "funcMakerFooГString", "funcMakerShimx۰EmitterГString", "funcMakerShimx۰EmitterГFoo"},
		},
		{name: "genSpecificMultiFile1", files: []string{hello1, hello2}, pkg: "hello", ids: []string{"MyTitle"},
			expected: []string{"runtime.RegisterFunction(MyTitle)", "funcMakerContext۰ContextStringГString"},
			excluded: []string{"MyOtherDoFn", "anotherFn", "runtime.RegisterType(reflect.TypeOf((*foo)(nil)).Elem())", "funcMakerFooГString", "funcMakerShimx۰EmitterГString", "funcMakerShimx۰EmitterГFoo"},
		},
		{name: "genSpecificMultiFile2", files: []string{hello1, hello2}, pkg: "hello", ids: []string{"anotherFn"},
			expected: []string{"funcMakerShimx۰EmitterГString", "funcMakerShimx۰EmitterГString"},
			excluded: []string{"MyOtherDoFn", "MyTitle", "runtime.RegisterType(reflect.TypeOf((*foo)(nil)).Elem())", "funcMakerFooГString"},
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
			var b bytes.Buffer
			if err := Generate(&b, test.name+".go", test.pkg, test.ids, fset, fs); err != nil {
				t.Fatal(err)
			}
			s := string(b.Bytes())
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

const hello1 = `
package hello

import (
	"context"
	"strings"
)

func MyTitle(ctx context.Context, v string) string {
	return strings.Title(v)
}

type foo struct{}

func MyOtherDoFn(v foo) string {
	return "constant"
}
`

const hello2 = `
package hello

import (
	"context"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/util/shimx"
)

func anotherFn(v shimx.Emitter) string {
	return v.Name
}

func fooFn(v shimx.Emitter) foo {
	return foo{}
}
`
