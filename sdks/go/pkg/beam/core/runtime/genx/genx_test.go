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

package genx

import (
	"context"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/core/sdf"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestRegisterDoFn(t *testing.T) {
	tDoFn01 := reflect.TypeOf((*DoFn01)(nil)).Elem()
	tDoFn02 := reflect.TypeOf((*DoFn02)(nil)).Elem()
	tCmbFn01 := reflect.TypeOf((*CombineFn01)(nil)).Elem()
	tDoFn03 := reflect.TypeOf((*DoFn03)(nil)).Elem()
	tDoFn04 := reflect.TypeOf((*DoFn04)(nil)).Elem()
	tR := reflect.TypeOf((*R)(nil)).Elem()
	tS := reflect.TypeOf((*S)(nil)).Elem()
	tT := reflect.TypeOf((*T)(nil)).Elem()
	tI := reflect.TypeOf((*I)(nil)).Elem()
	tA := reflect.TypeOf((*A)(nil)).Elem()
	tO := reflect.TypeOf((*O)(nil)).Elem()
	tRt := reflect.TypeOf((*sdf.LockRTracker)(nil)).Elem()

	tests := []struct {
		name   string
		dofn   interface{}
		ok     bool           // is this a valid dofn?
		isFunc bool           // is this a functional dofn?
		types  []reflect.Type // expected types
	}{
		{"predeclared type", reflectx.Int, false, false, nil},
		{"predeclared type", reflectx.String, false, false, nil},
		{"unnamed struct reflect", reflect.TypeOf(struct{ A int }{}), false, false, nil},
		{"declared non struct type", reflect.TypeOf(R(2)), false, false, nil},
		{"named struct reflect", reflect.TypeOf(S{}), false, false, nil},
		{"struct pointer reflect", reflect.TypeOf(&S{}), false, false, nil},
		{"slice reflect", reflect.TypeOf([]S{}), false, false, nil},
		{"array reflect", reflect.TypeOf([3]S{}), false, false, nil},
		{"map reflect", reflect.TypeOf(map[S]S{}), false, false, nil},
		{"anon func", func(S) *S { return nil }, true, true, []reflect.Type{tS}}, // anon so not recommeded.
		{"anon func reflect", reflect.TypeOf(func(S) *S { return nil }), false, false, nil},
		{"func reflect", reflect.TypeOf(NonAnon), false, false, nil},
		{"func", NonAnon, true, true, []reflect.Type{tS}},
		{"DoFn01", DoFn01{}, false, false, nil},
		{"DoFn01 typed nil", (*DoFn01)(nil), false, false, nil},
		{"DoFn01 reflect", tDoFn01, true, false, []reflect.Type{tDoFn01, tR, tS}},
		{"DoFn01 pointer", &DoFn01{}, true, false, []reflect.Type{tDoFn01, tR, tS}},
		{"DoFn01 pointer reflect", reflect.TypeOf(&DoFn01{}), true, false, []reflect.Type{tDoFn01, tR, tS}},
		{"DoFn02 reflect - filtered types", tDoFn02, true, false, []reflect.Type{tDoFn02}},
		{"CombineFn01 reflect - combine methods", tCmbFn01, true, false, []reflect.Type{tCmbFn01, tA, tI, tO}},
		{"DoFn03 reflect - sdf methods", tDoFn03, true, false, []reflect.Type{tDoFn03, tRt, tR}},
		{"DoFn04 reflect - containers", tDoFn04, true, false, []reflect.Type{tDoFn04, tR, tS, tT, tA, tI, tO}},
	}

	opts := []cmp.Option{
		cmpopts.EquateEmpty(),
		cmp.Comparer(func(a, b reflect.Type) bool {
			return toKey(a) == toKey(b)
		}),
		cmpopts.SortSlices(func(a, b reflect.Type) bool {
			if toKey(a) < toKey(b) {
				return true
			}
			return false
		}),
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			f, ts, err := registerDoFn(test.dofn)
			if (err != nil) && test.ok {
				t.Fatalf("registerDoFn(%+v) unexpected error: %v", test.dofn, err)
			}
			if test.isFunc && f == nil {
				t.Errorf("registerDoFn(%+v) didn't pass through function= %v, want non-nil", test.dofn, err)
			}
			if d := cmp.Diff(test.types, ts, opts...); d != "" {
				t.Errorf("return type mismatched registerDoFn(%#v) = %v, want %v; diff\n\t%v", test.dofn, ts, test.types, d)
			}
		})
	}
}

func TestRegisterDoFn_panic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Log("recovered")
		}
	}()
	RegisterDoFn(T{})
	t.Fatal("expected panic")
}

func TestRegisterDoFn_succeed(t *testing.T) {
	tDoFn04 := reflect.TypeOf((*DoFn04)(nil)).Elem()
	tR := reflect.TypeOf((*R)(nil)).Elem()
	tS := reflect.TypeOf((*S)(nil)).Elem()
	tT := reflect.TypeOf((*T)(nil)).Elem()
	tI := reflect.TypeOf((*I)(nil)).Elem()
	tA := reflect.TypeOf((*A)(nil)).Elem()
	tO := reflect.TypeOf((*O)(nil)).Elem()

	validate := func(t *testing.T, want reflect.Type) {
		t.Helper()
		key := toKey(want)
		got, ok := runtime.LookupType(key)
		if !ok {
			t.Errorf("expected type %v to have been registered", key)
			return
		}
		if got != want {
			t.Errorf("expected got %v == want %v", got, want)
		}
	}

	RegisterDoFn(NonAnon)
	validate(t, tS)

	RegisterDoFn(&DoFn04{})
	for _, want := range []reflect.Type{tDoFn04, tR, tS, tT, tA, tI, tO} {
		validate(t, want)
	}
}

func toKey(a reflect.Type) string {
	return a.PkgPath() + "." + a.Name()
}

func NonAnon(S) *S { return nil }

type R int

type S struct {
	A int
}

type T struct {
	B string
}

type A struct {
	C int64
	S float64
}

type I float64
type O float64

type neverAppears struct{}

type DoFn01 struct{}

func (*DoFn01) ProcessElement(s S) R {
	return R(s.A)
}

func (*DoFn01) notALifecycleMethod(neverAppears) *neverAppears {
	return nil
}

type DoFn02 struct{}

func (*DoFn02) ProcessElement(context.Context, string, int, func(*[]byte) bool, func([4]rune)) float64 {
	return 0
}

type CombineFn01 struct{}

func (*CombineFn01) CreateAccumulator() A {
	return A{}
}

func (*CombineFn01) AddInput(a A, i I) A {
	return A{S: a.S + float64(i), C: a.C + 1}
}

func (*CombineFn01) MergeAccumulators(a, b A) A {
	return A{S: a.S + b.S, C: a.C + b.C}
}

func (*CombineFn01) ExtractOutput(a A) O {
	return O(a.S / float64(a.C))
}

type RT struct {
	sdf.RTracker
}

type DoFn03 struct{}

func (*DoFn03) ProcessElement(*sdf.LockRTracker, string, func(float64)) {
}

func (fn *DoFn03) CreateInitialRestriction(s string) R {
	return R(0)
}

func (fn *DoFn03) SplitRestriction(_ string, rest R) []R {
	return []R{}
}

func (fn *DoFn03) RestrictionSize(_ string, rest R) float64 {
	return 0
}
func (fn *DoFn03) CreateTracker(rest R) *sdf.LockRTracker {
	return &sdf.LockRTracker{Rt: RT{}}
}

type DoFn04 struct{}

func (*DoFn04) ProcessElement([4]R, map[S]T, func(*O) bool, func() func(*I) bool, func([]A)) {
}
