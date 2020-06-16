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

package coder

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

type MyType struct{}

func (MyType) A()  {}
func (*MyType) B() {}

type a interface {
	A()
}
type b interface {
	A()
}

var (
	mPtrT = reflect.TypeOf((*MyType)(nil))
	mT    = mPtrT.Elem()
)

func TestValidEncoderForms(t *testing.T) {
	tests := []struct {
		t   reflect.Type
		enc interface{}
	}{
		{t: mT, enc: func(MyType) []byte { return nil }},
		{t: mT, enc: func(MyType) ([]byte, error) { return nil, nil }},
		{t: mT, enc: func(reflect.Type, MyType) []byte { return nil }},
		{t: mT, enc: func(reflect.Type, MyType) ([]byte, error) { return nil, nil }},
		// Using a universal type as encode type.
		{t: mT, enc: func(typex.T) []byte { return nil }},
		{t: mT, enc: func(typex.T) ([]byte, error) { return nil, nil }},
		{t: mT, enc: func(reflect.Type, typex.T) []byte { return nil }},
		{t: mT, enc: func(reflect.Type, typex.T) ([]byte, error) { return nil, nil }},
		// Using a satisfied interface type as encode type.
		{t: mT, enc: func(a) []byte { return nil }},
		{t: mT, enc: func(a) ([]byte, error) { return nil, nil }},
		{t: mT, enc: func(reflect.Type, a) []byte { return nil }},
		{t: mT, enc: func(reflect.Type, a) ([]byte, error) { return nil, nil }},

		{t: mPtrT, enc: func(a) []byte { return nil }},
		{t: mPtrT, enc: func(a) ([]byte, error) { return nil, nil }},
		{t: mPtrT, enc: func(reflect.Type, a) []byte { return nil }},
		{t: mPtrT, enc: func(reflect.Type, a) ([]byte, error) { return nil, nil }},

		{t: mPtrT, enc: func(b) []byte { return nil }},
		{t: mPtrT, enc: func(b) ([]byte, error) { return nil, nil }},
		{t: mPtrT, enc: func(reflect.Type, b) []byte { return nil }},
		{t: mPtrT, enc: func(reflect.Type, b) ([]byte, error) { return nil, nil }},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%T", test.enc), func(t *testing.T) {
			if err := validateEncoder(mT, test.enc); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestValidDecoderForms(t *testing.T) {
	tests := []struct {
		t   reflect.Type
		dec interface{}
	}{
		{t: mT, dec: func([]byte) MyType { return MyType{} }},
		{t: mT, dec: func([]byte) (MyType, error) { return MyType{}, nil }},
		{t: mT, dec: func(reflect.Type, []byte) MyType { return MyType{} }},
		{t: mT, dec: func(reflect.Type, []byte) (MyType, error) { return MyType{}, nil }},

		// Using a universal type as decode type.
		{t: mT, dec: func([]byte) typex.T { return MyType{} }},
		{t: mT, dec: func([]byte) (typex.T, error) { return MyType{}, nil }},
		{t: mT, dec: func(reflect.Type, []byte) typex.T { return MyType{} }},
		{t: mT, dec: func(reflect.Type, []byte) (typex.T, error) { return MyType{}, nil }},

		// Using satisfied interfaces as decode type.
		{t: mT, dec: func([]byte) a { return nil }},
		{t: mT, dec: func([]byte) (a, error) { return nil, nil }},
		{t: mT, dec: func(reflect.Type, []byte) a { return nil }},
		{t: mT, dec: func(reflect.Type, []byte) (a, error) { return nil, nil }},

		{t: mPtrT, dec: func([]byte) a { return nil }},
		{t: mPtrT, dec: func([]byte) (a, error) { return nil, nil }},
		{t: mPtrT, dec: func(reflect.Type, []byte) a { return nil }},
		{t: mPtrT, dec: func(reflect.Type, []byte) (a, error) { return nil, nil }},

		{t: mPtrT, dec: func([]byte) b { return nil }},
		{t: mPtrT, dec: func([]byte) (b, error) { return nil, nil }},
		{t: mPtrT, dec: func(reflect.Type, []byte) b { return nil }},
		{t: mPtrT, dec: func(reflect.Type, []byte) (b, error) { return nil, nil }},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%T", test.dec), func(t *testing.T) {
			if err := validateDecoder(test.t, test.dec); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCoder_String(t *testing.T) {
	ints := NewVarInt()
	bytes := NewBytes()
	bools := NewBool()
	global := NewGlobalWindow()
	interval := NewIntervalWindow()
	cusString, err := NewCustomCoder("customString", reflectx.String, func(string) []byte { return nil }, func([]byte) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	custom := &Coder{Kind: Custom, Custom: cusString, T: typex.New(reflectx.String)}

	tests := []struct {
		want string
		c    *Coder
	}{{
		want: "$",
		c:    nil,
	}, {
		want: "bytes",
		c:    bytes,
	}, {
		want: "bool",
		c:    bools,
	}, {
		want: "varint",
		c:    ints,
	}, {
		want: "string[customString]",
		c:    custom,
	}, {
		want: "W<varint>!GWC",
		c:    NewW(ints, global),
	}, {
		want: "W<bytes>!IWC",
		c:    NewW(bytes, interval),
	}, {
		want: "KV<bytes,varint>",
		c:    NewKV([]*Coder{bytes, ints}),
	}, {
		want: "CoGBK<bytes,varint,bytes>",
		c:    NewCoGBK([]*Coder{bytes, ints, bytes}),
	}, {
		want: "W<KV<bytes,varint>>!IWC",
		c:    NewW(NewKV([]*Coder{bytes, ints}), interval),
	}, {
		want: "CoGBK<bytes,varint,string[customString]>",
		c:    NewCoGBK([]*Coder{bytes, ints, custom}),
	},
	}
	for _, test := range tests {
		test := test
		t.Run(test.want, func(t *testing.T) {
			got := test.c.String()
			if test.want != got {
				t.Fatalf("c.String() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestCoder_Equals(t *testing.T) {
	ints := NewVarInt()
	bytes := NewBytes()
	global := NewGlobalWindow()
	interval := NewIntervalWindow()

	cusStrEnc := func(string) []byte { return nil }
	cusStrDec := func([]byte) string { return "" }

	cusString1, err := NewCustomCoder("cus1", reflectx.String, cusStrEnc, cusStrDec)
	if err != nil {
		t.Fatal(err)
	}
	custom1 := &Coder{Kind: Custom, Custom: cusString1, T: typex.New(reflectx.String)}
	cusString2, err := NewCustomCoder("cus2", reflectx.String, cusStrEnc, cusStrDec)
	if err != nil {
		t.Fatal(err)
	}
	custom2 := &Coder{Kind: Custom, Custom: cusString2, T: typex.New(reflectx.String)}
	cusSameAs1, err := NewCustomCoder("cus1", reflectx.String, cusStrEnc, cusStrDec)
	if err != nil {
		t.Fatal(err)
	}
	customSame := &Coder{Kind: Custom, Custom: cusSameAs1, T: typex.New(reflectx.String)}

	tests := []struct {
		want bool
		a, b *Coder
	}{{
		want: true,
		a:    bytes,
		b:    bytes,
	}, {
		want: true,
		a:    ints,
		b:    ints,
	}, {
		want: false,
		a:    ints,
		b:    bytes,
	}, {
		want: true,
		a:    custom1,
		b:    custom1,
	}, {
		want: false,
		a:    custom1,
		b:    custom2,
	}, {
		want: true,
		a:    custom1,
		b:    customSame,
	}, {
		want: true,
		a:    NewW(ints, global),
		b:    NewW(ints, global),
	}, {
		want: false,
		a:    NewW(ints, global),
		b:    NewW(ints, interval),
	}, {
		want: false,
		a:    NewW(bytes, global),
		b:    NewW(ints, global),
	}, {
		want: true,
		a:    NewW(custom1, interval),
		b:    NewW(customSame, interval),
	}, {
		want: true,
		a:    NewKV([]*Coder{custom1, ints}),
		b:    NewKV([]*Coder{customSame, ints}),
	}, {
		want: true,
		a:    NewCoGBK([]*Coder{custom1, ints, customSame}),
		b:    NewCoGBK([]*Coder{customSame, ints, custom1}),
	}, {
		want: false,
		a:    NewCoGBK([]*Coder{custom1, ints, customSame}),
		b:    NewCoGBK([]*Coder{customSame, ints, custom2}),
	},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v_vs_%v", test.a, test.b), func(t *testing.T) {
			if got := test.a.Equals(test.b); test.want != got {
				t.Errorf("A vs B: %v.Equals(%v) = %v, want %v", test.a, test.b, got, test.want)
			}
			if got := test.b.Equals(test.a); test.want != got {
				t.Errorf("B vs A: %v.Equals(%v) = %v, want %v", test.b, test.a, got, test.want)
			}
		})
	}
}

func TestCustomCoder_Equals(t *testing.T) {
	cusStrEnc := func(string) []byte { return nil }
	cusStrDec := func([]byte) string { return "" }

	cusStrEnc2 := func(string) []byte { return nil }
	cusStrDec2 := func([]byte) string { return "" }

	newCC := func(t *testing.T, name string, et reflect.Type, enc, dec interface{}) *CustomCoder {
		t.Helper()
		cc, err := NewCustomCoder(name, et, enc, dec)
		if err != nil {
			t.Fatalf("couldn't get custom coder for %v: %v", et, err)
		}
		return cc
	}

	cusBase := newCC(t, "cus1", reflectx.String, cusStrEnc, cusStrDec)
	cusSame := newCC(t, "cus1", reflectx.String, cusStrEnc, cusStrDec)
	cusDiffName := newCC(t, "cus2", reflectx.String, cusStrEnc, cusStrDec)
	cusDiffType := newCC(t, "cus1", reflectx.Int, func(int) []byte { return nil }, func([]byte) int { return 0 })
	cusDiffEnc := newCC(t, "cus1", reflectx.String, cusStrEnc2, cusStrDec)
	cusDiffDec := newCC(t, "cus1", reflectx.String, cusStrEnc, cusStrDec2)

	tests := []struct {
		name string
		want bool
		a, b *CustomCoder
	}{{
		name: "nils",
		want: true,
		a:    nil,
		b:    nil,
	}, {
		name: "baseVsNil",
		want: false,
		a:    cusBase,
		b:    nil,
	}, {
		name: "sameInstance",
		want: true,
		a:    cusBase,
		b:    cusBase,
	}, {
		name: "identical",
		want: true,
		a:    cusBase,
		b:    cusSame,
	}, {
		name: "diffType",
		want: false,
		a:    cusBase,
		b:    cusDiffType,
	}, {
		name: "diffEnc",
		want: false,
		a:    cusBase,
		b:    cusDiffEnc,
	}, {
		name: "diffDec",
		want: false,
		a:    cusBase,
		b:    cusDiffDec,
	}, {
		name: "diffName",
		want: false,
		a:    cusBase,
		b:    cusDiffName,
	},
	}
	for _, test := range tests {
		test := test
		t.Run(fmt.Sprintf("%v_vs_%v", test.a, test.b), func(t *testing.T) {
			if got := test.a.Equals(test.b); test.want != got {
				t.Errorf("A vs B: %v.Equals(%v) = %v, want %v", test.a, test.b, got, test.want)
			}
			if got := test.b.Equals(test.a); test.want != got {
				t.Fatalf("B vs A: %v.Equals(%v) = %v, want %v", test.b, test.a, got, test.want)
			}
		})
	}
}

func TestSkipW(t *testing.T) {
	want := NewBytes()
	t.Run("unwindowed", func(t *testing.T) {
		if got := SkipW(want); !want.Equals(got) {
			t.Fatalf("SkipW(%v) = %v, want %v", want, got, want)
		}
	})
	t.Run("windowed", func(t *testing.T) {
		in := NewW(want, NewGlobalWindow())
		if got := SkipW(in); !want.Equals(got) {
			t.Fatalf("SkipW(%v) = %v, want %v", in, got, want)
		}
	})
}

func TestNewW(t *testing.T) {
	global := NewGlobalWindow()
	bytes := NewBytes()

	tests := []struct {
		name        string
		c           *Coder
		w           *WindowCoder
		shouldpanic bool
		want        *Coder
	}{{
		name: "nil_nil",
		c:    nil, w: nil, shouldpanic: true,
	}, {
		name: "nil_global",
		c:    nil, w: global, shouldpanic: true,
	}, {
		name: "bytes_nil",
		c:    bytes, w: nil, shouldpanic: true,
	}, {
		name: "bytes_global",
		c:    bytes, w: global,
		want: &Coder{Kind: WindowedValue, T: typex.NewW(bytes.T), Window: global, Components: []*Coder{bytes}},
	},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if test.shouldpanic {
				defer func() {
					if p := recover(); p != nil {
						t.Log(p)
						return
					}
					t.Fatalf("NewW(%v, %v): want panic", test.c, test.w)
				}()
			}
			got := NewW(test.c, test.w)
			if !IsW(got) {
				t.Errorf("IsW(%v) = false, want true", got)
			}
			if test.want != nil && !test.want.Equals(got) {
				t.Fatalf("NewW(%v, %v) = %v, want %v", test.c, test.w, got, test.want)
			}
		})
	}
}

func TestNewKV(t *testing.T) {
	bytes := NewBytes()
	ints := NewVarInt()

	tests := []struct {
		name        string
		cs          []*Coder
		shouldpanic bool
		want        *Coder
	}{{
		name:        "nil",
		cs:          nil,
		shouldpanic: true,
	}, {
		name:        "empty",
		cs:          []*Coder{},
		shouldpanic: true,
	}, {
		name:        "bytes",
		cs:          []*Coder{bytes},
		shouldpanic: true,
	}, {
		name:        "bytes_nil",
		cs:          []*Coder{bytes, nil},
		shouldpanic: true,
	}, {
		name:        "nil_ints",
		cs:          []*Coder{nil, ints},
		shouldpanic: true,
	}, {
		name: "bytes_ints",
		cs:   []*Coder{bytes, ints},
		want: &Coder{Kind: KV, T: typex.NewKV(bytes.T, ints.T), Components: []*Coder{bytes, ints}},
	}, {
		name: "ints_bytes",
		cs:   []*Coder{ints, bytes},
		want: &Coder{Kind: KV, T: typex.NewKV(ints.T, bytes.T), Components: []*Coder{ints, bytes}},
	}, {
		name:        "ints_bytes_bytes",
		cs:          []*Coder{ints, bytes, bytes},
		shouldpanic: true,
	}, {
		name:        "ints_nil_bytes",
		cs:          []*Coder{ints, nil, bytes},
		shouldpanic: true,
	},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if test.shouldpanic {
				defer func() {
					if p := recover(); p != nil {
						t.Log(p)
						return
					}
					t.Fatalf("NewKV(%v): want panic", test.cs)
				}()
			}
			got := NewKV(test.cs)
			if !IsKV(got) {
				t.Errorf("IsKV(%v) = false, want true", got)
			}
			if test.want != nil && !test.want.Equals(got) {
				t.Fatalf("NewKV(%v) = %v, want %v", test.cs, got, test.want)
			}
		})
	}
}

func TestNewCoGBK(t *testing.T) {
	bytes := NewBytes()
	ints := NewVarInt()

	tests := []struct {
		name        string
		cs          []*Coder
		shouldpanic bool
		want        *Coder
	}{{
		name:        "nil",
		cs:          nil,
		shouldpanic: true,
	}, {
		name:        "empty",
		cs:          []*Coder{},
		shouldpanic: true,
	}, {
		name:        "bytes",
		cs:          []*Coder{bytes},
		shouldpanic: true,
	}, {
		name: "bytes_ints",
		cs:   []*Coder{bytes, ints},
		want: &Coder{Kind: CoGBK, T: typex.NewCoGBK(bytes.T, ints.T), Components: []*Coder{bytes, ints}},
	}, {
		name: "ints_bytes",
		cs:   []*Coder{ints, bytes},
		want: &Coder{Kind: CoGBK, T: typex.NewCoGBK(ints.T, bytes.T), Components: []*Coder{ints, bytes}},
	}, {
		name:        "ints_nil",
		cs:          []*Coder{ints, nil},
		shouldpanic: true,
	}, {
		name:        "nil_ints",
		cs:          []*Coder{nil, ints},
		shouldpanic: true,
	}, {
		name: "ints_bytes_bytes_ints_bytes,",
		cs:   []*Coder{ints, bytes, bytes, ints, bytes},
		want: &Coder{Kind: CoGBK, T: typex.NewCoGBK(ints.T, bytes.T, bytes.T, ints.T, bytes.T), Components: []*Coder{ints, bytes, bytes, ints, bytes}},
	}, {
		name:        "ints_bytes_bytes_ints_nil_bytes,",
		cs:          []*Coder{ints, bytes, bytes, ints, nil, bytes},
		shouldpanic: true,
	},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			if test.shouldpanic {
				defer func() {
					if p := recover(); p != nil {
						t.Log(p)
						return
					}
					t.Fatalf("NewCoGBK(%v): want panic", test.cs)
				}()
			}
			got := NewCoGBK(test.cs)
			if !IsCoGBK(got) {
				t.Errorf("IsCoGBK(%v) = false, want true", got)
			}
			if test.want != nil && !test.want.Equals(got) {
				t.Fatalf("NewCoGBK(%v) = %v, want %v", test.cs, got, test.want)
			}
		})
	}
}
