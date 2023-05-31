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
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/jsonx"
	"github.com/google/go-cmp/cmp"
)

func TestReflectionRowCoderGeneration(t *testing.T) {
	num := 35
	tests := []struct {
		want any
	}{
		{
			// Top level value check
			want: UserType1{
				A: "cats",
				B: 24,
				C: "pjamas",
			},
		}, {
			// Top level pointer check
			want: &UserType1{
				A: "marmalade",
				B: 24,
				C: "jam",
			},
		}, {
			// Inner pointer check.
			want: UserType2{
				A: "dogs",
				B: &UserType1{
					A: "cats",
					B: 24,
					C: "pjamas",
				},
				C: &num,
			},
		}, {
			// nil pointer check.
			want: UserType2{
				A: "dogs",
				B: nil,
				C: nil,
			},
		}, {
			// nested struct check
			want: UserType3{
				A: UserType1{
					A: "marmalade",
					B: 24,
					C: "jam",
				},
			},
		}, {
			// embedded struct check
			want: UserType4{
				UserType1{
					A: "marmalade",
					B: 24,
					C: "jam",
				},
			},
		}, {
			// embedded struct check2
			want: userType5{
				unexportedUserType: unexportedUserType{
					A: 24,
					B: "marmalade",
				},
				C: 79,
			},
		}, {
			// embedded struct check3
			want: userType6{
				UserType1: &UserType1{
					A: "marmalade",
					B: 24,
				},
				C: 81,
			},
		}, {
			// All zeroes
			want: struct {
				V00 bool
				V01 byte
				V02 uint8
				V03 int16
				V04 uint16
				V05 int32
				V06 uint32
				V07 int64
				V08 uint64
				V09 int
				V10 struct{}
				V11 *struct{}
				V12 [0]int
				V13 [2]int
				V14 []int
				V15 map[string]int
				V16 float32
				V17 float64
				V18 []byte
				V19 [2]*int
				V20 map[*string]*int
			}{},
		}, {
			want: struct {
				V00 bool
				V01 byte
				V02 uint8
				V03 int16
				V04 uint16
				V05 int32
				V06 uint32
				V07 int64
				V08 uint64
				V09 int
				V10 struct{}
				V11 *struct{}
				V12 [0]int
				V13 [2]int
				V14 []int
				V15 map[string]int
				V16 float32
				V17 float64
				V18 []byte
				V19 [2]*int
				V20 map[*string]*int
				V21 []*int
			}{
				V00: true,
				V01: 1,
				V02: 2,
				V03: 3,
				V04: 4,
				V05: 5,
				V06: 6,
				V07: 7,
				V08: 8,
				V09: 9,
				V10: struct{}{},
				V11: &struct{}{},
				V12: [0]int{},
				V13: [2]int{72, 908},
				V14: []int{12, 9326, 641346, 6},
				V15: map[string]int{"pants": 42},
				V16: 3.14169,
				V17: 2.6e100,
				V18: []byte{21, 17, 65, 255, 0, 16},
				V19: [2]*int{nil, &num},
				V20: map[*string]*int{
					nil: nil,
				},
				V21: []*int{nil, &num, nil},
			},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%+v", test.want), func(t *testing.T) {
			rt := reflect.TypeOf(test.want)
			enc, err := RowEncoderForStruct(rt)
			if err != nil {
				t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
			}
			var buf bytes.Buffer
			if err := enc(test.want, &buf); err != nil {
				t.Fatalf("enc(%v) = err, want nil error", err)
			}
			dec, err := RowDecoderForStruct(rt)
			if err != nil {
				t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
			}
			b := buf.Bytes()
			r := bytes.NewBuffer(b)
			got, err := dec(r)
			if err != nil {
				t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
			}
			if d := cmp.Diff(test.want, got, cmp.AllowUnexported(userType5{}, unexportedUserType{})); d != "" {
				t.Fatalf("dec(enc(%v)) = %v\ndiff (-want, +got): %v", test.want, got, d)
			}
		})
	}
}

func TestReflectionRowCoderGeneration_UnexportedEmbed(t *testing.T) {
	input := userType5{
		unexportedUserType: unexportedUserType{
			A: 24,
			B: "marmalade",
			c: 10,
		},
		C: 79,
	}
	rt := reflect.TypeOf(input)
	enc, err := RowEncoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowEncoderForStruct(%v) = %v, want nil error", rt, err)
	}
	var buf bytes.Buffer
	if err := enc(input, &buf); err != nil {
		t.Fatalf("enc(%v) = err, want nil error", err)
	}
	dec, err := RowDecoderForStruct(rt)
	if err != nil {
		t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
	}
	b := buf.Bytes()
	r := bytes.NewBuffer(b)
	got, err := dec(r)
	if err != nil {
		t.Fatalf("RowDecoderForStruct(%v) = %v, want nil error", rt, err)
	}
	// Arrange expected output, should not have a value for the unexported field.
	want := input
	want.unexportedUserType.c = 0
	if d := cmp.Diff(want, got, cmp.AllowUnexported(userType5{}, unexportedUserType{})); d != "" {
		t.Fatalf("dec(enc(%v)) = %v\ndiff (-want, +got): %v", want, got, d)
	}
}

type UserType1 struct {
	A string
	B int
	C string
}

type UserType2 struct {
	A string
	B *UserType1
	C *int
}

type UserType3 struct {
	A UserType1
}

// Embedding check.
type UserType4 struct {
	UserType1
}

type unexportedUserType struct {
	A int
	B string
	c int32
}

// Embedding check with unexported type.
type userType5 struct {
	unexportedUserType
	C int32
}

// Embedding check with a pointer Exported type
type userType6 struct {
	*UserType1
	C int32
}

// Note: pointers to unexported types can't be handled by
// this package. See https://golang.org/issue/21357.

func ut1Enc(val any, w io.Writer) error {
	if err := WriteSimpleRowHeader(3, w); err != nil {
		return err
	}
	elm := val.(UserType1)
	if err := EncodeStringUTF8(elm.A, w); err != nil {
		return err
	}
	if err := EncodeVarInt(int64(elm.B), w); err != nil {
		return err
	}
	if err := EncodeStringUTF8(elm.C, w); err != nil {
		return err
	}
	return nil
}

func ut1Dec(r io.Reader) (any, error) {
	if err := ReadSimpleRowHeader(3, r); err != nil {
		return nil, err
	}
	a, err := DecodeStringUTF8(r)
	if err != nil {
		return nil, fmt.Errorf("decoding string field A: %v", err)
	}
	b, err := DecodeVarInt(r)
	if err != nil {
		return nil, fmt.Errorf("decoding int field B: %v", err)
	}
	c, err := DecodeStringUTF8(r)
	if err != nil {
		return nil, fmt.Errorf("decoding string field C: %v, %v", c, err)
	}
	return UserType1{
		A: a,
		B: int(b),
		C: c,
	}, nil
}

func TestRowCoder_CustomCoder(t *testing.T) {
	customRT := reflect.TypeOf(UserType1{})
	customEnc := ut1Enc
	customDec := ut1Dec

	num := 35
	tests := []struct {
		want any
	}{
		{
			// Top level value check
			want: UserType1{
				A: "cats",
				B: 24,
				C: "pjamas",
			},
		}, {
			// Top level pointer check
			want: &UserType1{
				A: "marmalade",
				B: 24,
				C: "jam",
			},
		}, {
			// Inner pointer check.
			want: UserType2{
				A: "dogs",
				B: &UserType1{
					A: "cats",
					B: 24,
					C: "pjamas",
				},
				C: &num,
			},
		}, {
			// nil pointer check.
			want: UserType2{
				A: "dogs",
				B: nil,
				C: nil,
			},
		}, {
			// nested struct check
			want: UserType3{
				A: UserType1{
					A: "marmalade",
					B: 24,
					C: "jam",
				},
			},
		}, {
			// embedded struct check
			want: UserType4{
				UserType1{
					A: "marmalade",
					B: 24,
					C: "jam",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("%+v", test.want), func(t *testing.T) {
			rt := reflect.TypeOf(test.want)
			var encB RowEncoderBuilder
			encB.Register(customRT, func(reflect.Type) (func(any, io.Writer) error, error) { return customEnc, nil })
			enc, err := encB.Build(rt)
			if err != nil {
				t.Fatalf("RowEncoderBuilder.Build(%v) = %v, want nil error", rt, err)
			}
			var decB RowDecoderBuilder
			decB.Register(customRT, func(reflect.Type) (func(io.Reader) (any, error), error) { return customDec, nil })
			dec, err := decB.Build(rt)
			if err != nil {
				t.Fatalf("RowDecoderBuilder.Build(%v) = %v, want nil error", rt, err)
			}
			var buf bytes.Buffer
			if err := enc(test.want, &buf); err != nil {
				t.Fatalf("enc(%v) = err, want nil error", err)
			}
			_, err = dec(&buf)
			if err != nil {
				t.Fatalf("BuildDecoder(%v) = %v, want nil error", rt, err)
			}
		})
	}
}

func BenchmarkRowCoder_RoundTrip(b *testing.B) {
	ut1Enc := func(val any, w io.Writer) error {
		elm := val.(UserType1)
		// We have 3 fields we use.
		if err := EncodeVarInt(3, w); err != nil {
			return err
		}
		// Never nils, so we write the 0 byte header.
		if err := EncodeVarInt(0, w); err != nil {
			return err
		}
		if err := EncodeStringUTF8(elm.A, w); err != nil {
			return err
		}
		if err := EncodeVarInt(int64(elm.B), w); err != nil {
			return err
		}
		if err := EncodeStringUTF8(elm.C, w); err != nil {
			return err
		}
		return nil
	}
	ut1Dec := func(r io.Reader) (any, error) {
		// We have 3 fields we use.
		n, err := DecodeVarInt(r)
		if err != nil {
			return nil, fmt.Errorf("decoding header fieldcount: %v, %v", n, err)
		}
		if n != 3 {
			return nil, fmt.Errorf("decoding header field count, got %v, want %v", n, 3)
		}
		// Never nils, so we read the 0 byte header.
		n, err = DecodeVarInt(r)
		if err != nil {
			return nil, fmt.Errorf("decoding header nils: %v, %v", n, err)
		}
		if n != 0 {
			return nil, fmt.Errorf("decoding header nils count, got %v, want %v", n, 0)
		}
		a, err := DecodeStringUTF8(r)
		if err != nil {
			return nil, fmt.Errorf("decoding string field A: %v", err)
		}
		b, err := DecodeVarInt(r)
		if err != nil {
			return nil, fmt.Errorf("decoding int field B: %v", err)
		}
		c, err := DecodeStringUTF8(r)
		if err != nil {
			return nil, fmt.Errorf("decoding string field C: %v, %v", c, err)
		}
		return UserType1{
			A: a,
			B: int(b),
			C: c,
		}, nil
	}

	num := 35
	benches := []struct {
		want      any
		customRT  reflect.Type
		customEnc func(any, io.Writer) error
		customDec func(io.Reader) (any, error)
	}{
		{
			// Top level value check
			want: UserType1{
				A: "cats",
				B: 24,
				C: "pjamas",
			},
			customRT:  reflect.TypeOf(UserType1{}),
			customEnc: ut1Enc,
			customDec: ut1Dec,
		}, {
			// Top level pointer check
			want: &UserType1{
				A: "marmalade",
				B: 24,
				C: "jam",
			},
			customRT:  reflect.TypeOf(UserType1{}),
			customEnc: ut1Enc,
			customDec: ut1Dec,
		}, {
			// Inner pointer check.
			want: UserType2{
				A: "dogs",
				B: &UserType1{
					A: "cats",
					B: 24,
					C: "pjamas",
				},
				C: &num,
			},
			customRT:  reflect.TypeOf(UserType1{}),
			customEnc: ut1Enc,
			customDec: ut1Dec,
		}, {
			// nil pointer check.
			want: UserType2{
				A: "dogs",
				B: nil,
				C: nil,
			},
			customRT:  reflect.TypeOf(UserType1{}),
			customEnc: ut1Enc,
			customDec: ut1Dec,
		}, {
			// nested struct check
			want: UserType3{
				A: UserType1{
					A: "marmalade",
					B: 24,
					C: "jam",
				},
			},
			customRT:  reflect.TypeOf(UserType1{}),
			customEnc: ut1Enc,
			customDec: ut1Dec,
		}, {
			// embedded struct check
			want: UserType4{
				UserType1{
					A: "marmalade",
					B: 24,
					C: "jam",
				},
			},
			customRT:  reflect.TypeOf(UserType1{}),
			customEnc: ut1Enc,
			customDec: ut1Dec,
		}, {
			// embedded struct check2
			want: userType5{
				unexportedUserType: unexportedUserType{
					B: "marmalade",
					A: 24,
				},
				C: 79,
			},
		}, {
			// embedded struct check3
			want: userType6{
				UserType1: &UserType1{
					A: "marmalade",
					B: 24,
				},
				C: 81,
			},
		},
	}
	for _, bench := range benches {
		rt := reflect.TypeOf(bench.want)
		{
			enc, err := RowEncoderForStruct(rt)
			if err != nil {
				b.Fatalf("BuildEncoder(%v) = %v, want nil error", rt, err)
			}
			dec, err := RowDecoderForStruct(rt)
			if err != nil {
				b.Fatalf("BuildDecoder(%v) = %v, want nil error", rt, err)
			}
			var buf bytes.Buffer
			b.Run(fmt.Sprintf("SCHEMA %+v", bench.want), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := enc(bench.want, &buf); err != nil {
						b.Fatalf("enc(%v) = err, want nil error", err)
					}
					_, err := dec(&buf)
					if err != nil {
						b.Fatalf("BuildDecoder(%v) = %v, want nil error", rt, err)
					}
				}
			})
		}
		if bench.customEnc != nil && bench.customDec != nil && rt == bench.customRT {
			var buf bytes.Buffer
			b.Run(fmt.Sprintf("CUSTOM %+v", bench.want), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := bench.customEnc(bench.want, &buf); err != nil {
						b.Fatalf("enc(%v) = err, want nil error", err)
					}
					_, err := bench.customDec(&buf)
					if err != nil {
						b.Fatalf("BuildDecoder(%v) = %v, want nil error", rt, err)
					}
				}
			})
		}
		if bench.customEnc != nil && bench.customDec != nil {
			var encB RowEncoderBuilder
			encB.Register(bench.customRT, func(reflect.Type) (func(any, io.Writer) error, error) { return bench.customEnc, nil })
			enc, err := encB.Build(rt)
			if err != nil {
				b.Fatalf("RowEncoderBuilder.Build(%v) = %v, want nil error", rt, err)
			}
			var decB RowDecoderBuilder
			decB.Register(bench.customRT, func(reflect.Type) (func(io.Reader) (any, error), error) { return bench.customDec, nil })
			dec, err := decB.Build(rt)
			if err != nil {
				b.Fatalf("RowDecoderBuilder.Build(%v) = %v, want nil error", rt, err)
			}
			var buf bytes.Buffer
			b.Run(fmt.Sprintf("REGISTERED %+v", bench.want), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					if err := enc(bench.want, &buf); err != nil {
						b.Fatalf("enc(%v) = err, want nil error", err)
					}
					_, err := dec(&buf)
					if err != nil {
						b.Fatalf("BuildDecoder(%v) = %v, want nil error", rt, err)
					}
				}
			})
		}
		{
			b.Run(fmt.Sprintf("JSON %+v", bench.want), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					data, err := jsonx.Marshal(bench.want)
					if err != nil {
						b.Fatalf("jsonx.Marshal(%v) = err, want nil error", err)
					}
					val := reflect.New(rt)
					if err := jsonx.Unmarshal(val.Interface(), data); err != nil {
						b.Fatalf("jsonx.Unmarshal(%v) = %v, want nil error; type: %v", rt, err, val.Type())
					}
				}
			})
		}
	}
}

type testInterface interface {
	TestEncode() ([]byte, error)
	TestDecode(b []byte) error
}

var (
	testInterfaceType = reflect.TypeOf((*testInterface)(nil)).Elem()
	testStorageType   = reflect.TypeOf((*struct{ TestData []byte })(nil)).Elem()
	testParDoType     = reflect.TypeOf((*testParDo)(nil))
)

type testProvider struct{}

func (p *testProvider) FromLogicalType(rt reflect.Type) (reflect.Type, error) {
	if !rt.Implements(testInterfaceType) {
		return nil, fmt.Errorf("%s does not implement testInterface", rt)
	}
	return testStorageType, nil
}

func (p *testProvider) BuildEncoder(rt reflect.Type) (func(any, io.Writer) error, error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}

	return func(iface any, w io.Writer) error {
		v := iface.(testInterface)
		data, err := v.TestEncode()
		if err != nil {
			return err
		}
		if err := WriteSimpleRowHeader(1, w); err != nil {
			return err
		}
		if err := EncodeBytes(data, w); err != nil {
			return err
		}
		return nil
	}, nil
}

func (p *testProvider) BuildDecoder(rt reflect.Type) (func(io.Reader) (any, error), error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
		return func(r io.Reader) (any, error) {
			if err := ReadSimpleRowHeader(1, r); err != nil {
				return nil, err
			}
			data, err := DecodeBytes(r)
			if err != nil {
				return nil, err
			}
			v, ok := reflect.New(rt).Interface().(testInterface)
			if !ok {
				return nil, fmt.Errorf("%s is not %s", reflect.PtrTo(rt), testInterfaceType)
			}
			if err := v.TestDecode(data); err != nil {
				return nil, err
			}
			return v, nil
		}, nil
	}
	return func(r io.Reader) (any, error) {
		if err := ReadSimpleRowHeader(1, r); err != nil {
			return nil, err
		}
		data, err := DecodeBytes(r)
		if err != nil {
			return nil, err
		}
		v, ok := reflect.New(rt).Elem().Interface().(testInterface)
		if !ok {
			return nil, fmt.Errorf("%s is not %s", rt, testInterfaceType)
		}
		if err := v.TestDecode(data); err != nil {
			return nil, err
		}
		return v, nil
	}, nil
}

type testStruct struct {
	A int64

	b int64
}

func (s *testStruct) TestEncode() ([]byte, error) {
	var buf bytes.Buffer
	if err := EncodeVarInt(s.A, &buf); err != nil {
		return nil, err
	}
	if err := EncodeVarInt(s.b, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *testStruct) TestDecode(b []byte) error {
	buf := bytes.NewReader(b)
	var err error
	s.A, err = DecodeVarInt(buf)
	if err != nil {
		return err
	}
	s.b, err = DecodeVarInt(buf)
	if err != nil {
		return err
	}
	return nil
}

var _ testInterface = &testStruct{}

type testParDo struct {
	Struct         testStruct
	StructPtr      *testStruct
	StructSlice    []testStruct
	StructPtrSlice []*testStruct
	StructMap      map[int64]testStruct
	StructPtrMap   map[int64]*testStruct
}

func TestSchemaProviderInterface(t *testing.T) {
	p := testProvider{}
	encb := &RowEncoderBuilder{}
	encb.Register(testInterfaceType, p.BuildEncoder)
	enc, err := encb.Build(testParDoType)
	if err != nil {
		t.Fatalf("RowEncoderBuilder.Build(%v): %v", testParDoType, err)
	}
	decb := &RowDecoderBuilder{}
	decb.Register(testInterfaceType, p.BuildDecoder)
	dec, err := decb.Build(testParDoType)
	if err != nil {
		t.Fatalf("RowDecoderBuilder.Build(%v): %v", testParDoType, err)
	}
	want := &testParDo{
		Struct: testStruct{
			A: 1,
			b: 2,
		},
		StructPtr: &testStruct{
			A: 3,
			b: 4,
		},
		StructSlice: []testStruct{
			{
				A: 5,
				b: 6,
			},
			{
				A: 7,
				b: 8,
			},
			{
				A: 9,
				b: 10,
			},
		},
		StructPtrSlice: []*testStruct{
			{
				A: 11,
				b: 12,
			},
			{
				A: 13,
				b: 14,
			},
			{
				A: 15,
				b: 16,
			},
		},
		StructMap: map[int64]testStruct{
			0: {
				A: 17,
				b: 18,
			},
			1: {
				A: 19,
				b: 20,
			},
			2: {
				A: 21,
				b: 22,
			},
		},
		StructPtrMap: map[int64]*testStruct{
			0: {
				A: 23,
				b: 24,
			},
			1: {
				A: 25,
				b: 26,
			},
			2: {
				A: 27,
				b: 28,
			},
		},
	}
	var buf bytes.Buffer
	if err := enc(want, &buf); err != nil {
		t.Fatalf("Encode(%v): %v", want, err)
	}
	got, err := dec(&buf)
	if err != nil {
		t.Fatalf("Decode(%v): %v", buf.Bytes(), err)
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(testStruct{})); diff != "" {
		t.Errorf("Decode(Encode(%v)): %v", want, diff)
	}
}

func TestRowHeader_TrailingZeroBytes(t *testing.T) {
	// https://github.com/apache/beam/issues/21232: The row header should elide trailing 0 bytes.
	// But be tolerant of trailing 0 bytes.

	const count = 255
	// For each bit, lets ensure we can check and lookup all the nils.
	for i := -1; i < count; i++ {
		t.Run(fmt.Sprintf("%d is nil", i), func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteRowHeader(count, func(f int) bool { return f == i }, &buf); err != nil {
				t.Fatalf("WriteRowHeader failed: %v", err)
			}
			// 3+(i/8+1) is the expected byte count for the header when trailing nils are elided.
			// 2 bytes for the varint encoded `count`
			// 1 byte for the varint encoded bitset length
			// i/8+1 nil filed index to get the number of bytes in the bitset.
			const fcl = 3
			if got, want := len(buf.Bytes()), fcl+(i/8+1); i != -1 && got != want {
				t.Errorf("len(header: %+v) = %v, want %v", buf.Bytes(), got, want)
			}
			// In the no nils case, header should only be fcl bytes long.
			if got, want := len(buf.Bytes()), fcl; i == -1 && got != want {
				t.Errorf("len(header: %+v) = %v, want %v", buf.Bytes(), got, want)
			}
			n, nils, err := ReadRowHeader(&buf)
			if err != nil {
				t.Fatalf("ReadRowHeader failed: %v", err)
			}
			if got, want := n, count; got != want {
				t.Fatalf("ReadRowHeader returned %v fields, but want %v", got, want)
			}
			// Look up all fields, and ensure they actually match.
			// Only a single nil field is being set at most, the matching iteration index.
			for f := 0; f < count; f++ {
				if got, want := IsFieldNil(nils, f), i == f; got != want {
					t.Errorf("IsFieldNil(%v, %v) = %v but want %v", nils, f, got, want)
				}
			}
		})
	}

}
