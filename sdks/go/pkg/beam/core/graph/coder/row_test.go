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

	"github.com/apache/beam/sdks/go/pkg/beam/core/util/jsonx"
	"github.com/google/go-cmp/cmp"
)

func TestReflectionRowCoderGeneration(t *testing.T) {
	num := 35
	tests := []struct {
		want interface{}
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

func ut1Enc(val interface{}, w io.Writer) error {
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

func ut1Dec(r io.Reader) (interface{}, error) {
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
		want interface{}
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
			encB.Register(customRT, func(reflect.Type) (func(interface{}, io.Writer) error, error) { return customEnc, nil })
			enc, err := encB.Build(rt)
			if err != nil {
				t.Fatalf("RowEncoderBuilder.Build(%v) = %v, want nil error", rt, err)
			}
			var decB RowDecoderBuilder
			decB.Register(customRT, func(reflect.Type) (func(io.Reader) (interface{}, error), error) { return customDec, nil })
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
	ut1Enc := func(val interface{}, w io.Writer) error {
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
	ut1Dec := func(r io.Reader) (interface{}, error) {
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
		want      interface{}
		customRT  reflect.Type
		customEnc func(interface{}, io.Writer) error
		customDec func(io.Reader) (interface{}, error)
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
			encB.Register(bench.customRT, func(reflect.Type) (func(interface{}, io.Writer) error, error) { return bench.customEnc, nil })
			enc, err := encB.Build(rt)
			if err != nil {
				b.Fatalf("RowEncoderBuilder.Build(%v) = %v, want nil error", rt, err)
			}
			var decB RowDecoderBuilder
			decB.Register(bench.customRT, func(reflect.Type) (func(io.Reader) (interface{}, error), error) { return bench.customDec, nil })
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
