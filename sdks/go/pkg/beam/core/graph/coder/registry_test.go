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
	"reflect"
	"testing"
)

func clearRegistry() {
	coderRegistry = make(map[reflect.Type]func(reflect.Type) *CustomCoder)
	interfaceOrdering = []reflect.Type{}
}

type MyInt int

func (MyInt) SatisfiesA() {}
func (MyInt) SatisfiesB() {}

type MyStruct struct{}

func (*MyStruct) SatisfiesB() {} // Pointer Receiver
func (MyStruct) SatisfiesC()  {} // Value Receiver

type myA interface {
	SatisfiesA()
}
type myB interface {
	SatisfiesB()
}
type myC interface {
	SatisfiesC()
}

var (
	miType    = reflect.TypeOf((*MyInt)(nil)).Elem() // Implements myA & myB
	msPtrType = reflect.TypeOf((*MyStruct)(nil))     // Implements myB & myC
	msType    = msPtrType.Elem()                     // Implements MyC
	aType     = reflect.TypeOf((*myA)(nil)).Elem()
	bType     = reflect.TypeOf((*myB)(nil)).Elem()
	cType     = reflect.TypeOf((*myC)(nil)).Elem()
)

// Encoders and Decoder functions for the above types.

func miEnc(MyInt) []byte { return nil }
func miDec([]byte) MyInt { return 0 }

func msEnc(MyStruct) []byte     { return nil }
func msDec([]byte) MyStruct     { return MyStruct{} }
func msPtrEnc(*MyStruct) []byte { return nil }
func msPtrDec([]byte) *MyStruct { return nil }

func aEnc(myA) []byte { return nil }
func aDec([]byte) myA { return nil }

func bEnc(myB) []byte { return nil }
func bDec([]byte) myB { return nil }

func cEnc(myC) []byte { return nil }
func cDec([]byte) myC { return nil }

// TestRegisterCoder checks that RegisterCoder panics ag the panic behavior when validation fails.
// All the examples below are negative examples, and are intended to fail.
func TestRegisterCoder(t *testing.T) {
	tests := []struct {
		name     string
		typ      reflect.Type
		enc, dec any
	}{{
		name: "nonSatisfyingInterface",
		typ:  msType, enc: aEnc, dec: aDec,
	}, {
		name: "swapped",
		typ:  msType, enc: msDec, dec: msEnc,
	}, {
		name: "mismatchedDec",
		typ:  msType, enc: msEnc, dec: miDec,
	}, {
		name: "mismatchedEnc",
		typ:  msType, enc: miEnc, dec: msDec,
	}, {
		// TODO(BEAM-6578)- Consider using the pointer coder if it exists.
		name: "pointerType",
		typ:  msType, enc: msPtrEnc, dec: msPtrDec,
	}, {
		name: "valueType",
		typ:  msPtrType, enc: msEnc, dec: msDec,
	}, {
		name: "badEnc-inappropriateFn",
		typ:  msType, enc: func() {}, dec: msDec,
	}, {
		name: "badDec-inappropriateFn",
		typ:  msType, enc: msEnc, dec: func() {},
	}, {
		name: "badEnc-nil",
		typ:  msType, enc: nil, dec: msDec,
	}, {
		name: "badDec-nil",
		typ:  msType, enc: msEnc, dec: nil,
	}, {
		name: "badEnc-int",
		typ:  msType, enc: 41, dec: msDec,
	}, {
		name: "badDec-int",
		typ:  msType, enc: msEnc, dec: 43,
	},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					return
				}
				t.Fatalf("RegisterCoder(%v, %T, %T): want panic", msType, test.enc, test.dec)
			}()
			RegisterCoder(test.typ, test.enc, test.dec)
		})
	}
}

func TestLookupCustomCoder(t *testing.T) {
	newCC := func(t *testing.T, name string, et reflect.Type, enc, dec any) *CustomCoder {
		t.Helper()
		cc, err := NewCustomCoder(name, et, enc, dec)
		if err != nil {
			t.Errorf("couldn't get custom coder for %v: %v", et, err)
		}
		return cc
	}

	miCC := newCC(t, miType.String(), miType, miEnc, miDec)
	msCC := newCC(t, msType.String(), msType, msEnc, msDec)
	msPtrCC := newCC(t, msPtrType.String(), msPtrType, msPtrEnc, msPtrDec)

	tests := []struct {
		name   string
		config func(t *testing.T)
		lookup reflect.Type
		want   *CustomCoder
	}{{
		name:   "not_registered_specific",
		lookup: miType,
		want:   nil,
		config: func(t *testing.T) {},
	}, {
		name:   "not_registered_interface",
		lookup: cType,
		want:   nil,
		config: func(t *testing.T) {},
	}, {
		name:   "specific",
		lookup: miType,
		want:   miCC,
		config: func(t *testing.T) {
			RegisterCoder(msType, msEnc, msDec)
			RegisterCoder(miType, miEnc, miDec)
		},
	}, {
		name:   "interfaceDirect",
		lookup: aType,
		want:   newCC(t, aType.String(), aType, aEnc, aDec),
		config: func(t *testing.T) {
			RegisterCoder(aType, aEnc, aDec)
		},
	}, {
		name:   "interfaceImplements",
		lookup: miType,
		want:   newCC(t, aType.String(), miType, aEnc, aDec),
		config: func(t *testing.T) {
			RegisterCoder(aType, aEnc, aDec)
		},
	}, {
		name:   "interfaceNotImplemented",
		lookup: miType,
		want:   nil,
		config: func(t *testing.T) {
			RegisterCoder(cType, cEnc, cDec)
		},
	}, {
		name:   "specificPrecedence",
		lookup: miType,
		want:   miCC,
		config: func(t *testing.T) {
			RegisterCoder(miType, miEnc, miDec)
			RegisterCoder(aType, aEnc, aDec)
		},
	}, {
		name:   "interfacePrecedence",
		lookup: miType,
		want:   newCC(t, bType.String(), miType, bEnc, bDec),
		config: func(t *testing.T) {
			RegisterCoder(aType, aEnc, aDec)
			RegisterCoder(bType, bEnc, bDec)
		},
	}, {
		name:   "interfacePrecedence2",
		lookup: miType,
		want:   newCC(t, bType.String(), miType, bEnc, bDec),
		config: func(t *testing.T) {
			RegisterCoder(aType, aEnc, aDec)
			RegisterCoder(bType, bEnc, bDec)
			RegisterCoder(cType, cEnc, cDec)
			if got, want := len(interfaceOrdering), 3; got != want {
				t.Fatalf("interfaceOrdering but has %v elements, want %v; contents: %v", got, want, interfaceOrdering)
			}
		},
	}, {
		name:   "interfacePrecedenceOverriding",
		lookup: miType,
		want:   newCC(t, aType.String(), miType, aEnc, aDec),
		config: func(t *testing.T) {
			RegisterCoder(aType, aEnc, aDec)
			RegisterCoder(bType, bEnc, bDec)
			RegisterCoder(aType, aEnc, aDec)
			RegisterCoder(cType, cEnc, cDec)
			if got, want := len(interfaceOrdering), 3; got != want {
				t.Fatalf("interfaceOrdering but has %v elements, want %v; contents: %v", got, want, interfaceOrdering)
			}
		},
	}, {
		name:   "interfacePointerRecv",
		lookup: msPtrType,
		want:   newCC(t, bType.String(), msPtrType, bEnc, bDec),
		config: func(t *testing.T) {
			RegisterCoder(cType, cEnc, cDec)
			RegisterCoder(bType, bEnc, bDec)
		},
	}, {
		name:   "interfacePointerRecv_A",
		lookup: msPtrType,
		// Pointer Receivers also implement all value methods,
		// so *MyStruct, implements myC. This means if an interface
		// is registered late, that a pointer's value type satisfies,
		// then the value type coder will be used.
		want: newCC(t, cType.String(), msPtrType, cEnc, cDec),
		config: func(t *testing.T) {
			RegisterCoder(bType, bEnc, bDec)
			RegisterCoder(cType, cEnc, cDec)
		},
	}, {
		name:   "interfaceValueRecv",
		lookup: msType,
		want:   newCC(t, cType.String(), msType, cEnc, cDec),
		config: func(t *testing.T) {
			RegisterCoder(cType, cEnc, cDec) // Only one that satisfies.
			RegisterCoder(bType, bEnc, bDec)
		},
	}, {
		name:   "pointersPtrOverValue",
		lookup: msPtrType,
		want:   msPtrCC,
		config: func(t *testing.T) {
			RegisterCoder(msType, msEnc, msDec)
			RegisterCoder(msPtrType, msPtrEnc, msPtrDec) // Should pick this one.
			// Conflating interfaces.
			RegisterCoder(bType, bEnc, bDec)
			RegisterCoder(cType, cEnc, cDec)
		},
	}, {
		name:   "pointersValueOverPointer",
		lookup: msType,
		want:   msCC,
		config: func(t *testing.T) {
			RegisterCoder(msType, msEnc, msDec) // Should pick this one.
			RegisterCoder(msPtrType, msPtrEnc, msPtrDec)
			// Conflating interfaces.
			RegisterCoder(bType, bEnc, bDec)
			RegisterCoder(cType, cEnc, cDec)
		},
	}, {
		// TODO(BEAM-6578)- Consider using the pointer coder if it exists.
		name:   "pointersNoDerefereningType",
		lookup: msType,
		want:   nil,
		config: func(t *testing.T) {
			RegisterCoder(msPtrType, msPtrEnc, msPtrDec)
		},
	}, {
		name:   "specificNoIndirectingType",
		lookup: msPtrType,
		want:   nil,
		config: func(t *testing.T) {
			// Will never be relaxed, as it would lead to excess copying.
			RegisterCoder(msType, msEnc, msDec)
		},
	},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			clearRegistry()
			test.config(t)
			got := LookupCustomCoder(test.lookup)
			if !test.want.Equals(got) {
				t.Errorf("lookupCustomCoder(%v) = %v, want %v", test.lookup, got, test.want)
			}
		})
	}
}
