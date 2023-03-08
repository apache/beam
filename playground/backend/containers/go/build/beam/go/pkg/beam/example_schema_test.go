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

package beam_test

import (
	"bytes"
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/google/go-cmp/cmp"
)

// RegisterSchemaProvider must be called before beam.Init, and conventionally in a package init block.
func init() {
	beam.RegisterSchemaProvider(reflect.TypeOf((*Alphabet)(nil)).Elem(), &AlphabetProvider{})
	// TODO(BEAM-9615): Registerying a self encoding type causes a cycle. Needs resolving.
	// beam.RegisterType(reflect.TypeOf((*Cyrillic)(nil)))
	beam.RegisterType(reflect.TypeOf((*Latin)(nil)))
	beam.RegisterType(reflect.TypeOf((*Ελληνικά)(nil)))
}

type Alphabet interface {
	alphabet() string
}

type Cyrillic struct {
	A, B int
}

func (*Cyrillic) alphabet() string {
	return "Cyrillic"
}

type Latin struct {
	// Unexported fields are not serializable by beam schemas by default
	// so we need to handle this ourselves.
	c uint64
	d *float32
}

func (*Latin) alphabet() string {
	return "Latin"
}

type Ελληνικά struct {
	q string
	G func() string
}

func (*Ελληνικά) alphabet() string {
	return "Ελληνικά"
}

// AlphabetProvider provides encodings for types that implement the Alphabet interface.
type AlphabetProvider struct {
	enc *coder.RowEncoderBuilder
	dec *coder.RowDecoderBuilder
}

var (
	typeCyrillic = reflect.TypeOf((*Cyrillic)(nil))
	typeLatin    = reflect.TypeOf((*Latin)(nil))
	typeΕλληνικά = reflect.TypeOf((*Ελληνικά)(nil))
)

func (p *AlphabetProvider) FromLogicalType(rt reflect.Type) (reflect.Type, error) {
	// FromLogicalType produces schema representative types, which match the encoders
	// and decoders that this function generates for this type.
	// While this example uses statically assigned schema representative types, it's
	// possible to generate the returned reflect.Type dynamically instead, using the
	// reflect package.
	switch rt {
	// The Cyrillic type is able to be encoded by default, so we simply use it directly
	// as it's own representative type.
	case typeCyrillic:
		return typeCyrillic, nil
	case typeLatin:
		// The Latin type only has unexported fields, so we need to make the equivalent
		// have exported fields.
		return reflect.TypeOf((*struct {
			C uint64
			D *float32
		})(nil)).Elem(), nil
	case typeΕλληνικά:
		return reflect.TypeOf((*struct{ Q string })(nil)).Elem(), nil
	}
	return nil, fmt.Errorf("Unknown Alphabet: %v", rt)
}

// BuildEncoder returns beam schema encoder functions for types with the Alphabet interface.
func (p *AlphabetProvider) BuildEncoder(rt reflect.Type) (func(any, io.Writer) error, error) {
	switch rt {
	case typeCyrillic:
		if p.enc == nil {
			p.enc = &coder.RowEncoderBuilder{}
		}
		// Since Cyrillic is by default encodable, defer to the standard schema row decoder for the type.
		return p.enc.Build(rt)
	case typeLatin:
		return func(iface any, w io.Writer) error {
			v := iface.(*Latin)
			// Beam Schema Rows have a header that indicates which fields if any, are nil.
			if err := coder.WriteRowHeader(2, func(i int) bool {
				if i == 1 {
					return v.d == nil
				}
				return false
			}, w); err != nil {
				return err
			}
			// Afterwards, each field is encoded using the appropriate helper.
			if err := coder.EncodeVarUint64(v.c, w); err != nil {
				return err
			}
			// Nil fields have nothing written for them other than the header.
			if v.d != nil {
				if err := coder.EncodeDouble(float64(*v.d), w); err != nil {
					return err
				}
			}
			return nil
		}, nil
	case typeΕλληνικά:
		return func(iface any, w io.Writer) error {
			// Since the representation for Ελληνικά never has nil fields
			// we can use the simple header helper.
			if err := coder.WriteSimpleRowHeader(1, w); err != nil {
				return err
			}
			v := iface.(*Ελληνικά)
			if err := coder.EncodeStringUTF8(v.q, w); err != nil {
				return fmt.Errorf("decoding string field A: %v", err)
			}
			return nil
		}, nil
	}
	return nil, fmt.Errorf("Unknown Alphabet: %v", rt)
}

// BuildDecoder returns beam schema decoder functions for types with the Alphabet interface.
func (p *AlphabetProvider) BuildDecoder(rt reflect.Type) (func(io.Reader) (any, error), error) {
	switch rt {
	case typeCyrillic:
		if p.dec == nil {
			p.dec = &coder.RowDecoderBuilder{}
		}
		// Since Cyrillic is by default encodable, defer to the standard schema row decoder for the type.
		return p.dec.Build(rt)
	case typeLatin:
		return func(r io.Reader) (any, error) {
			// Since the d field can be nil, we use the header get the nil bits.
			n, nils, err := coder.ReadRowHeader(r)
			if err != nil {
				return nil, err
			}
			// Header returns the number of fields, so we check if it has what we
			// expect. This allows schemas to evolve if necessary.
			if n != 2 {
				return nil, fmt.Errorf("expected 2 fields, but got %v", n)
			}
			c, err := coder.DecodeVarUint64(r)
			if err != nil {
				return nil, err
			}
			// Check if the field is nil before trying to decode a value for it.
			var d *float32
			if !coder.IsFieldNil(nils, 1) {
				f, err := coder.DecodeDouble(r)
				if err != nil {
					return nil, err
				}
				f32 := float32(f)
				d = &f32
			}
			return &Latin{
				c: c,
				d: d,
			}, nil
		}, nil
	case typeΕλληνικά:
		return func(r io.Reader) (any, error) {
			// Since the representation for Ελληνικά never has nil fields
			// we can use the simple header helper. Returns an error if
			// something unexpected occurs.
			if err := coder.ReadSimpleRowHeader(1, r); err != nil {
				return nil, err
			}
			q, err := coder.DecodeStringUTF8(r)
			if err != nil {
				return nil, fmt.Errorf("decoding string field A: %v", err)
			}
			return &Ελληνικά{
				q: q,
			}, nil
		}, nil
	}
	return nil, nil
}

// Schema providers work on fields of schema encoded types.
type translation struct {
	C *Cyrillic
	L *Latin
	E *Ελληνικά
}

func ExampleRegisterSchemaProvider() {
	f := float32(42.789)
	want := translation{
		C: &Cyrillic{A: 123, B: 456},
		L: &Latin{c: 789, d: &f},
		E: &Ελληνικά{q: "testing"},
	}
	rt := reflect.TypeOf((*translation)(nil)).Elem()
	enc, err := coder.RowEncoderForStruct(rt)
	if err != nil {
		panic(err)
	}
	dec, err := coder.RowDecoderForStruct(rt)
	if err != nil {
		panic(err)
	}
	var buf bytes.Buffer
	if err := enc(want, &buf); err != nil {
		panic(err)
	}
	got, err := dec(&buf)
	if err != nil {
		panic(err)
	}
	if d := cmp.Diff(want, got,
		cmp.AllowUnexported(Latin{}, Ελληνικά{})); d != "" {
		fmt.Printf("diff in schema encoding translation: (-want,+got)\n%v\n", d)
	} else {
		fmt.Println("No diffs!")
	}
	// Output: No diffs!
}
