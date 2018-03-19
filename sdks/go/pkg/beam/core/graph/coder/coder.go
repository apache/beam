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

// Package coder contains coder representation and utilities. Coders describe
// how to serialize and deserialize pipeline data and may be provided by users.
package coder

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

// CustomCoder contains possibly untyped encode/decode user functions that are
// type-bound at runtime. Universal coders can thus be used for many different
// types, but each CustomCoder instance will be bound to a specific type.
type CustomCoder struct {
	// Name is the coder name. Informational only.
	Name string
	// Type is the underlying concrete type that is being coded. It is
	// available to Enc and Dec. It must be a concrete type.
	Type reflect.Type

	// Enc is the encoding function : T -> []byte. It may optionally take a
	// reflect.Type parameter and return an error as well.
	Enc *funcx.Fn
	// Dec is the decoding function: []byte -> T. It may optionally take a
	// reflect.Type parameter and return an error as well.
	Dec *funcx.Fn
}

// TODO(herohde) 5/16/2017: do we want/need to allow user coders that follow the
// internal signature, which takes io.Reader/io.Writer? Do we need size estimation?
// Maybe we can get away with just handling protos as an internal coder.

// TODO(herohde) 5/16/2017: we're ignoring the inner/outer context concept
// present in java/python. Not clear whether we actually need it.

// Equals returns true iff the two custom coders are equal. It assumes that
// functions with the same name and types are identical.
func (c *CustomCoder) Equals(o *CustomCoder) bool {
	if c.Name != o.Name {
		return false
	}
	if c.Type != o.Type {
		return false
	}
	if c.Dec.Fn.Name() != o.Dec.Fn.Name() {
		return false
	}
	return c.Enc.Fn.Name() == o.Enc.Fn.Name()
}

func (c *CustomCoder) String() string {
	return fmt.Sprintf("%v[%v]", c.Type, c.Name)
}

// Type signatures of encode/decode for verification.
var (
	encodeSig = &funcx.Signature{
		OptArgs:   []reflect.Type{reflectx.Type},
		Args:      []reflect.Type{typex.TType}, // T to be substituted
		Return:    []reflect.Type{reflectx.ByteSlice},
		OptReturn: []reflect.Type{reflectx.Error}}

	decodeSig = &funcx.Signature{
		OptArgs:   []reflect.Type{reflectx.Type},
		Args:      []reflect.Type{reflectx.ByteSlice},
		Return:    []reflect.Type{typex.TType}, // T to be substituted
		OptReturn: []reflect.Type{reflectx.Error}}
)

// NewCustomCoder creates a coder for the supplied parameters defining a
// particular encoding strategy.
func NewCustomCoder(id string, t reflect.Type, encode, decode interface{}) (*CustomCoder, error) {
	enc, err := funcx.New(reflectx.MakeFunc(encode))
	if err != nil {
		return nil, fmt.Errorf("bad encode: %v", err)
	}
	if err := funcx.Satisfy(encode, funcx.Replace(encodeSig, typex.TType, t)); err != nil {
		return nil, fmt.Errorf("encode has incorrect signature: %v", err)
	}

	dec, err := funcx.New(reflectx.MakeFunc(decode))
	if err != nil {
		return nil, fmt.Errorf("bad decode: %v", err)
	}
	if err := funcx.Satisfy(decode, funcx.Replace(decodeSig, typex.TType, t)); err != nil {
		return nil, fmt.Errorf("decode has incorrect signature: %v", err)
	}

	c := &CustomCoder{
		Name: id,
		Type: t,
		Enc:  enc,
		Dec:  dec,
	}
	return c, nil
}

// Kind represents the type of coder used.
type Kind string

// Tags for the various Beam encoding strategies. https://beam.apache.org/documentation/programming-guide/#coders
// documents the usage of coders in the Beam environment.
const (
	Custom        Kind = "Custom" // Implicitly length-prefixed
	Bytes         Kind = "bytes"  // Implicitly length-prefixed as part of the encoding
	VarInt        Kind = "varint"
	WindowedValue Kind = "W"
	KV            Kind = "KV"

	// CoGBK is currently equivalent to either
	//
	//     KV<X,Iterable<Y>>         (if GBK)
	//     KV<X,Iterable<KV<int,Y>>> (if CoGBK, using a tagged union encoding)
	//
	// It requires special handling in translation to the model pipeline in the latter case
	// to add the incoming index for each input.
	//
	// TODO(BEAM-490): once this JIRA is done, this coder should become the new thing.
	CoGBK Kind = "CoGBK"
)

// Coder is a description of how to encode and decode values of a given type.
// Except for the "custom" kind, they are built in and must adhere to the
// (unwritten) Beam specification.
type Coder struct {
	Kind Kind
	T    typex.FullType

	Components []*Coder       // WindowedValue, KV, CoGBK
	Custom     *CustomCoder   // Custom
	Window     *window.Window // WindowedValue
}

// Equals returns true iff the two coders are equal. It assumes that
// functions with the same name and types are identical.
func (c *Coder) Equals(o *Coder) bool {
	if c.Kind != o.Kind {
		return false
	}
	if !typex.IsEqual(c.T, o.T) {
		return false
	}
	if len(c.Components) != len(o.Components) {
		return false
	}
	for i, elm := range c.Components {
		if !elm.Equals(o.Components[i]) {
			return false
		}
	}
	if c.Custom != nil {
		if !c.Custom.Equals(o.Custom) {
			return false
		}
	}
	if c.Window != nil {
		if !c.Window.Equals(o.Window) {
			return false
		}
	}
	return true
}

func (c *Coder) String() string {
	if c == nil {
		return "$"
	}
	if c.Custom != nil {
		return c.Custom.String()
	}

	ret := fmt.Sprintf("%v", c.Kind)
	if len(c.Components) > 0 {
		var args []string
		for _, elm := range c.Components {
			args = append(args, fmt.Sprintf("%v", elm))
		}
		ret += fmt.Sprintf("<%v>", strings.Join(args, ","))
	}
	if c.Window != nil {
		ret += fmt.Sprintf("!%v", c.Window)
	}
	return ret
}

// NewBytes returns a new []byte coder using the built-in scheme. It
// is always nested, for now.
func NewBytes() *Coder {
	return &Coder{Kind: Bytes, T: typex.New(reflectx.ByteSlice)}
}

// NewVarInt returns a new int32 coder using the built-in scheme.
func NewVarInt() *Coder {
	return &Coder{Kind: VarInt, T: typex.New(reflectx.Int32)}
}

// IsW returns true iff the coder is for a WindowedValue.
func IsW(c *Coder) bool {
	return c.Kind == WindowedValue
}

// NewW returns a WindowedValue coder for the window of elements.
func NewW(c *Coder, w *window.Window) *Coder {
	if c == nil {
		panic("coder must not be nil")
	}
	if w == nil {
		panic("window must not be nil")
	}

	return &Coder{
		Kind:       WindowedValue,
		T:          typex.NewW(c.T),
		Window:     w,
		Components: []*Coder{c},
	}
}

// IsKV returns true iff the coder is for key-value pairs.
func IsKV(c *Coder) bool {
	return c.Kind == KV
}

// NewKV returns a coder for key-value pairs.
func NewKV(components []*Coder) *Coder {
	checkCodersNotNil(components)
	return &Coder{
		Kind:       KV,
		T:          typex.New(typex.KVType, Types(components)...),
		Components: components,
	}
}

// IsCoGBK returns true iff the coder is for a CoGBK type.
func IsCoGBK(c *Coder) bool {
	return c.Kind == CoGBK
}

// NewCoGBK returns a coder for CoGBK elements.
func NewCoGBK(components []*Coder) *Coder {
	checkCodersNotNil(components)
	return &Coder{
		Kind:       CoGBK,
		T:          typex.New(typex.CoGBKType, Types(components)...),
		Components: components,
	}
}

// SkipW returns the data coder used by a WindowedValue, or returns the coder. This
// allows code to seamlessly traverse WindowedValues without additional conditional
// code.
func SkipW(c *Coder) *Coder {
	if c.Kind == WindowedValue {
		return c.Components[0]
	}
	return c
}

// Types returns a slice of types used by the supplied coders.
func Types(list []*Coder) []typex.FullType {
	var ret []typex.FullType
	for _, c := range list {
		ret = append(ret, c.T)
	}
	return ret
}

func checkCodersNotNil(list []*Coder) {
	for i, c := range list {
		if c == nil {
			panic(fmt.Sprintf("nil coder at index: %v", i))
		}
	}
}
