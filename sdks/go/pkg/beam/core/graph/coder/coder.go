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
	"io"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
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

	ID string // (optional) This coder's ID if translated from a pipeline proto.
}

// TODO(herohde) 5/16/2017: do we want/need to allow user coders that follow the
// internal signature, which takes io.Reader/io.Writer? Do we need size estimation?
// Maybe we can get away with just handling protos as an internal coder.

// TODO(herohde) 5/16/2017: we're ignoring the inner/outer context concept
// present in java/python. Not clear whether we actually need it.

// Equals returns true iff the two custom coders are equal. It assumes that
// functions with the same name and types are identical.
func (c *CustomCoder) Equals(o *CustomCoder) bool {
	if c == nil && o == nil {
		return true
	}
	if c == nil && o != nil || c != nil && o == nil {
		return false
	}
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
	if c.ID == "" {
		return fmt.Sprintf("%v[%v]", c.Type, c.Name)
	}
	return fmt.Sprintf("%v[%v;%v]", c.Type, c.Name, c.ID)
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

// ElementEncoder encapsulates being able to encode an element into a writer.
type ElementEncoder interface {
	Encode(element interface{}, w io.Writer) error
}

// ElementDecoder encapsulates being able to decode an element from a reader.
type ElementDecoder interface {
	Decode(r io.Reader) (interface{}, error)
}

func validateEncoder(t reflect.Type, encode interface{}) error {
	// Check if it uses the real type in question.
	if err := funcx.Satisfy(encode, funcx.Replace(encodeSig, typex.TType, t)); err != nil {
		return errors.WithContext(err, "validateEncoder: validating signature")
	}
	// TODO(lostluck): 2019.02.03 - Determine if there are encode allocation bottlenecks.
	return nil
}

func validateDecoder(t reflect.Type, decode interface{}) error {
	// Check if it uses the real type in question.
	if err := funcx.Satisfy(decode, funcx.Replace(decodeSig, typex.TType, t)); err != nil {
		return errors.WithContext(err, "validateDecoder: validating signature")
	}
	// TODO(lostluck): 2019.02.03 - Expand cases to avoid []byte -> interface{} conversion
	// in exec, & a beam Decoder interface.
	return nil
}

// NewCustomCoder creates a coder for the supplied parameters defining a
// particular encoding strategy.
func NewCustomCoder(id string, t reflect.Type, encode, decode interface{}) (*CustomCoder, error) {
	if err := validateEncoder(t, encode); err != nil {
		return nil, errors.WithContext(err, "NewCustomCoder")
	}
	enc, err := funcx.New(reflectx.MakeFunc(encode))
	if err != nil {
		return nil, errors.Wrap(err, "bad encode")
	}
	if err := validateDecoder(t, decode); err != nil {
		return nil, errors.WithContext(err, "NewCustomCoder")
	}

	dec, err := funcx.New(reflectx.MakeFunc(decode))
	if err != nil {
		return nil, errors.Wrap(err, "bad decode")
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
	Custom             Kind = "Custom" // Implicitly length-prefixed
	Bytes              Kind = "bytes"  // Implicitly length-prefixed as part of the encoding
	String             Kind = "string" // Implicitly length-prefixed as part of the encoding.
	Bool               Kind = "bool"
	VarInt             Kind = "varint"
	Double             Kind = "double"
	Row                Kind = "R"
	Timer              Kind = "T"
	WindowedValue      Kind = "W"
	ParamWindowedValue Kind = "PW"
	Iterable           Kind = "I"
	KV                 Kind = "KV"
	LP                 Kind = "LP" // Explicitly length prefixed, likely at the runner's direction.

	Window Kind = "window" // A debug wrapper around a window coder.

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

	Components []*Coder     // WindowedValue, KV, CoGBK
	Custom     *CustomCoder // Custom
	Window     *WindowCoder // WindowedValue

	ID string // (optional) This coder's ID if translated from a pipeline proto.
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
		if c.ID == "" {
			return c.Custom.String()
		}
		return fmt.Sprintf("%v;%v", c.Custom, c.ID)
	}

	ret := fmt.Sprintf("%v", c.Kind)
	if c.ID != "" {
		ret = fmt.Sprintf("%v;%v", c.Kind, c.ID)
	}
	if len(c.Components) > 0 {
		var args []string
		for _, elm := range c.Components {
			args = append(args, fmt.Sprintf("%v", elm))
		}
		ret += fmt.Sprintf("<%v>", strings.Join(args, ","))
	}
	switch c.Kind {
	case WindowedValue, ParamWindowedValue, Window, Timer:
		ret += fmt.Sprintf("!%v", c.Window)
	case KV, CoGBK, Bytes, Bool, VarInt, Double, String, LP: // No additional info.
	default:
		ret += fmt.Sprintf("[%v]", c.T)
	}
	return ret
}

// NewBytes returns a new []byte coder using the built-in scheme. It
// is always nested, for now.
func NewBytes() *Coder {
	return &Coder{Kind: Bytes, T: typex.New(reflectx.ByteSlice)}
}

// NewBool returns a new bool coder using the built-in scheme.
func NewBool() *Coder {
	return &Coder{Kind: Bool, T: typex.New(reflectx.Bool)}
}

// NewVarInt returns a new int64 coder using the built-in scheme.
func NewVarInt() *Coder {
	return &Coder{Kind: VarInt, T: typex.New(reflectx.Int64)}
}

// NewDouble returns a new double coder using the built-in scheme.
func NewDouble() *Coder {
	return &Coder{Kind: Double, T: typex.New(reflectx.Float64)}
}

// NewString returns a new string coder using the built-in scheme.
func NewString() *Coder {
	return &Coder{Kind: String, T: typex.New(reflectx.String)}
}

// IsW returns true iff the coder is for a WindowedValue.
func IsW(c *Coder) bool {
	return c.Kind == WindowedValue
}

// NewW returns a WindowedValue coder for the window of elements.
func NewW(c *Coder, w *WindowCoder) *Coder {
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

// NewPW returns a ParamWindowedValue coder for the window of elements.
func NewPW(c *Coder, w *WindowCoder) *Coder {
	if c == nil {
		panic("coder must not be nil")
	}
	if w == nil {
		panic("window must not be nil")
	}

	return &Coder{
		Kind:       ParamWindowedValue,
		T:          typex.NewW(c.T),
		Window:     w,
		Components: []*Coder{c},
	}
}

// NewT returns a timer coder for the window of elements.
func NewT(c *Coder, w *WindowCoder) *Coder {
	if c == nil {
		panic("coder must not be nil")
	}
	if w == nil {
		panic("window must not be nil")
	}

	// TODO(BEAM-10660): Implement proper timer support.
	return &Coder{
		Kind: Timer,
		T: typex.New(reflect.TypeOf((*struct {
			Key                          []byte // elm type.
			Tag                          string
			Windows                      []byte // []typex.Window
			Clear                        bool
			FireTimestamp, HoldTimestamp int64
			Span                         int
		})(nil)).Elem()),
		Window:     w,
		Components: []*Coder{c},
	}
}

// NewI returns an iterable coder in the form of a slice.
func NewI(c *Coder) *Coder {
	if c == nil {
		panic("coder must not be nil")
	}
	t := typex.New(reflect.SliceOf(c.T.Type()), c.T)
	return &Coder{Kind: Iterable, T: t, Components: []*Coder{c}}
}

// NewR returns a schema row coder for the type.
func NewR(t typex.FullType) *Coder {
	return &Coder{
		Kind: Row,
		T:    t,
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

// CoderFrom is a helper that creates a Coder from a CustomCoder.
func CoderFrom(c *CustomCoder) *Coder {
	return &Coder{Kind: Custom, T: typex.New(c.Type), Custom: c}
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
