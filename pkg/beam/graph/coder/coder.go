package coder

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
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
	Enc *userfn.UserFn
	// Dec is the decoding function: []byte -> T. It may optionally take a
	// reflect.Type parameter and return an error as well.
	Dec *userfn.UserFn
}

// TODO(herohde) 5/16/2017: do we want/need to allow user coders that follow the
// internal signature, which takes io.Reader/io.Writer? Do we need size estimation?
// Maybe we can get away with just handling protos as an internal coder.

// TODO(herohde) 5/16/2017: we're ignoring the inner/outer context concept
// present in java/python. Not clear whether we actually need it.

func (c *CustomCoder) String() string {
	return fmt.Sprintf("%v[%v]", c.Type, c.Name)
}

// Type signatures of encode/decode for verification.
var (
	encodeSig = &userfn.Signature{
		OptArgs:   []reflect.Type{reflectx.Type},
		Args:      []reflect.Type{typex.TType}, // T to be substituted
		Return:    []reflect.Type{reflectx.ByteSlice},
		OptReturn: []reflect.Type{reflectx.Error}}

	decodeSig = &userfn.Signature{
		OptArgs:   []reflect.Type{reflectx.Type},
		Args:      []reflect.Type{reflectx.ByteSlice},
		Return:    []reflect.Type{typex.TType}, // T to be substituted
		OptReturn: []reflect.Type{reflectx.Error}}
)

// NewCustomCoder creates a coder for the supplied parameters defining a
// particular encoding strategy.
func NewCustomCoder(id string, t reflect.Type, encode, decode interface{}) (*CustomCoder, error) {
	enc, err := userfn.New(encode)
	if err != nil {
		return nil, fmt.Errorf("bad encode: %v", err)
	}
	if err := userfn.Satisfy(encode, userfn.Replace(encodeSig, typex.TType, t)); err != nil {
		return nil, fmt.Errorf("encode has incorrect signature: %v", err)
	}

	dec, err := userfn.New(decode)
	if err != nil {
		return nil, fmt.Errorf("bad decode: %v", err)
	}
	if err := userfn.Satisfy(decode, userfn.Replace(decodeSig, typex.TType, t)); err != nil {
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
	Bytes         Kind = "bytes"  // Implicitly length-prefixed
	WindowedValue Kind = "W"
	KV            Kind = "KV"
	GBK           Kind = "GBK"
	CoGBK         Kind = "CoGBK"
)

// Coder is a description of how to encode and decode values of a given type.
// Except for the "custom" kind, they are built in and must adhere to the
// (unwritten) Beam specification.
type Coder struct {
	Kind Kind
	T    typex.FullType

	Components []*Coder       // WindowedValue, KV, GCK, CoGBK
	Custom     *CustomCoder   // Custom
	Window     *window.Window // WindowedValue
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

// Convenience methods to operate through the top-level WindowedValue.

// IsW returns true iff the coder is for a WindowedValue.
func IsW(c *Coder) bool {
	return c.Kind == WindowedValue
}

// NewW returns a WindowedValue coder for the window of elements.
func NewW(c *Coder, w *window.Window) *Coder {
	return &Coder{
		Kind:       WindowedValue,
		T:          typex.NewW(c.T),
		Window:     w,
		Components: []*Coder{c},
	}
}

// IsWKV returns true iff the coder is for a WindowedValue key-value pair.
func IsWKV(c *Coder) bool {
	return IsW(c) && SkipW(c).Kind == KV
}

// NewWKV returns a WindowedValue coder for the window of KV elements.
func NewWKV(components []*Coder, w *window.Window) *Coder {
	c := &Coder{
		Kind:       KV,
		T:          typex.New(typex.KVType, Types(components)...),
		Components: components,
	}
	return NewW(c, w)
}

// IsWGBK returns true iff the coder is for a WindowedValue GBK type.
func IsWGBK(c *Coder) bool {
	return IsW(c) && SkipW(c).Kind == GBK
}

// NewWGBK returns a WindowedValue coder for the window of GBK elements.
func NewWGBK(components []*Coder, w *window.Window) *Coder {
	c := &Coder{
		Kind:       GBK,
		T:          typex.New(typex.GBKType, Types(components)...),
		Components: components,
	}
	return NewW(c, w)
}

// IsWCoGBK returns true iff the coder is for a windowed CoGBK type.
func IsWCoGBK(c *Coder) bool {
	return IsW(c) && SkipW(c).Kind == CoGBK
}

// NewWCoGBK returns a WindowedValue coder for the window of CoGBK elements.
func NewWCoGBK(components []*Coder, w *window.Window) *Coder {
	c := &Coder{
		Kind:       CoGBK,
		T:          typex.New(typex.CoGBKType, Types(components)...),
		Components: components,
	}
	return NewW(c, w)
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
