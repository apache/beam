package coder

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/userfn"
	"reflect"
)

// CustomCoder contains possibly untyped encode/decode user functions that are
// type-bound at runtime. Universal coders can thus be used for many different
// types, but each CustomCoder instance will be bound to a specific type.
type CustomCoder struct {
	// Name is the coder name. Informational only.
	Name string
	// FullType is the underlying concrete type that is being coded. It is
	// available to Enc and Dec. It must be a concrete type.
	Type reflect.Type

	// Enc is the encoding function : T -> []byte.
	Enc *userfn.UserFn
	// Dec is the decoding function: []byte -> T.
	Dec *userfn.UserFn
}

func (c *CustomCoder) String() string {
	return fmt.Sprintf("%s<%v>", c.Name, c.Type)
}

func NewCustomCoder(id string, t reflect.Type, encode, decode interface{}) (*CustomCoder, error) {
	enc, err := userfn.New(encode)
	if err != nil {
		return nil, fmt.Errorf("Bad encode: %v", err)
	}
	dec, err := userfn.New(decode)
	if err != nil {
		return nil, fmt.Errorf("Bad decode: %v", err)
	}

	// TODO(herohde): validate coder signature.

	c := &CustomCoder{
		Name: id,
		Type: t,
		Enc:  enc,
		Dec:  dec,
	}
	return c, nil
}

// WindowKind
type WindowKind string

const (
	GlobalWindow WindowKind = "GlobalWindow"
)

// Window represents the window encoding.
type Window struct {
	Kind WindowKind
}

func (w *Window) String() string {
	return fmt.Sprintf("%v", w.Kind)
}

// CoderKind
type CoderKind string

const (
	Custom        CoderKind = "Custom" // Implicitly length-prefixed
	VarInt        CoderKind = "VarInt"
	Bytes         CoderKind = "Bytes"
	WindowedValue CoderKind = "WindowedValue"
	KV            CoderKind = "KV"
	GBK           CoderKind = "GBK"
	CoGBK         CoderKind = "CoGBK"
)

// Coder is a description of how to encode and decode values of a given type.
// Except for the "custom" kind, they are built in and must adhere to the
// (unwritten) Beam specification.
type Coder struct {
	Kind CoderKind
	T    typex.FullType

	Components []*Coder
	Custom     *CustomCoder
	Window     *Window
}

func (c *Coder) String() string {
	ret := fmt.Sprintf("{%v", c.Kind)
	if len(c.Components) > 0 {
		ret += fmt.Sprintf(" %v", c.Components)
	}
	if c.Custom != nil {
		ret += fmt.Sprintf(" %v", c.Custom)
	}
	if c.Window != nil {
		ret += fmt.Sprintf(" @%v", c.Window)
	}
	ret += "}"
	return ret
}

// Convenience methods to operate through the top-level WindowedValue.

func IsW(c *Coder) bool {
	return c.Kind == WindowedValue
}

func NewW(c *Coder, w *Window) *Coder {
	return &Coder{
		Kind:       WindowedValue,
		T:          typex.NewW(c.T),
		Window:     w,
		Components: []*Coder{c},
	}
}

func IsWKV(c *Coder) bool {
	return IsW(c) && SkipW(c).Kind == KV
}

func NewWKV(components []*Coder, w *Window) *Coder {
	c := &Coder{
		Kind:       KV,
		T:          typex.New(typex.KVType, Types(components)...),
		Components: components,
	}
	return NewW(c, w)
}

func IsWGBK(c *Coder) bool {
	return IsW(c) && SkipW(c).Kind == GBK
}

func NewWGBK(components []*Coder, w *Window) *Coder {
	c := &Coder{
		Kind:       GBK,
		T:          typex.New(typex.GBKType, Types(components)...),
		Components: components,
	}
	return NewW(c, w)
}

func IsWCoGBK(c *Coder) bool {
	return IsW(c) && SkipW(c).Kind == CoGBK
}

func NewWCoGBK(components []*Coder, w *Window) *Coder {
	c := &Coder{
		Kind:       CoGBK,
		T:          typex.New(typex.CoGBKType, Types(components)...),
		Components: components,
	}
	return NewW(c, w)
}

func SkipW(c *Coder) *Coder {
	if c.Kind == WindowedValue {
		return c.Components[0]
	}
	return c
}

func Types(list []*Coder) []typex.FullType {
	var ret []typex.FullType
	for _, c := range list {
		ret = append(ret, c.T)
	}
	return ret
}
