package graph

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/v1"
	"github.com/apache/beam/sdks/go/pkg/beam/protox"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"reflect"
)

// TODO(herohde) 4/11/2017: re-introduce TypeTag and disallow coder data context?

// CustomCoder contains possibly untyped encode/decode user functions that are
// type-bound at runtime. Universal coders can thus be used for many different
// types, but each CustomCoder instance will be bound to a specific type.
type CustomCoder struct {
	// Name is the coder name. Informational only.
	Name string
	// T is the underlying concrete type that is being coded.
	T reflect.Type

	// Enc is the encoding function : T -> []byte.
	Enc *UserFn
	// Dec is the decoding function: []byte -> T.
	Dec *UserFn

	// Data holds immediate values to be bound into "data" context field, if
	// present. We require symmetry between enc and dec for simplicity.
	Data interface{}
}

func (c *CustomCoder) String() string {
	return fmt.Sprintf("%s<%v>", c.Name, c.T)
}

func NewCustomCoder(id string, t reflect.Type, encode, decode, data interface{}) (*CustomCoder, error) {
	enc, err := ReflectFn(encode)
	if err != nil {
		return nil, fmt.Errorf("Bad encode: %v", err)
	}
	dec, err := ReflectFn(decode)
	if err != nil {
		return nil, fmt.Errorf("Bad decode: %v", err)
	}

	// TODO(herohde): validate coder signature.

	c := &CustomCoder{
		Name: id,
		T:    t,
		Enc:  enc,
		Dec:  dec,
		Data: data,
	}
	return c, nil
}

// CoderKind
type CoderKind string

const (
	Custom        CoderKind = "Custom"
	VarInt        CoderKind = "VarInt"
	Bytes         CoderKind = "Bytes"
	Pair          CoderKind = "Pair"
	LengthPrefix  CoderKind = "LengthPrefix"
	WindowedValue CoderKind = "WindowedValue"
	Stream        CoderKind = "Stream"
)

// WindowKind
type WindowKind string

const (
	GlobalWindow WindowKind = "GlobalWindow"
)

type Window struct {
	Kind WindowKind
}

func (w *Window) String() string {
	return fmt.Sprintf("%v", w.Kind)
}

// Coder is a description of how to encode and decode values of a given type.
// Except for the "custom" kind, they are built in and must adhere to the
// (unwritten) Beam specification. Immutable.
type Coder struct {
	Kind CoderKind
	T    reflect.Type

	Components []*Coder
	Custom     *CustomCoder
	Window     *Window
}

func (c *Coder) IsCustom() bool {
	_, ok := c.UnfoldCustom()
	return ok
}

func (c *Coder) UnfoldCustom() (*CustomCoder, bool) {
	if c.Kind != Custom || c.Custom == nil {
		return nil, false
	}
	return c.Custom, true
}

func (c *Coder) IsPair() bool {
	_, _, ok := c.UnfoldPair()
	return ok
}

func (c *Coder) UnfoldPair() (*Coder, *Coder, bool) {
	if c.Kind != Pair || len(c.Components) != 2 {
		return nil, nil, false
	}
	return c.Components[0], c.Components[1], true
}

func (c *Coder) IsLengthPrefix() bool {
	_, ok := c.UnfoldLengthPrefix()
	return ok
}

func (c *Coder) UnfoldLengthPrefix() (*Coder, bool) {
	if c.Kind != LengthPrefix || len(c.Components) != 1 {
		return nil, false
	}
	return c.Components[0], true
}

func (c *Coder) IsWindowedValue() bool {
	_, _, ok := c.UnfoldWindowedValue()
	return ok
}

func (c *Coder) UnfoldWindowedValue() (*Coder, *Window, bool) {
	if c.Kind != WindowedValue || len(c.Components) != 1 || c.Window == nil {
		return nil, nil, false
	}
	return c.Components[0], c.Window, true
}

func (c *Coder) IsStream() bool {
	_, ok := c.UnfoldStream()
	return ok
}

func (c *Coder) UnfoldStream() (*Coder, bool) {
	if c.Kind != Stream || len(c.Components) != 1 {
		return nil, false
	}
	return c.Components[0], true
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

/*
    "@type": "kind:windowed_value",
    "component_encodings": [
      {
         "@type": "StrUtf8Coder$eNprYEpOLEhMzkiNT0pNzNVLzk9JLSqGUlzBJUWhJWkWziAeVyGDZmMhY20hU5IeAAajEkY=",
         "component_encodings": []
      },
      {
         "@type": "kind:global_window"
      }
   ],
   "is_wrapper": true


    "@type": "kind:windowed_value",
    "component_encodings":[
      {"@type":"json"},
      {"@type":"kind:global_window"}
    ]
*/

// NOTE: the service may insert length-prefixed wrappers when it needs to know,
// such as inside KVs before GBK. It won't remove encoding layers.
//
//    "@type":"kind:windowed_value"
//    "component_encodings": [
//       { "@type":"kind:pair"
//         "component_encodings":[
//             {"@type":"kind:length_prefix",
//              "component_encodings":[
//                  {"@type":"json"}
//             ]},
//             {"@type":"kind:length_prefix",
//              "component_encodings":[
//                  {"@type":"json"}
//             ]}
//         ]
//       },
//       {"@type":"kind:global_window"}
//    ]

// CoderRef defines the (structured) Coder in serializable form. It is
// an artifact of the CloudObject encoding.
type CoderRef struct {
	Type         string      `json:"@type,omitempty"`
	Components   []*CoderRef `json:"component_encodings,omitempty"`
	IsWrapper    bool        `json:"is_wrapper,omitempty"`
	IsPairLike   bool        `json:"is_pair_like,omitempty"`
	IsStreamLike bool        `json:"is_stream_like,omitempty"`
}

const (
	pairType          = "kind:pair"
	lengthPrefixType  = "kind:length_prefix"
	windowedValueType = "kind:windowed_value"
	streamType        = "kind:stream"

	globalWindowType = "kind:global_window"
)

// EncodeCoder returns the encoded form understood by the runner.
func EncodeCoder(c *Coder) (*CoderRef, error) {
	switch c.Kind {
	case Custom:
		ref, err := EncodeCustomCoder(c.Custom)
		if err != nil {
			return nil, err
		}
		data, err := protox.EncodeBase64(ref)
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: data}, nil

	case Pair:
		if len(c.Components) != 2 {
			return nil, fmt.Errorf("Bad pair: %+v", c)
		}

		key, err := EncodeCoder(c.Components[0])
		if err != nil {
			return nil, err
		}
		value, err := EncodeCoder(c.Components[1])
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: pairType, Components: []*CoderRef{key, value}, IsPairLike: true}, nil

	case LengthPrefix:
		if len(c.Components) != 1 {
			return nil, fmt.Errorf("Bad length prefix: %+v", c)
		}

		elm, err := EncodeCoder(c.Components[0])
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: lengthPrefixType, Components: []*CoderRef{elm}}, nil

	case WindowedValue:
		if len(c.Components) != 1 || c.Window == nil {
			return nil, fmt.Errorf("Bad windowed value: %+v", c)
		}

		elm, err := EncodeCoder(c.Components[0])
		if err != nil {
			return nil, err
		}
		window, err := EncodeWindow(c.Window)
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: windowedValueType, Components: []*CoderRef{elm, window}, IsWrapper: true}, nil

	case Stream:
		if len(c.Components) != 1 {
			return nil, fmt.Errorf("Bad stream: %+v", c)
		}

		elm, err := EncodeCoder(c.Components[0])
		if err != nil {
			return nil, err
		}
		return &CoderRef{Type: streamType, Components: []*CoderRef{elm}, IsStreamLike: true}, nil

	default:
		return nil, fmt.Errorf("Bad coder kind: %v", c.Kind)
	}
}

func DecodeCoder(c *CoderRef) (*Coder, error) {
	switch c.Type {
	case pairType:
		if len(c.Components) != 2 {
			return nil, fmt.Errorf("Bad pair: %+v", c)
		}

		key, err := DecodeCoder(c.Components[0])
		if err != nil {
			return nil, err
		}
		value, err := DecodeCoder(c.Components[1])
		if err != nil {
			return nil, err
		}

		var t reflect.Type
		if value.Kind == Stream {
			// GBK result

			c, _ := value.UnfoldStream()

			t, err = reflectx.MakeGBK(key.T, c.T)
			if err != nil {
				return nil, err
			}
		} else {
			// KV

			t, err = reflectx.MakeKV(key.T, value.T)
			if err != nil {
				return nil, err
			}
		}
		return &Coder{Kind: Pair, T: t, Components: []*Coder{key, value}}, nil

	case lengthPrefixType:
		if len(c.Components) != 1 {
			return nil, fmt.Errorf("Bad length prefix: %+v", c)
		}

		elm, err := DecodeCoder(c.Components[0])
		if err != nil {
			return nil, err
		}
		return &Coder{Kind: LengthPrefix, T: elm.T, Components: []*Coder{elm}}, nil

	case windowedValueType:
		if len(c.Components) != 2 {
			return nil, fmt.Errorf("Bad windowed value: %+v", c)
		}

		elm, err := DecodeCoder(c.Components[0])
		if err != nil {
			return nil, err
		}
		window, err := DecodeWindow(c.Components[1])
		if err != nil {
			return nil, err
		}

		// TODO(herohde): construct type for WindowedValue? We may
		// need to handle them specially. Stream, too.

		return &Coder{Kind: WindowedValue, T: elm.T, Components: []*Coder{elm}, Window: window}, nil

	case streamType:
		if len(c.Components) != 1 {
			return nil, fmt.Errorf("Bad stream: %+v", c)
		}

		elm, err := DecodeCoder(c.Components[0])
		if err != nil {
			return nil, err
		}
		t := reflect.ChanOf(reflect.BothDir, elm.T)
		return &Coder{Kind: Stream, T: t, Components: []*Coder{elm}}, nil

	default:
		var ref v1.CustomCoder
		if err := protox.DecodeBase64(c.Type, &ref); err != nil {
			return nil, err
		}
		custom, err := DecodeCustomCoder(&ref)
		if err != nil {
			return nil, err
		}
		return &Coder{Kind: Custom, T: custom.T, Custom: custom}, nil
	}
}

func EncodeWindow(w *Window) (*CoderRef, error) {
	switch w.Kind {
	case GlobalWindow:
		return &CoderRef{Type: globalWindowType}, nil
	default:
		return nil, fmt.Errorf("Bad window kind: %v", w.Kind)
	}
}

func DecodeWindow(w *CoderRef) (*Window, error) {
	switch w.Type {
	case globalWindowType:
		return &Window{Kind: GlobalWindow}, nil
	default:
		return nil, fmt.Errorf("Bad window: %v", w.Type)
	}
}
