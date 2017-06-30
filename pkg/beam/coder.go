package beam

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// Coder defines how to encode and decode values of type 'A' into byte streams.
// Coders are attached to PCollections of the same type. For PCollections
// consumed by GBK, the attached coders are required to be deterministic.
type Coder struct {
	coder *coder.Coder
}

// IsValid returns true iff the Coder is valid. Any use of an invalid Coder
// will result in a panic.
func (c Coder) IsValid() bool {
	return c.coder != nil
}

// Type returns the full type 'A' of elements the coder can encode and decode.
// 'A' must be a concrete Windowed Value type, such as W<int> or
// W<KV<int,string>>.
func (c Coder) Type() typex.FullType {
	if !c.IsValid() {
		panic("Invalid Coder")
	}
	return c.coder.T
}

func (c Coder) String() string {
	if c.coder == nil {
		return "$"
	}
	return c.coder.String()
}

// TODO(herohde) 4/4/2017: for convenience, we use the magic json coding
// everywhere. To be replaced by Coder registry, sharing, etc.

// TODO: select optimal coder based on type, notably handling int, string, etc.

// NewCoder infers a Coder for any bound full type.
func NewCoder(t typex.FullType) Coder {
	c, err := inferCoder(t)
	if err != nil {
		panic(err) // for now
	}
	return Coder{c}
}

func inferCoder(t typex.FullType) (*coder.Coder, error) {
	switch t.Class() {
	case typex.Concrete, typex.Container:
		c, err := newJSONCoder(t.Type())
		if err != nil {
			return nil, err
		}
		return &coder.Coder{Kind: coder.Custom, T: t, Custom: c}, nil

	case typex.Composite:
		c, err := inferCoders(t.Components())
		if err != nil {
			return nil, err
		}

		switch t.Type() {
		case typex.KVType:
			return &coder.Coder{Kind: coder.KV, T: t, Components: c}, nil
		case typex.GBKType:
			return &coder.Coder{Kind: coder.GBK, T: t, Components: c}, nil
		case typex.CoGBKType:
			return &coder.Coder{Kind: coder.CoGBK, T: t, Components: c}, nil
		case typex.WindowedValueType:
			return &coder.Coder{Kind: coder.WindowedValue, T: t, Components: c, Window: window.NewGlobalWindow()}, nil

		default:
			panic(fmt.Sprintf("Unexpected composite type: %v", t))
		}
	default:
		panic(fmt.Sprintf("Unexpected type: %v", t))
	}
}

func inferCoders(list []typex.FullType) ([]*coder.Coder, error) {
	var ret []*coder.Coder
	for _, t := range list {
		c, err := inferCoder(t)
		if err != nil {
			return nil, err
		}
		ret = append(ret, c)
	}
	return ret, nil
}

// TODO(herohde) 4/5/2017: decide whether we want an Encoded form. For now,
// we'll use exploded form coders only using typex.T. We might also need a
// form that doesn't require LengthPrefix'ing to cut up the bytestream from
// the FnHarness.

// Concrete and universal custom coders both have a similar signature.
// Conversion is handled by reflection.

func jsonEnc(in typex.T) ([]byte, error) {
	return json.Marshal(in)
}

func jsonDec(t reflect.Type, in []byte) (typex.T, error) {
	val := reflect.New(t)
	if err := json.Unmarshal(in, val.Interface()); err != nil {
		return nil, err
	}
	return val.Elem().Interface(), nil
}

func newJSONCoder(t reflect.Type) (*coder.CustomCoder, error) {
	c, err := coder.NewCustomCoder("json", t, jsonEnc, jsonDec)
	if err != nil {
		return nil, fmt.Errorf("invalid coder: %v", err)
	}
	return c, nil
}
