package beam

import (
	"encoding/json"
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/coder"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"reflect"
)

// Coder represents a coder.
type Coder struct {
	coder *coder.Coder
}

func (c Coder) IsValid() bool {
	return c.coder != nil
}

func (c Coder) String() string {
	if c.coder == nil {
		return "$"
	}
	return c.coder.String()
}

// TODO(herohde) 4/4/2017: for convenience, we use the magic json coding
// everywhere. To be replaced by Coder registry, sharing, etc.

func NewCoder(t typex.FullType) Coder {
	c, err := inferCoder(t)
	if err != nil {
		panic(err) // for now
	}
	return Coder{c}
}

func inferCoder(t typex.FullType) (*coder.Coder, error) {
	switch t.Class() {
	case typex.Concrete:
		c, err := NewJSONCoder(t.Type())
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
			return &coder.Coder{Kind: coder.WindowedValue, T: t, Components: c, Window: &coder.Window{Kind: coder.GlobalWindow}}, nil

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

// Concrete and universal coders both have a similar signature. Conversion is
// handled by reflection.

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

// TODO: select optimal coder based on type, notably handling int, string, etc.

func NewJSONCoder(t reflect.Type) (*coder.CustomCoder, error) {
	c, err := coder.NewCustomCoder("json", t, jsonEnc, jsonDec)
	if err != nil {
		return nil, fmt.Errorf("invalid coder: %v", err)
	}
	return c, nil
}
