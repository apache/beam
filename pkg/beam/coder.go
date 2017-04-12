package beam

import (
	"encoding/json"
	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/reflectx"
	"github.com/apache/beam/sdks/go/pkg/beam/typex"
	"log"
	"reflect"
)

// Coder represents a coder.
type Coder struct {
	coder *graph.Coder
}

// TODO(herohde) 4/4/2017: for convenience, we use the magic json coding
// everywhere. To be replaced by Coder registry, sharing, etc.

func NewCoder(t reflect.Type) Coder {
	inner, err := inferCoder(t)
	if err != nil {
		panic(err) // for now
	}

	c := &graph.Coder{
		Kind:       graph.WindowedValue,
		Components: []*graph.Coder{inner},
		Window:     &graph.Window{Kind: graph.GlobalWindow},
	}
	return Coder{c}
}

func inferCoder(t reflect.Type) (*graph.Coder, error) {
	if k, v, ok := reflectx.UnfoldComposite(t); ok {
		key, err := inferCoder(k)
		if err != nil {
			return nil, err
		}
		value, err := inferCoder(v)
		if err != nil {
			return nil, err
		}
		return &graph.Coder{Kind: graph.Pair, T: t, Components: []*graph.Coder{key, value}}, nil
	}

	if t.Kind() == reflect.Chan {
		elm, err := inferCoder(t.Elem())
		if err != nil {
			return nil, err
		}
		return &graph.Coder{Kind: graph.Stream, T: t, Components: []*graph.Coder{elm}}, nil
	}

	return &graph.Coder{Kind: graph.LengthPrefix, T: t, Components: []*graph.Coder{{
		Kind: graph.Custom, T: t, Custom: NewCustomCoder(t),
	}}}, nil
}

// TODO(herohde) 4/5/2017: decide whether we want an Encoded form. For now,
// we'll use exploded form coders only using typex.T. It seems too cumbersome
// to to allow channel form for coders. We might also need a form that doesn't
// require LengthPrefix'ing to cut up the bytestream from the FnHarness.

// Concrete and universal coders both have a similar signature. Conversion is
// handled by reflection.

type jsonContext struct {
	T graph.DataType `beam:"data"`
}

func jsonEnc(_ jsonContext, in typex.T) ([]byte, error) {
	return json.Marshal(in)
}

func jsonDec(ctx jsonContext, in []byte) (typex.T, error) {
	val := reflect.New(ctx.T.T)
	if err := json.Unmarshal(in, val.Interface()); err != nil {
		return nil, err
	}
	return val.Elem().Interface(), nil
}

// TODO: select optimal coder based on type, notably handling int, string, etc.

func NewCustomCoder(t reflect.Type) *graph.CustomCoder {
	if reflectx.ClassOf(t) != reflectx.Concrete {
		log.Fatalf("Type must be concrete: %v", t)
	}

	coder, err := graph.NewCustomCoder("json", t, jsonEnc, jsonDec, graph.DataType{t})
	if err != nil {
		log.Fatalf("Bad coder: %v", err)
	}
	return coder
}
