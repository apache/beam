package beam

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/graph/window"
	"github.com/apache/beam/sdks/go/pkg/beam/util/reflectx"
)

func init() {
	RegisterType(reflect.TypeOf((*create)(nil)).Elem())
}

// Create inserts a fixed set of values into the pipeline. The values must
// be of the same type 'A' and the returned PCollection is of type W<A>.
// For example:
//
//    foo := beam.Create(p, "a", "b", "c")  // foo : W<string>
//    bar := beam.Create(p, 1, 2, 3)        // bar : W<int>
//
// The returned PCollections can be used as any other PCollections. The values
// are JSON-coded. Each runner may place limits on the sizes of the values and
// Create should generally only be used for small collections.
func Create(p *Pipeline, values ...interface{}) PCollection {
	return Must(TryCreate(p, values...))
}

// TryCreate inserts a fixed set of values into the pipeline. The values must
// be of the same type.
func TryCreate(p *Pipeline, values ...interface{}) (PCollection, error) {
	if len(values) == 0 {
		return PCollection{}, fmt.Errorf("create has no values")
	}

	fn := &create{}
	t := reflect.ValueOf(values[0]).Type()
	for i, value := range values {
		if other := reflect.ValueOf(value).Type(); other != t {
			return PCollection{}, fmt.Errorf("value %v at index %v has type %v, want %v", value, i, other, t)
		}
		fn.Values = append(fn.Values, value)
	}

	dofn, err := graph.NewDoFn(fn)
	if err != nil {
		panic(fmt.Sprintf("internal error: %v", err))
	}

	// (1) Insert empty impulse as trigger.

	imp := graph.NewImpulse(p.real, p.parent, []byte{})

	// (2) Insert the create ParDo manually, because the return type cannot be
	// inferred from the signature.

	n := p.real.NewNode(typex.NewW(typex.New(t)), window.NewGlobalWindow())

	edge := p.real.NewEdge(p.parent)
	edge.Op = graph.ParDo
	edge.DoFn = dofn
	edge.Input = []*graph.Inbound{{Kind: graph.Main, From: imp.Output[0].To, Type: typex.NewW(typex.New(reflectx.ByteSlice))}}
	edge.Output = []*graph.Outbound{{To: n, Type: typex.NewW(typex.New(typex.TType))}}

	ret := PCollection{edge.Output[0].To}
	ret.SetCoder(NewCoder(ret.Type()))
	return ret, nil
}

// TODO(herohde) 6/26/2017: make 'create' a SDF once supported. See BEAM-2421.

type create struct {
	Values []interface{} `json:"values"`
}

func (c *create) ProcessElement(_ []byte, emit func(typex.T)) {
	for _, value := range c.Values {
		emit(value)
	}
}
