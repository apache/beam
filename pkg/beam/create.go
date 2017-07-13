package beam

import (
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

func init() {
	RegisterType(reflect.TypeOf((*createFn)(nil)).Elem())
}

// TODO(herohde) 7/11/2017: add variants that use coder encoding.

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

// CreateList inserts a fixed set of values into the pipeline from a slice or
// array. It is a convenience wrapper over Create. For example:
//
//    list := []string{"a", "b", "c"}
//    foo := beam.CreateList(p, list)  // foo : W<string>
func CreateList(p *Pipeline, list interface{}) PCollection {
	var ret []interface{}
	val := reflect.ValueOf(list)
	if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
		panic(fmt.Sprintf("Input %v must be a slice or array", list))
	}
	for i := 0; i < val.Len(); i++ {
		ret = append(ret, val.Index(i).Interface())
	}
	return Must(TryCreate(p, ret...))
}

// TryCreate inserts a fixed set of values into the pipeline. The values must
// be of the same type.
func TryCreate(p *Pipeline, values ...interface{}) (PCollection, error) {
	if len(values) == 0 {
		return PCollection{}, fmt.Errorf("create has no values")
	}

	fn := &createFn{}
	t := reflect.ValueOf(values[0]).Type()
	for i, value := range values {
		if other := reflect.ValueOf(value).Type(); other != t {
			return PCollection{}, fmt.Errorf("value %v at index %v has type %v, want %v", value, i, other, t)
		}
		fn.Values = append(fn.Values, value)
	}

	imp := Impulse(p)

	ret, err := TryParDo(p, fn, imp, TypeDefinition{Var: typex.TType, T: t})
	if err != nil || len(ret) != 1 {
		panic(fmt.Sprintf("internal error: %v", err))
	}
	return ret[0], nil
}

// TODO(herohde) 6/26/2017: make 'create' a SDF once supported. See BEAM-2421.

type createFn struct {
	Values []interface{} `json:"values"`
}

func (c *createFn) ProcessElement(_ []byte, emit func(typex.T)) {
	for _, value := range c.Values {
		emit(value)
	}
}
