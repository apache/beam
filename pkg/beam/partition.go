package beam

import (
	"fmt"
	"reflect"

	"encoding/json"

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

var (
	sig = &funcx.Signature{Args: []reflect.Type{typex.TType}, Return: []reflect.Type{reflectx.Int}} // T -> int
)

// Partition takes a PCollection<T> and a PartitionFn, uses the PartitionFn to
// split the elements of the input PCollection into N partitions, and returns
// a []PCollection<T> that bundles N PCollection<T>s containing the split elements.
func Partition(p *Pipeline, n int, fn interface{}, col PCollection) []PCollection {
	p = p.Composite(fmt.Sprintf("Partition(%v)", n))

	if n < 1 {
		panic(fmt.Sprintf("n must be > 0"))
	}
	t := typex.SkipW(col.Type()).Type()
	funcx.MustSatisfy(fn, funcx.Replace(sig, typex.TType, t))

	// The partitionFn is a DoFn with a signature that is dependent on the input, so
	// neither reflection nor type-specialization is adequate. Instead, it uses a
	// dynamic function.

	emit := reflect.FuncOf([]reflect.Type{typex.EventTimeType, t}, nil, false)
	in := []reflect.Type{typex.EventTimeType, t}
	for i := 0; i < n; i++ {
		in = append(in, emit)
	}
	fnT := reflect.FuncOf(in, []reflect.Type{reflectx.Error}, false)

	data, err := json.Marshal(partitionData{N: n, Fn: EncodedFn{Fn: reflect.ValueOf(fn)}})
	if err != nil {
		panic(fmt.Sprintf("failed to encode partition function: %v", err))
	}

	return ParDoN(p, &graph.DynFn{Name: "beam.partitionFn", Data: data, T: fnT, Gen: makePartitionFn}, col)
}

// partitionData contains the data needed for the partition DoFn generator.
type partitionData struct {
	N  int       `json:"n"`
	Fn EncodedFn `json:"fn"`
}

// makePartitionFn returns a closure with the following underlying type:
//
//     fn : (EventTime, T, emit_1, emit_2, ..., emit_N) -> error
//
// where emit_i : (EventTime, T) -> () and N is given by the encoded
// partitionData value.
func makePartitionFn(enc []byte) func([]reflect.Value) []reflect.Value {
	var data partitionData
	if err := json.Unmarshal(enc, &data); err != nil {
		panic(fmt.Sprintf("failed to unmarshal partitionFn data: %v", err))
	}
	return func(args []reflect.Value) []reflect.Value {
		timestamp := args[0]
		value := args[1]

		n := data.Fn.Fn.Call([]reflect.Value{value})[0].Interface().(int)
		if n < 0 || n >= data.N {
			ret := reflect.ValueOf(fmt.Errorf("partitionFn(%v) = %v, want [0,%v)", value.Interface(), n, data.N))
			return []reflect.Value{ret.Convert(reflectx.Error)} // convert to error interface
		}

		emit := args[n+2]
		emit.Call([]reflect.Value{timestamp, value})
		return []reflect.Value{reflect.Zero(reflectx.Error)}
	}
}
