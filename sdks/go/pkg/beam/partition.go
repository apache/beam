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

package beam

import (
	"fmt"
	"reflect"

	"encoding/json"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
)

var (
	sig   = &funcx.Signature{Args: []reflect.Type{TType}, Return: []reflect.Type{reflectx.Int}}        // T -> int
	sigKV = &funcx.Signature{Args: []reflect.Type{TType, UType}, Return: []reflect.Type{reflectx.Int}} // KV<T, U> -> int
)

// Partition takes a PCollection<T> and a PartitionFn, uses the PartitionFn to
// split the elements of the input PCollection into N partitions, and returns
// a []PCollection<T> that bundles N PCollection<T>s containing the split elements.
//
// A PartitionFn has the signature `func(T) int.`
//
//	func lenToTen(s string) int {
//		if len(s) > 9 {
//			return 10
//		}
//		return len(s)
//	}
//
//	// Partition functions must be registered with Beam, and must not be closures.
//	func init() { register.Function1x1(lenToTen) }
//
//	// The number of partitions goes up to 11 since we can return 0 through 10
//	wordsByLength := beam.Partition(s, 11, lenToTen, inputStrings)
//
// T is permitted to be a KV.
func Partition(s Scope, n int, fn any, col PCollection) []PCollection {
	s = s.Scope(fmt.Sprintf("Partition(%v)", n))

	if n < 1 {
		panic("n must be > 0")
	}
	var emit reflect.Type
	var in []reflect.Type
	if typex.IsKV(col.Type()) {
		comps := col.Type().Components()
		k, v := comps[0].Type(), comps[1].Type()
		funcx.MustSatisfy(fn, funcx.Replace(funcx.Replace(sigKV, TType, k), UType, v))
		emit = reflect.FuncOf([]reflect.Type{EventTimeType, k, v}, nil, false)
		in = []reflect.Type{EventTimeType, k, v}
	} else {
		t := col.Type().Type()
		funcx.MustSatisfy(fn, funcx.Replace(sig, TType, t))
		emit = reflect.FuncOf([]reflect.Type{EventTimeType, t}, nil, false)
		in = []reflect.Type{EventTimeType, t}
	}

	// The partitionFn is a DoFn with a signature that is dependent on the input, so
	// neither reflection nor type-specialization is adequate. Instead, it uses a
	// dynamic function.
	for i := 0; i < n; i++ {
		in = append(in, emit)
	}
	fnT := reflect.FuncOf(in, []reflect.Type{reflectx.Error}, false)

	data, err := json.Marshal(partitionData{KV: typex.IsKV(col.Type()), N: n, Fn: EncodedFunc{Fn: reflectx.MakeFunc(fn)}})
	if err != nil {
		panic(errors.WithContext(err, "encoding partition function"))
	}

	return ParDoN(s, &graph.DynFn{Name: "beam.partitionFn", Data: data, T: fnT, Gen: makePartitionFn}, col)
}

// partitionData contains the data needed for the partition DoFn generator.
type partitionData struct {
	KV bool        `json:"kv"`
	N  int         `json:"n"`
	Fn EncodedFunc `json:"fn"`
}

// partitionFn is a Func with the following underlying type:
//
//	fn : (EventTime, T, emit_1, emit_2, ..., emit_N) -> error
//
// where emit_i : (EventTime, T) -> () and N is given by the encoded
// partitionData value. For any input element, it invokes to the
// given partition function to determine which emitter to use.
type partitionFn struct {
	name string
	t    reflect.Type
	n    int
	fn   reflectx.Func1x1
}

func (f *partitionFn) Name() string {
	return f.name
}

func (f *partitionFn) Type() reflect.Type {
	return f.t
}

func (f *partitionFn) Call(args []any) []any {
	timestamp := args[0]
	value := args[1]

	n := f.fn.Call1x1(value).(int)
	if n < 0 || n >= f.n {
		return []any{errors.Errorf("partitionFn(%v) = %v, want [0,%v)", value, n, f.n)}
	}

	emit := args[n+2]
	reflectx.MakeFunc2x0(emit).Call2x0(timestamp, value)

	var err error
	return []any{err}
}

// partitionFnKV is a Func with the following underlying type:
//
//	fn : (EventTime, K, V, emit_1, emit_2, ..., emit_N) -> error
//
// where emit_i : (EventTime, K, V) -> () and N is given by the encoded
// partitionData value. For any input element, it invokes to the
// given partition function to determine which emitter to use.
type partitionFnKV struct {
	name string
	t    reflect.Type
	n    int
	fnKV reflectx.Func2x1
}

func (f *partitionFnKV) Name() string {
	return f.name
}

func (f *partitionFnKV) Type() reflect.Type {
	return f.t
}

func (f *partitionFnKV) Call(args []any) []any {
	timestamp := args[0]
	key := args[1]
	value := args[2]

	n := f.fnKV.Call2x1(key, value).(int)
	if n < 0 || n >= f.n {
		return []any{errors.Errorf("partitionFn(%v) = %v, want [0,%v)", value, n, f.n)}
	}

	emit := args[n+3]
	reflectx.MakeFunc3x0(emit).Call3x0(timestamp, key, value)

	var err error
	return []any{err}
}

func makePartitionFn(name string, t reflect.Type, enc []byte) reflectx.Func {
	var data partitionData
	if err := json.Unmarshal(enc, &data); err != nil {
		panic(errors.WithContext(err, "unmarshalling partitionFn data"))
	}
	if data.KV {
		return &partitionFnKV{
			name: name,
			t:    t,
			n:    data.N,
			fnKV: reflectx.ToFunc2x1(data.Fn.Fn),
		}
	}
	return &partitionFn{
		name: name,
		t:    t,
		n:    data.N,
		fn:   reflectx.ToFunc1x1(data.Fn.Fn),
	}
}
