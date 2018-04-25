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

	"github.com/apache/beam/sdks/go/pkg/beam/core/funcx"
	"github.com/apache/beam/sdks/go/pkg/beam/core/graph"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
)

func init() {
	RegisterFunction(makePartitionFn)
}

var (
	sig = &funcx.Signature{Args: []reflect.Type{TType}, Return: []reflect.Type{reflectx.Int}} // T -> int
)

// Partition takes a PCollection<T> and a PartitionFn, uses the PartitionFn to
// split the elements of the input PCollection into N partitions, and returns
// a []PCollection<T> that bundles N PCollection<T>s containing the split elements.
func Partition(s Scope, n int, fn interface{}, col PCollection) []PCollection {
	s = s.Scope(fmt.Sprintf("Partition(%v)", n))

	if n < 1 {
		panic(fmt.Sprintf("n must be > 0"))
	}
	t := col.Type().Type()
	funcx.MustSatisfy(fn, funcx.Replace(sig, TType, t))

	// The partitionFn is a DoFn with a signature that is dependent on the input, so
	// neither reflection nor type-specialization is adequate. Instead, it uses a
	// dynamic function.

	emit := reflect.FuncOf([]reflect.Type{EventTimeType, t}, nil, false)
	in := []reflect.Type{EventTimeType, t}
	for i := 0; i < n; i++ {
		in = append(in, emit)
	}
	fnT := reflect.FuncOf(in, []reflect.Type{reflectx.Error}, false)

	data, err := json.Marshal(partitionData{N: n, Fn: EncodedFunc{Fn: reflectx.MakeFunc(fn)}})
	if err != nil {
		panic(fmt.Sprintf("failed to encode partition function: %v", err))
	}

	return ParDoN(s, &graph.DynFn{Name: "beam.partitionFn", Data: data, T: fnT, Gen: makePartitionFn}, col)
}

// partitionData contains the data needed for the partition DoFn generator.
type partitionData struct {
	N  int         `json:"n"`
	Fn EncodedFunc `json:"fn"`
}

// partitionFn is a Func with the following underlying type:
//
//     fn : (EventTime, T, emit_1, emit_2, ..., emit_N) -> error
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

func (f *partitionFn) Call(args []interface{}) []interface{} {
	timestamp := args[0]
	value := args[1]

	n := f.fn.Call1x1(value).(int)
	if n < 0 || n >= f.n {
		return []interface{}{fmt.Errorf("partitionFn(%v) = %v, want [0,%v)", value, n, f.n)}
	}

	emit := args[n+2]
	reflectx.MakeFunc2x0(emit).Call2x0(timestamp, value)

	var err error
	return []interface{}{err}
}

func makePartitionFn(name string, t reflect.Type, enc []byte) reflectx.Func {
	var data partitionData
	if err := json.Unmarshal(enc, &data); err != nil {
		panic(fmt.Sprintf("failed to unmarshal partitionFn data: %v", err))
	}
	return &partitionFn{
		name: name,
		t:    t,
		n:    data.N,
		fn:   reflectx.ToFunc1x1(data.Fn.Fn),
	}
}
