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

	ret, err := TryParDo(p, fn, imp, TypeDefinition{Var: TType, T: t})
	if err != nil || len(ret) != 1 {
		panic(fmt.Sprintf("internal error: %v", err))
	}
	return ret[0], nil
}

// TODO(herohde) 6/26/2017: make 'create' a SDF once supported. See BEAM-2421.

type createFn struct {
	Values []interface{} `json:"values"`
}

func (c *createFn) ProcessElement(_ []byte, emit func(T)) {
	for _, value := range c.Values {
		emit(value)
	}
}
