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
	"bytes"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// Create inserts a fixed non-empty set of values into the pipeline. The values must
// be of the same type 'A' and the returned PCollection is of type A.
//
// The returned PCollections can be used as any other PCollections. The values
// are JSON-coded. Each runner may place limits on the sizes of the values and
// Create should generally only be used for small collections.
func Create(s Scope, values ...interface{}) PCollection {
	return Must(TryCreate(s, values...))
}

// CreateList inserts a fixed set of values into the pipeline from a slice or
// array. Unlike Create this supports the creation of an empty PCollection.
func CreateList(s Scope, list interface{}) PCollection {
	val := reflect.ValueOf(list)
	if val.Kind() != reflect.Slice && val.Kind() != reflect.Array {
		panic(fmt.Sprintf("Input %v must be a slice or array", list))
	}
	var ret []interface{}
	for i := 0; i < val.Len(); i++ {
		ret = append(ret, val.Index(i).Interface())
	}
	var t reflect.Type
	if len(ret) == 0 {
		t = reflect.TypeOf(list).Elem()
	} else {
		t = reflect.ValueOf(ret[0]).Type()
	}
	return Must(TryCreateList(s, ret, t))
}

func addCreateCtx(err error, s Scope) error {
	return errors.WithContextf(err, "inserting Create in scope %s", s)
}

// TryCreate inserts a fixed non-empty set of values into the pipeline. The
// values must be of the same type.
func TryCreate(s Scope, values ...interface{}) (PCollection, error) {
	if len(values) == 0 {
		return PCollection{}, addCreateCtx(errors.New("create has no values"), s)
	}

	t := reflect.ValueOf(values[0]).Type()
	return TryCreateList(s, values, t)
}

// TryCreateList inserts a fixed set of values into the pipeline from a slice or
// array. The values must be of the same type. Unlike TryCreate this supports
// the creation of an empty PCollection.
func TryCreateList(s Scope, values []interface{}, t reflect.Type) (PCollection, error) {
	fn := &createFn{Type: EncodedType{T: t}}
	enc := NewElementEncoder(t)

	for i, value := range values {
		if other := reflect.ValueOf(value).Type(); other != t {
			err := errors.Errorf("value %v at index %v has type %v, want %v", value, i, other, t)
			return PCollection{}, addCreateCtx(err, s)
		}
		var buf bytes.Buffer
		if err := enc.Encode(value, &buf); err != nil {
			return PCollection{}, addCreateCtx(errors.Wrapf(err, "marshalling of %v failed", value), s)
		}
		fn.Values = append(fn.Values, buf.Bytes())
	}

	imp := Impulse(s)

	ret, err := TryParDo(s, fn, imp, TypeDefinition{Var: TType, T: t})
	if err != nil || len(ret) != 1 {
		panic(addCreateCtx(errors.WithContext(err, "internal error"), s))
	}
	return ret[0], nil
}

// TODO(herohde) 6/26/2017: make 'create' a SDF once supported. See BEAM-2421.

type createFn struct {
	Values [][]byte    `json:"values"`
	Type   EncodedType `json:"type"`
}

func (c *createFn) ProcessElement(_ []byte, emit func(T)) error {
	dec := NewElementDecoder(c.Type.T)
	for _, val := range c.Values {
		element, err := dec.Decode(bytes.NewBuffer(val))
		if err != nil {
			return err
		}
		emit(element)
	}
	return nil
}
