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

	"github.com/apache/beam/sdks/go/pkg/beam/core/typex"
)

// ValidateKVType panics if the type of the PCollection is not KV<A,B>.
// It returns (A,B).
func ValidateKVType(col PCollection) (typex.FullType, typex.FullType) {
	t := col.Type()
	if !typex.IsKV(t) {
		panic(fmt.Sprintf("pcollection must be of KV type: %v", col))
	}
	return t.Components()[0], t.Components()[1]
}

// ValidateConcreteType panics if the type of the PCollection is not a
// composite type. It returns the type.
func ValidateNonCompositeType(col PCollection) typex.FullType {
	t := col.Type()
	if typex.IsComposite(t.Type()) {
		panic(fmt.Sprintf("pcollection must be of non-composite type: %v", col))
	}
	return t
}

// validate validates and processes the input collection and options. Private convenience
// function.
func validate(s Scope, col PCollection, opts []Option) ([]SideInput, map[string]reflect.Type, error) {
	if !s.IsValid() {
		return nil, nil, fmt.Errorf("invalid scope")
	}
	if !col.IsValid() {
		return nil, nil, fmt.Errorf("invalid main pcollection")
	}
	side, defs := parseOpts(opts)
	for i, in := range side {
		if !in.Input.IsValid() {
			return nil, nil, fmt.Errorf("invalid side pcollection: index %v", i)
		}
	}
	typedefs, err := makeTypedefs(defs)
	if err != nil {
		return nil, nil, err
	}
	return side, typedefs, nil
}

func makeTypedefs(list []TypeDefinition) (map[string]reflect.Type, error) {
	typedefs := make(map[string]reflect.Type)
	for _, v := range list {
		if !typex.IsUniversal(v.Var) {
			return nil, fmt.Errorf("type var %s must be a universal type", v.Var)
		}
		if !typex.IsConcrete(v.T) {
			return nil, fmt.Errorf("type value %s must be a concrete type", v.T)
		}
		typedefs[v.Var.Name()] = v.T
	}
	return typedefs, nil
}
