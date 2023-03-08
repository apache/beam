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

// Package python contains data structures required for python external transforms in a multilanguage pipeline.
package python

import (
	"fmt"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

const (
	pythonCallableUrn = "beam:logical_type:python_callable:v1"
	// ExpansionServiceModule is the module containing the python expansion service for python external transforms.
	ExpansionServiceModule = "apache_beam.runners.portability.expansion_service_main"
)

var (
	pcsType        = reflect.TypeOf((*CallableSource)(nil)).Elem()
	pcsStorageType = reflectx.String
)

func init() {
	beam.RegisterType(pcsType)
	beam.RegisterSchemaProviderWithURN(pcsType, &callableSourceProvider{}, pythonCallableUrn)
}

// CallableSource is a wrapper object storing a Python function definition
// that can be evaluated to Python callables in Python SDK.
//
// The snippet of Python code can be a valid Python expression such as
//
//	   lambda x: x * x
//		  str.upper
//
// a fully qualified name such as
//
//	math.sin
//
// or a complete multi-line function or class definition such as
//
//	   def foo(x):
//		   ...
//	   class Foo:
//		   ...
//
// Any lines preceding the function definition are first evaluated to provide context in which to
// define the function which can be useful to declare imports or any other needed values, e.g.
//
//	import math
//
//	def helper(x):
//	    return x * x
//
//	def func(y):
//	    return helper(y) + y
//
// in which case `func` would get applied to each element.
type CallableSource string

// callableSourceProvider implement the SchemaProvider interface for logical types
type callableSourceProvider struct{}

// FromLogicalType returns the goType of the logical type
func (p *callableSourceProvider) FromLogicalType(rt reflect.Type) (reflect.Type, error) {
	if rt != pcsType {
		return nil, fmt.Errorf("unable to provide schema.LogicalType for type %v, want %v", rt, pcsType)
	}
	return pcsStorageType, nil
}

// BuildEncoder encodes the PythonCallableSource logical type
func (p *callableSourceProvider) BuildEncoder(rt reflect.Type) (func(any, io.Writer) error, error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}

	return func(iface any, w io.Writer) error {
		v := iface.(CallableSource)
		return coder.EncodeStringUTF8(string(v), w)
	}, nil
}

// BuildDecoder decodes the PythonCallableSource logical type
func (p *callableSourceProvider) BuildDecoder(rt reflect.Type) (func(io.Reader) (any, error), error) {
	if _, err := p.FromLogicalType(rt); err != nil {
		return nil, err
	}

	return func(r io.Reader) (any, error) {
		s, err := coder.DecodeStringUTF8(r)
		if err != nil {
			return nil, err
		}
		return CallableSource(s), nil
	}, nil
}

// NewExternalTransform creates a new instance for python external transform. It accepts two types:
// A: used for normal arguments
// K: used for keyword arguments
func NewExternalTransform[A, K any](constructor string) *pythonExternalTransform[A, K] {
	return &pythonExternalTransform[A, K]{Constructor: constructor}
}

// PythonExternalTransform holds the details required for an External Python Transform.
type pythonExternalTransform[A, K any] struct {
	Constructor string `beam:"constructor"`
	Args        A      `beam:"args"`
	Kwargs      K      `beam:"kwargs"`
}

// WithArgs adds arguments to the External Python Transform.
func (p *pythonExternalTransform[A, K]) WithArgs(args any) {
	p.Args = args.(A)
}

// WithKwargs adds keyword arguments to the External Python Transform.
func (p *pythonExternalTransform[A, K]) WithKwargs(kwargs any) {
	p.Kwargs = kwargs.(K)
}
