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
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/graphx/schema"
)

// RegisterSchemaProvider allows pipeline authors to provide special handling
// to convert types to schema representations, when those types are used as
// fields in types being encoded as schema rows.
//
// At present, the only supported provider interface is SchemaProvider,
// though this may change in the future.
//
// Providers only need to support a limited set of types for conversion,
// specifically a single struct type or a pointer to struct type,
// or an interface type, which they are registered with.
//
// Providers have three tasks with respect to a given supported logical type:
//
//   - Producing schema representative types for their logical types.
//   - Producing schema encoders for values of that type, writing beam
//     schema encoded bytes for a value, matching the schema representative type.
//   - Producing schema decoders for values of that type, reading beam
//     schema encoded bytes, and producing a value of that type.
//
// Representative Schema types must be structs with only exported fields.
//
// A provider should be thread safe, but it's not required that a produced
// encoder or decoder is thread safe, since a separate encoder or decoder
// will be used for simultaneously executed bundles.
//
// If the supported type is an interface, that interface must have a non-empty
// method set. That is, it cannot be the empty interface.
//
// RegisterSchemaProvider must be called before beam.Init(), and conventionally
// is called in a package init() function.
func RegisterSchemaProvider(rt reflect.Type, provider any) {
	p := provider.(SchemaProvider)
	switch rt.Kind() {
	case reflect.Interface:
		schema.RegisterLogicalTypeProvider(rt, p.FromLogicalType)
	case reflect.Ptr:
		if rt.Elem().Kind() != reflect.Struct {
			panic(fmt.Sprintf("beam.RegisterSchemaProvider: unsupported type kind for schema provider %v is a %v, must be interface, struct or *struct.", rt, rt.Kind()))
		}
		fallthrough
	case reflect.Struct:
		st, err := p.FromLogicalType(rt)
		if err != nil {
			panic(fmt.Sprintf("beam.RegisterSchemaProvider: schema type provider for %v, doesn't support that type", rt))
		}
		schema.RegisterLogicalType(schema.ToLogicalType(rt.Name(), rt, st))
	default:
		panic(fmt.Sprintf("beam.RegisterSchemaProvider: unsupported type kind for schema provider %v is a %v, must be interface, struct or *struct.", rt, rt.Kind()))
	}

	coder.RegisterSchemaProviders(rt, p.BuildEncoder, p.BuildDecoder)
}

// RegisterSchemaProviderWithURN is for internal use only. Users are recommended to use
// beam.RegisterSchemaProvider() instead.
// RegisterSchemaProviderWithURN registers a new schema provider for a new logical type defined
// in pkg/beam/model/pipeline_v1/schema.pb.go
//
// RegisterSchemaProviderWithURN must be called before beam.Init(), and conventionally
// is called in a package init() function.
func RegisterSchemaProviderWithURN(rt reflect.Type, provider any, urn string) {
	p := provider.(SchemaProvider)
	st, err := p.FromLogicalType(rt)
	if err != nil {
		panic(fmt.Sprintf("beam.RegisterSchemaProvider: schema type provider for %v, doesn't support that type", rt))
	}
	schema.RegisterLogicalType(schema.ToLogicalType(urn, rt, st))
	coder.RegisterSchemaProviders(rt, p.BuildEncoder, p.BuildDecoder)
}

// SchemaProvider specializes schema handling for complex types, including conversion to a
// valid schema base type,
//
// In particular, they are intended to handle schema for interface types.
//
// Sepearated out the acting type from the provider implementation is good.
type SchemaProvider interface {
	FromLogicalType(reflect.Type) (reflect.Type, error)
	BuildEncoder(rt reflect.Type) (func(any, io.Writer) error, error)
	BuildDecoder(rt reflect.Type) (func(io.Reader) (any, error), error)
}
