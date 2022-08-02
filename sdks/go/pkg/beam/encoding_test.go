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
	"reflect"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/coder/testutil"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
	"github.com/google/go-cmp/cmp"
)

type registeredTestType struct {
	A int
	B string
	_ float64
	D []byte
	// Currently Not encodable by default, so validates the registration.
	Foo [0]int
	Bar map[string]int
}

var registeredTestTypeType = reflect.TypeOf((*registeredTestType)(nil)).Elem()

func init() {
	RegisterType(registeredTestTypeType)
}

func TestEncodedType(t *testing.T) {
	var v testutil.SchemaCoder
	v.CmpOptions = []cmp.Option{
		cmp.Comparer(func(a reflect.Type, b reflect.Type) bool {
			return a.AssignableTo(b)
		}),
	}
	v.Validate(t, encodedTypeType, encodedTypeEnc, encodedTypeDec, struct{ Str string }{},
		[]EncodedType{
			{reflectx.String},
			{reflectx.Int},
			{reflectx.Int8},
			{reflectx.Int16},
			{reflectx.Int32},
			{reflectx.Int64},
			{reflectx.Uint},
			{reflectx.Uint8},
			{reflectx.Uint16},
			{reflectx.Uint32},
			{reflectx.Uint64},
			{reflectx.Float32},
			{reflectx.Float64},
			{reflectx.ByteSlice},
			{reflectx.Error},
			{reflectx.Bool},
			{reflectx.Context},

			{reflect.PtrTo(reflectx.String)},
			{reflect.PtrTo(reflectx.Int)},
			{reflect.PtrTo(reflectx.Int8)},
			{reflect.PtrTo(reflectx.Int16)},
			{reflect.PtrTo(reflectx.Int32)},
			{reflect.PtrTo(reflectx.Int64)},
			{reflect.PtrTo(reflectx.Uint)},
			{reflect.PtrTo(reflectx.Uint8)},
			{reflect.PtrTo(reflectx.Uint16)},
			{reflect.PtrTo(reflectx.Uint32)},
			{reflect.PtrTo(reflectx.Uint64)},
			{reflect.PtrTo(reflectx.Float32)},
			{reflect.PtrTo(reflectx.Float64)},
			{reflect.PtrTo(reflectx.ByteSlice)},
			{reflect.PtrTo(reflectx.Error)},
			{reflect.PtrTo(reflectx.Bool)},

			{reflect.ChanOf(reflect.BothDir, reflectx.Bool)},
			{reflect.ChanOf(reflect.RecvDir, reflectx.Float64)},
			{reflect.ChanOf(reflect.SendDir, reflectx.ByteSlice)},

			// Needs changes to Go SDK v1 type protos to support without registering.
			//	{reflect.ArrayOf(0, reflectx.Int)},
			//	{reflect.ArrayOf(7, reflectx.Int)},
			// {reflect.MapOf(reflectx.String, reflectx.ByteSlice)},

			{registeredTestTypeType},

			{reflect.FuncOf([]reflect.Type{reflectx.Context, reflectx.String, reflectx.Int}, []reflect.Type{reflectx.Error}, false)},

			{encodedTypeType},
			{encodedFuncType},
			{reflect.TypeOf((*struct{ A, B, C int })(nil))},
			{reflect.TypeOf((*struct{ A, B, C int })(nil)).Elem()},
		})
}

func registeredLocalTestFunction(_ string) int {
	return 0
}

func init() {
	// Functions must be registered anyway, in particular for unit tests.
	RegisterFunction(registeredLocalTestFunction)
}

func TestEncodedFunc(t *testing.T) {
	var v testutil.SchemaCoder
	v.CmpOptions = []cmp.Option{
		cmp.Comparer(func(a reflectx.Func, b reflectx.Func) bool {
			return a.Type() == b.Type() && a.Name() == b.Name()
		}),
	}
	v.Validate(t, encodedFuncType, encodedFuncEnc, encodedFuncDec, struct{ Str string }{},
		[]EncodedFunc{
			{reflectx.MakeFunc(registeredLocalTestFunction)},
		})
}
