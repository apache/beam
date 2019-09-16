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

package reflectx

import (
	"fmt"
	"reflect"
	"runtime"
	"unsafe"
)

// TODO(herohde) 7/21/2017: we rely on the happy fact that the function name is
// also the symbol name. We should perhaps make that connection explicit.

// FunctionName returns the symbol name of a function. It panics if the given
// value is not a function.
func FunctionName(fn interface{}) string {
	val := reflect.ValueOf(fn)
	if val.Kind() != reflect.Func {
		panic(fmt.Sprintf("value %v is not a function", fn))
	}

	return runtime.FuncForPC(uintptr(val.Pointer())).Name()
}

// LoadFunction loads a function from a pointer and type. Assumes the pointer
// points to a valid function implementation.
func LoadFunction(ptr uintptr, t reflect.Type) interface{} {
	v := reflect.New(t).Elem()
	p := new(uintptr)
	*p = ptr
	*(*unsafe.Pointer)(unsafe.Pointer(v.Addr().Pointer())) = unsafe.Pointer(p)
	return v.Interface()
}
