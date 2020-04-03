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
	"reflect"
	"testing"
)

func testFunction() int64 {
	return 42
}

func TestLoadFunction(t *testing.T) {
	val := reflect.ValueOf(testFunction)
	fi := uintptr(val.Pointer())
	typ := val.Type()

	callable := LoadFunction(fi, typ)

	cv := reflect.ValueOf(callable)
	out := cv.Call(nil)
	if len(out) != 1 {
		t.Errorf("got %d return values, wanted 1.", len(out))
	}

	if out[0].Int() != testFunction() {
		t.Errorf("got %d, wanted %d", out[0].Int(), testFunction())
	}
}
