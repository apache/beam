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

package spannerio

import (
	"reflect"
	"testing"
)

type TestDto struct {
	One string `spanner:"one"`
	Two int    `spanner:"two"`
}

func TestColumnsFromStructReturnsColumns(t *testing.T) {
	// arrange
	// act
	cols := columnsFromStruct(reflect.TypeOf(TestDto{}))

	// assert
	if len(cols) != 2 {
		t.Fatalf("got %v columns, expected 2", len(cols))
	}
}
