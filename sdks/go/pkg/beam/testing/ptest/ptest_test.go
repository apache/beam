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

package ptest

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
)

func TestMain(m *testing.M) {
	Main(m)
}

func TestCreate(t *testing.T) {
	inputs := []any{"a", "b", "c"}
	p, s, col := Create(inputs)
	passert.EqualsList(s, col, inputs)
	if err := Run(p); err != nil {
		t.Errorf("Run(p) returned error %v, want nil", err)
	}
}

func TestCreateList(t *testing.T) {
	inputs := []string{"a", "b", "c"}
	p, s, col := CreateList(inputs)
	passert.EqualsList(s, col, inputs)
	if err := Run(p); err != nil {
		t.Errorf("Run(p) returned error %v, want nil", err)
	}
}

func TestCreate2(t *testing.T) {
	inputOne := []any{"a", "b", "c"}
	inputTwo := []any{"d", "e", "f", "g"}
	p, s, colOne, colTwo := Create2(inputOne, inputTwo)
	passert.EqualsList(s, colOne, inputOne)
	passert.EqualsList(s, colTwo, inputTwo)
	if err := Run(p); err != nil {
		t.Errorf("Run(p) returned error %v, want nil", err)
	}
}

func TestCreateList2(t *testing.T) {
	inputOne := []string{"a", "b", "c"}
	inputTwo := []string{"d", "e", "f", "g"}
	p, s, colOne, colTwo := CreateList2(inputOne, inputTwo)
	passert.EqualsList(s, colOne, inputOne)
	passert.EqualsList(s, colTwo, inputTwo)
	if err := Run(p); err != nil {
		t.Errorf("Run(p) returned error %v, want nil", err)
	}
}
