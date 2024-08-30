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

package primitives

import (
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"github.com/apache/beam/sdks/v2/go/test/integration"
)

func TestValueState(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, ValueStateParDo)
}

func TestValueStateWindowed(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, ValueStateParDoWindowed)
}

func TestValueStateClear(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, ValueStateParDoClear)
}

func TestBagState(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, BagStateParDo)
}

func TestBagStateClear(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, BagStateParDoClear)
}

func TestCombiningState(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, CombiningStateParDo)
}

func TestMapState(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, MapStateParDo)
}

func TestMapStateClear(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, MapStateParDoClear)
}

func TestSetState(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, SetStateParDo)
}

func TestSetStateClear(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, SetStateParDoClear)
}
