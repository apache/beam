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

func TestTestStreamStrings(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamStrings)
}

func TestTestStreamByteSliceSequence(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamByteSliceSequence)
}

func TestTestStreamInt64Sequence(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamInt64Sequence)
}

func TestTestStreamInt16Sequence(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamInt16Sequence)
}

func TestTestStreamTwoInt64Sequences(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamTwoInt64Sequences)
}

func TestTestStreamFloat64Sequence(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamFloat64Sequence)
}

func TestTestStreamTwoFloat64Sequences(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamTwoFloat64Sequences)
}

func TestTestStreamBoolSequence(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamBoolSequence)
}

func TestTestStreamTwoBoolSequences(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamTwoBoolSequences)
}

func TestTestStreamTwoUserTypeSequences(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamTwoUserTypeSequences)
}

func TestTestStreamSimple(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamSimple)
}

func TestTestStreamSimple_InfinityDefault(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamSimple_InfinityDefault)
}

func TestTestStreamToGBK(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamToGBK)
}

func TestTestStreamTimersEventTime(t *testing.T) {
	integration.CheckFilters(t)
	ptest.BuildAndRun(t, TestStreamTimersEventTime)
}
