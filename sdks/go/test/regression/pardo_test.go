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

package regression

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/util/reflectx"
	"testing"

	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
)

func TestDirectParDo(t *testing.T) {
	if err := ptest.Run(DirectParDo()); err != nil {
		t.Error(err)
	}
}

func TestEmitParDo(t *testing.T) {
	if err := ptest.Run(EmitParDo()); err != nil {
		t.Error(err)
	}
}

func TestMultiEmitParDo(t *testing.T) {
	if err := ptest.Run(MultiEmitParDo()); err != nil {
		t.Error(err)
	}
}

func TestMixedOutputParDo(t *testing.T) {
	if err := ptest.Run(MixedOutputParDo()); err != nil {
		t.Error(err)
	}
}

func TestDirectParDoAfterGBK(t *testing.T) {
	if err := ptest.Run(DirectParDoAfterGBK()); err != nil {
		t.Error(err)
	}
}

func TestEmitParDoAfterGBK(t *testing.T) {
	if err := ptest.Run(EmitParDoAfterGBK()); err != nil {
		t.Error(err)
	}
}

// Keep the ParDo misalignment messages concise.
// FIXME: Review
// [beam-10169] identified that the error message returned to the user when the
// DoFn output is misaligned with the ParDo output was unclear. in order to
// make the user debug experience shorter, a more concise error message is
// required.
//
// This suite of brittle tests are to ensure that the returned errors are adhere
// to the ParDo API provided by the go sdk.

func TestRecommendParDoWithOneOutput(t *testing.T) {
	expected := reflectx.FunctionName(beam.ParDo)
	result := reflectx.FunctionName(beam.RecommendParDo(1))
	if expected != result {
		t.Errorf("want %v, got %v", expected, result)
	}
}

func TestRecommendParDoWithMoreThanSevenOutputs(t *testing.T) {
	expected := reflectx.FunctionName(beam.ParDoN)
	result := reflectx.FunctionName(beam.RecommendParDo(10))

	if expected != result {
		t.Errorf("want %v, got %v", expected, result)
	}
}

func TestRecommendParDoWithZeroOutputs(t *testing.T) {
	expected := reflectx.FunctionName(beam.ParDo0)
	result := reflectx.FunctionName(beam.RecommendParDo(0))

	if expected != result {
		t.Errorf("want %v, got %v", expected, result)
	}
}
