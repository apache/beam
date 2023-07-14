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

package internal_test

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
)

// This file covers pipelines with stateful DoFns, in particular, that they
// use the state and timers APIs.

func TestStateful(t *testing.T) {
	initRunner(t)

	tests := []struct {
		pipeline func(s beam.Scope)
		metrics  func(t *testing.T, pr beam.PipelineResult)
	}{
		//{pipeline: primitives.BagStateParDo},
	}

	for _, test := range tests {
		t.Run(intTestName(test.pipeline), func(t *testing.T) {
			p, s := beam.NewPipelineWithRoot()
			test.pipeline(s)
			pr, err := executeWithT(context.Background(), t, p)
			if err != nil {
				t.Fatal(err)
			}
			if test.metrics != nil {
				test.metrics(t, pr)
			}
		})
	}
}
