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

package test

import (
	"beam.apache.org/learning/katas/core_transforms/branching/branching/pkg/task"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"testing"
)

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	tests := []struct {
		input        beam.PCollection
		wantReversed []interface{}
		wantToUpper  []interface{}
	}{
		{
			input:        beam.Create(s, "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"),
			wantReversed: []interface{}{"kciuq", "nworb", "xof", "spmuj", "revo", "eht", "yzal", "god"},
			wantToUpper:  []interface{}{"QUICK", "BROWN", "FOX", "JUMPS", "OVER", "THE", "LAZY", "DOG"},
		},
	}
	for _, tt := range tests {
		gotReversed, gotToUpper := task.ApplyTransform(s, tt.input)
		passert.Equals(s, gotReversed, tt.wantReversed...)
		passert.Equals(s, gotToUpper, tt.wantToUpper...)

		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}

