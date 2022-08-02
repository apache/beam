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
	"beam.apache.org/learning/katas/common_transforms/aggregation/count/pkg/task"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"testing"
)

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	tests := []struct {
		input beam.PCollection
		want []interface{}
	}{
		{
			input: beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
			want: []interface{}{10},
		},
	}
	for _, tt := range tests {
		got := task.ApplyTransform(s, tt.input)
		passert.Equals(s, got, tt.want...)
		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}