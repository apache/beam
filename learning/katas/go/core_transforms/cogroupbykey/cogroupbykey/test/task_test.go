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

package task

import (
	"beam.apache.org/learning/katas/core_transforms/cogroupbykey/cogroupbykey/pkg/task"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"testing"
)

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	type args struct {
		fruits    beam.PCollection
		countries beam.PCollection
	}
	tests := []struct {
		args args
		want []interface{}
	}{
		{
			args: args{
				fruits: beam.Create(s.Scope("Fruits"), "apple", "banana", "cherry"),
				countries: beam.Create(s.Scope("Countries"), "australia", "brazil", "canada"),
			},
			want: []interface{}{
				"WordsAlphabet{Alphabet:a Fruit:apple Country:australia}",
				"WordsAlphabet{Alphabet:b Fruit:banana Country:brazil}",
				"WordsAlphabet{Alphabet:c Fruit:cherry Country:canada}",
			},
		},
	}
	for _, tt := range tests {
		got := task.ApplyTransform(s, tt.args.fruits, tt.args.countries)
		passert.Equals(s, got, tt.want...)
		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}
