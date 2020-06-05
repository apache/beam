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
	"combine_perkey/pkg/task"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"testing"
)

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()

	tests := []struct {
		input beam.PCollection
		want  map[string]int
	}{
		{
			input: beam.ParDo(s, func(_ []byte, emit func(string, int)) {
				emit(task.Player1, 15)
				emit(task.Player2, 10)
				emit(task.Player1, 100)
				emit(task.Player3, 25)
				emit(task.Player2, 75)
			}, beam.Impulse(s)),
			want: map[string]int{
				task.Player1: 115,
				task.Player2: 85,
				task.Player3: 25,
			},
		},
	}
	for _, tt := range tests {
		gotKV := task.ApplyTransform(s, tt.input)
		beam.ParDo(s, func(player string, iter func(*int) bool, emit func(bool)) {
			var got int
			iter(&got)
			want := tt.want[player]
			if got != want {
				t.Errorf("%s = %v , want %v", player, got, want)
			}
		}, beam.GroupByKey(s, gotKV))
		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}
