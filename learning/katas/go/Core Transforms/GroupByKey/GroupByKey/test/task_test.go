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
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"groupbykey/pkg/task"
	"reflect"
	"testing"
)

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	tests := []struct {
		input beam.PCollection
		want map[uint8][]string
	}{
		{
			input: beam.Create(s, "apple", "ball", "car", "bear", "cheetah", "ant"),
			want: map[uint8][]string{
				97: []string{"apple", "ant"},
				98: []string{"ball", "bear"},
				99: []string{"car", "cheetah"},
			},
		},
	}
	for _, tt := range tests {
		got := task.ApplyTransform(s, tt.input)
		beam.ParDo0(s, func(key uint8, values func(*string) bool) {
			var got []string
			var v string
			for values(&v) {
				got = append(got, v)
			}
			if !reflect.DeepEqual(got, tt.want[key]) {
				t.Errorf("ApplyTransform() = %v , want %v", got, tt.want[key])
			}
		}, got)
		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}
