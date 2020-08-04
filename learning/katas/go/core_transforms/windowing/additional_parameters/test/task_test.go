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
	"beam.apache.org/learning/katas/core_transforms/additional_parameters/additional_parameters/pkg/common"
	"beam.apache.org/learning/katas/core_transforms/additional_parameters/additional_parameters/pkg/task"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/testing/ptest"
	"github.com/google/go-cmp/cmp"
	"testing"
	"time"
)

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	tests := []struct {
		input beam.PCollection
		want []interface{}
	}{
		{
			input: common.CreateLines(s),
			want: []interface{}{
				task.Commit{
					MaxTimestampWindow: time.Unix(1596211199, 0),
					EventTimestamp:     time.Unix(1596210725, 0),
					Line:               "3c6c45924a Remove trailing whitespace from README",
				},
				task.Commit{
					MaxTimestampWindow: time.Unix(1596211199, 0),
					EventTimestamp:     time.Unix(1596211180, 0),
					Line:               "a52be99b62 Merge pull request #12443 from KevinGG/whitespace",
				},
				task.Commit{
					MaxTimestampWindow: time.Unix(1596214799, 0),
					EventTimestamp:     time.Unix(1596211656, 0),
					Line:               "7c1772d13f Merge pull request #12439 from ibzib/beam-9199-1",
				},
				task.Commit{
					MaxTimestampWindow: time.Unix(1596214799, 0),
					EventTimestamp:     time.Unix(1596213341, 0),
					Line:               "d971ba13b8 Widen ranges for GCP libraries (#12198)",
				},
				task.Commit{
					MaxTimestampWindow: time.Unix(1596243599, 0),
					EventTimestamp:     time.Unix(1596240445, 0),
					Line:               "875620111b Enable all Jenkins jobs triggering for committers (#12407)",
				},
			},
		},
	}
	for _, tt := range tests {
		got := task.ApplyTransform(s, tt.input)
		cmp.Equal(got, tt.want)
		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}
