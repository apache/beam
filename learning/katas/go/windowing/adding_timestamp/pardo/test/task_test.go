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
	"beam.apache.org/learning/katas/windowing/adding_timestamp/pardo/pkg/common"
	"beam.apache.org/learning/katas/windowing/adding_timestamp/pardo/pkg/task"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/graph/mtime"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
	"testing"
)

func TestApplyTransform(t *testing.T) {
	p, s := beam.NewPipelineWithRoot()
	tests := []struct {
		input beam.PCollection
	}{
		{
			input: common.CreateCommits(s),
		},
	}
	for _, tt := range tests {
		got := task.ApplyTransform(s, tt.input)
		beam.ParDo0(s, func(et beam.EventTime, commit task.Commit) {
			// assert whether student assigned the correct timestamp
			if et.Milliseconds()*1e6 != commit.Datetime.UnixNano() {
				t.Errorf("ApplyTransform() = %v , want %v", et, mtime.FromTime(commit.Datetime))
			}
		}, got)
		if err := ptest.Run(p); err != nil {
			t.Error(err)
		}
	}
}
