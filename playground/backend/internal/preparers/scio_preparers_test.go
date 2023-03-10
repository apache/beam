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

package preparers

import (
	"github.com/google/go-cmp/cmp"
	"testing"
)

func TestGetScioPreparers(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name string
		args args
		want scioPreparer
	}{
		{
			// Test case with calling GetScioPreparers method.
			// As a result, want to receive slice of preparers with len = 3
			name: "Get scio preparers",
			args: args{"MOCK_FILEPATH"},
			want: scioPreparer{preparer{filePath: "MOCK_FILEPATH"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetScioPreparer(tt.args.filePath)
			if !cmp.Equal(got, tt.want, cmp.AllowUnexported(scioPreparer{}, preparer{})) {
				t.Errorf("GetScioPreparers() diff = %s", cmp.Diff(got, tt.want, cmp.AllowUnexported(scioPreparer{}, preparer{})))
			}
		})
	}
}
