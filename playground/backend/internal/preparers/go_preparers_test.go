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

func TestGetGoPreparers(t *testing.T) {
	const filePath = "testfile.go"
	type args struct {
		filePath   string
		isUnitTest bool
	}
	tests := []struct {
		name string
		args args
		want goPreparer
	}{
		{
			// getting the expected preparer
			name: "Get expected preparer",
			args: args{filePath: filePath, isUnitTest: false},
			want: goPreparer{
				preparer:   preparer{filePath: filePath},
				isUnitTest: false,
			},
		},
		{
			// getting the expected preparer
			name: "Get expected preparer",
			args: args{filePath: filePath, isUnitTest: true},
			want: goPreparer{
				preparer:   preparer{filePath: filePath},
				isUnitTest: true,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetGoPreparer(tt.args.filePath, tt.args.isUnitTest)
			if !cmp.Equal(got, tt.want, cmp.AllowUnexported(goPreparer{}, preparer{})) {
				t.Errorf("GetGoPreparers() diff = %s", cmp.Diff(got, tt.want, cmp.AllowUnexported(goPreparer{}, preparer{})))
			}
		})
	}
}

func Test_formatCode(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			// formatting code that does not contain errors
			name:    "File without errors",
			args:    args{correctGoFile},
			wantErr: false,
		},
		{
			// formatting code that contain errors
			name:    "File with errors",
			args:    args{incorrectGoFile},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := formatCode(tt.args.filePath); (err != nil) != tt.wantErr {
				t.Errorf("formatCode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
