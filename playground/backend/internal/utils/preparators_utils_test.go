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

package utils

import "testing"

func TestSpacesToEqualsOption(t *testing.T) {
	type args struct {
		pipelineOptions string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "args is empty string",
			args: args{pipelineOptions: ""},
			want: "",
		},
		{
			name: "args with one option",
			args: args{pipelineOptions: "--opt1 valOpt"},
			want: "--opt1=valOpt",
		},
		{
			name: "args with some options",
			args: args{pipelineOptions: "--opt1 valOpt --opt2 valOpt --opt3 valOpt"},
			want: "--opt1=valOpt --opt2=valOpt --opt3=valOpt",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SpacesToEqualsOption(tt.args.pipelineOptions); got != tt.want {
				t.Errorf("SpacesToEqualsOption() = %v, want %v", got, tt.want)
			}
		})
	}
}
