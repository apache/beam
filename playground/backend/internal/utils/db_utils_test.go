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

func TestID(t *testing.T) {
	type args struct {
		salt    string
		content string
		length  int
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "ID generation in the usual case",
			args: args{
				salt:    "MOCK_SALT",
				content: "MOCK_CONTENT",
				length:  11,
			},
			want:    "Zl_s-8seE6k",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		result, err := ID(tt.args.salt, tt.args.content, tt.args.length)
		if (err != nil) != tt.wantErr {
			t.Errorf("ID() error = %v, wantErr %v", err, tt.wantErr)
			return
		}
		if result != tt.want {
			t.Errorf("ID() result = %v, want %v", result, tt.want)
		}
	}
}
