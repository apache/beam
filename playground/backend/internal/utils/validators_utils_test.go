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

import (
	playground "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/validators"
	"reflect"
	"testing"
)

func TestGetValidators(t *testing.T) {
	type args struct {
		sdk      playground.Sdk
		filepath string
	}
	tests := []struct {
		name    string
		args    args
		want    *[]validators.Validator
		wantErr bool
	}{
		{
			// Test case with calling GetValidators method with incorrect SDK.
			// As a result, want to receive an error.
			name: "incorrect sdk",
			args: args{
				sdk:      playground.Sdk_SDK_UNSPECIFIED,
				filepath: "",
			},
			want:    nil,
			wantErr: true,
		},
		{
			// Test case with calling GetValidators method with correct SDK.
			// As a result, want to receive an expected slice of validators.
			name: "correct sdk",
			args: args{
				sdk:      playground.Sdk_SDK_JAVA,
				filepath: "",
			},
			want:    validators.GetJavaValidators(""),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := GetValidators(tt.args.sdk, tt.args.filepath)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetValidators() err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != nil {
				if !reflect.DeepEqual(len(*got), len(*tt.want)) {
					t.Errorf("GetValidators() len = %v, want %v", len(*got), len(*tt.want))
				}
				for i := range *got {
					gotVal := (*got)[i]
					wantVal := (*tt.want)[i]
					if !reflect.DeepEqual(gotVal.Args, wantVal.Args) {
						t.Errorf("GetValidators() %d = %v, want %v", i, gotVal.Args, wantVal.Args)
					}
				}
			}
		})
	}
}
