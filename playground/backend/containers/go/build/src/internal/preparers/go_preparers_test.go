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
	"fmt"
	"reflect"
	"testing"
)

// getPreparedArgs returns array of received arguments
func getPreparedArgs(args ...interface{}) []interface{} {
	preparedArgs := make([]interface{}, len(args))
	for i := range preparedArgs {
		preparedArgs[i] = args[i]
	}
	return preparedArgs
}

func TestGetGoPreparers(t *testing.T) {
	type args struct {
		filePath      string
		prepareParams map[string]string
	}
	tests := []struct {
		name string
		args args
		want *[]Preparer
	}{
		{
			// getting the expected preparer
			name: "Get expected preparer",
			args: args{filePath: "", prepareParams: make(map[string]string)},
			want: &[]Preparer{{Prepare: formatCode, Args: nil}, {Prepare: changeGoTestFileName, Args: nil}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewPreparersBuilder(tt.args.filePath, tt.args.prepareParams)
			GetGoPreparers(builder, true)
			if got := builder.Build().GetPreparers(); !reflect.DeepEqual(fmt.Sprint(got), fmt.Sprint(tt.want)) {
				t.Errorf("GetGoPreparers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_formatCode(t *testing.T) {
	preparedArgs1 := getPreparedArgs(correctGoFile)
	preparedArgs2 := getPreparedArgs(incorrectGoFile)
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			// formatting code that does not contain errors
			name:    "File without errors",
			args:    args{preparedArgs1},
			wantErr: false,
		},
		{
			// formatting code that contain errors
			name:    "File with errors",
			args:    args{preparedArgs2},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := formatCode(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("formatCode() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
