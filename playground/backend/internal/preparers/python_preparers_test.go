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
	"os"
	"strings"
	"testing"
)

func TestGetPythonPreparers(t *testing.T) {
	type args struct {
		filePath string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			// Test case with calling GetPythonPreparers method.
			// As a result, want to receive slice of preparers with len = 1
			name: "get python preparers",
			args: args{"MOCK_FILEPATH"},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewPreparersBuilder(tt.args.filePath)
			GetPythonPreparers(builder, false)
			if got := builder.Build().GetPreparers(); len(*got) != tt.want {
				t.Errorf("GetPythonPreparers() returns %v Preparers, want %v", len(*got), tt.want)
			}
		})
	}
}

func Test_addCodeToFile(t *testing.T) {
	originalCode := "import logging as l\n\nif __name__ == \"__main__\":\n    logging.info(\"INFO\")\n"
	wantCode := "import logging\nlogging.basicConfig(\n    level=logging.DEBUG,\n    format=\"%(asctime)s [%(levelname)s] %(message)s\",\n    handlers=[\n        logging.FileHandler(\"logs.log\"),\n    ]\n)\n" + originalCode

	err := os.WriteFile("original.py", []byte(originalCode), 0600)
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll("original.py")

	type args struct {
		args []interface{}
	}
	tests := []struct {
		name     string
		args     args
		wantCode string
		wantErr  bool
	}{
		{
			// Test case with calling addCodeToFile method when original file doesn't exist.
			// As a result, want to receive error
			name:    "original file doesn't exist",
			args:    args{[]interface{}{"someFile.java", saveLogs}},
			wantErr: true,
		},
		{
			// Test case with calling addCodeToFile method when original file exists.
			// As a result, want to receive updated code in the original file
			name:     "original file exists",
			args:     args{[]interface{}{"original.py", saveLogs}},
			wantCode: wantCode,
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := addCodeToFile(tt.args.args...); (err != nil) != tt.wantErr {
				t.Errorf("addToCode() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				data, err := os.ReadFile(tt.args.args[0].(string))
				if err != nil {
					t.Errorf("addToCode() unexpected error = %v", err)
				}
				if !strings.EqualFold(string(data), tt.wantCode) {
					t.Errorf("addToCode() code = {%v}, wantCode {%v}", string(data), tt.wantCode)
				}
			}
		})
	}
}
