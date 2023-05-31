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
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"beam.apache.org/playground/backend/internal/utils"
)

func TestGetPythonPreparers(t *testing.T) {
	type args struct {
		filePath      string
		prepareParams map[string]string
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{
			name: "Get number of python preparers",
			args: args{"MOCK_FILEPATH", make(map[string]string)},
			want: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewPreparersBuilder(tt.args.filePath, tt.args.prepareParams)
			GetPythonPreparers(builder, false)
			if got := builder.Build().GetPreparers(); len(*got) != tt.want {
				t.Errorf("GetPythonPreparers() returns %v Preparers, want %v", len(*got), tt.want)
			}
		})
	}
}

func Test_addCodeToFile(t *testing.T) {
	wantCode := "import logging\nlogging.basicConfig(\n    level=logging.INFO,\n    format=\"%(asctime)s [%(levelname)s] %(message)s\",\n    handlers=[\n        logging.FileHandler(\"logs.log\"),\n    ]\n)\n" + pyCode

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
			// Call addCodeToFile method if the file doesn't exist.
			// As a result, want to receive error
			name:    "File doesn't exist",
			args:    args{[]interface{}{incorrectPyFile, saveLogs}},
			wantErr: true,
		},
		{
			// Call addCodeToFile method when file exists.
			// As a result, want to receive an updated code
			name:     "Original file exists",
			args:     args{[]interface{}{correctPyFile, saveLogs}},
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

func Test_saveLogs(t *testing.T) {
	file, _ := os.Open(correctPyFile)
	tmp, _ := utils.CreateTempFile(correctPyFile)
	defer tmp.Close()
	tmp2, _ := os.OpenFile(correctPyFile, os.O_RDONLY, 0)
	defer tmp2.Close()

	type args struct {
		from *os.File
		to   *os.File
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			// Call saveLogs method to add logs code to tmp file.
			name: "Save logs successfully",
			args: args{
				from: file,
				to:   tmp,
			},
			wantErr: false,
		},
		{
			// Call saveLogs method to add logs code to read-only file.
			name: "Save logs to read-only file",
			args: args{
				from: file,
				to:   tmp2,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := saveLogs(tt.args.from, tt.args.to); (err != nil) != tt.wantErr {
				t.Errorf("saveLogs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_writeToFile(t *testing.T) {
	tmp, _ := utils.CreateTempFile(correctPyFile)
	defer tmp.Close()
	tmp2, _ := os.OpenFile(incorrectPyFile, os.O_CREATE, 0)
	defer tmp2.Close()

	type args struct {
		to  *os.File
		str string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			// Call writeToFile method to successfully add line to tmp file.
			name: "Successfully write to file",
			args: args{
				to:  tmp,
				str: "just a string",
			},
			wantErr: false,
		},
		{
			// Call writeToFile method to write to tmp file which is open only for reading.
			// As a result, want to receive error.
			name: "Write to file which is read-only",
			args: args{
				to:  tmp2,
				str: "just a string",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := writeToFile(tt.args.to, tt.args.str); (err != nil) != tt.wantErr {
				t.Errorf("writeToFile() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_saveGraph(t *testing.T) {
	file, _ := os.Open(pyGraphFile)
	noPipelineFile, _ := os.Open(correctPyFile)
	tmp, _ := utils.CreateTempFile(pyGraphFile)
	defer tmp.Close()
	type args struct {
		from     *os.File
		tempFile *os.File
	}
	tests := []struct {
		name      string
		args      args
		wantErr   bool
		wantGraph bool
	}{
		{
			// Call saveGraph method to add code (to tmp file) which will save graph to file.
			name: "Successfully save graph",
			args: args{
				from:     file,
				tempFile: tmp,
			},
			wantErr:   false,
			wantGraph: true,
		},
		{
			// There is no pipeline definition at the code and no graph code will be added.
			name: "No pipeline at code",
			args: args{
				from:     noPipelineFile,
				tempFile: tmp,
			},
			wantErr:   false,
			wantGraph: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := saveGraph(tt.args.from, tt.args.tempFile); (err != nil) != tt.wantErr {
				t.Errorf("saveGraph() error = %v, wantErr %v", err, tt.wantErr)
			}
			bytes, _ := ioutil.ReadFile(fmt.Sprintf("%s_%s", "tmp", pyGraphFile))
			if tt.wantGraph && !strings.Contains(string(bytes), "pipeline_graph.PipelineGraph") {
				t.Errorf("saveGraph() error = %v, wantErr %v", "No graph code was added", tt.wantErr)
			}
		})
	}
}
