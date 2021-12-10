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
	"beam.apache.org/playground/backend/internal/logger"
	"fmt"
	"os"
	"reflect"
	"testing"
)

const (
	correctCode   = "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello world\")\n\n\n}\n"
	correctFile   = "correct.go"
	incorrectCode = "package main\n\nimport \"fmt\"\n\nfunc main() {\n\tfmt.Println(\"hello world\"\n\n}\n"
	incorrectFile = "incorrect.go"
)

func TestMain(m *testing.M) {
	err := setupPreparedFiles()
	if err != nil {
		logger.Fatal(err)
	}
	defer teardown()
	m.Run()
}

// setupPreparedFiles creates 2 go programs:
// correctFile - program without errors
// incorrectFile - program with errors
func setupPreparedFiles() error {
	err := createFile(correctFile, correctCode)
	if err != nil {
		return err
	}
	err = createFile(incorrectFile, incorrectCode)
	if err != nil {
		return err
	}
	return nil
}

//createFile create file with fileName and write text to it
func createFile(fileName, text string) error {
	f, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(text))
	if err != nil {
		return err
	}
	return nil
}

func teardown() {
	err := os.Remove(correctFile)
	if err != nil {
		logger.Fatal(err)
	}
	err = os.Remove(incorrectFile)
	if err != nil {
		logger.Fatal(err)
	}
}

// getPreparedArgs returns array of received arguments
func getPreparedArgs(args ...interface{}) []interface{} {
	preparedArgs := make([]interface{}, len(args))
	for i := range preparedArgs {
		preparedArgs[i] = args[i]
	}
	return preparedArgs
}

func TestGetGoPreparers(t *testing.T) {
	expectedPreparer := Preparer{Prepare: formatCode, Args: nil}
	type args struct {
		filePath string
	}
	tests := []struct {
		name string
		args args
		want *[]Preparer
	}{
		{
			// getting the expected preparer
			name: "get expected preparer",
			args: args{filePath: ""},
			want: &[]Preparer{expectedPreparer},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := GetGoPreparers(tt.args.filePath); !reflect.DeepEqual(fmt.Sprint(got), fmt.Sprint(tt.want)) {
				t.Errorf("GetGoPreparers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_formatCode(t *testing.T) {
	preparedArgs1 := getPreparedArgs(correctFile)
	preparedArgs2 := getPreparedArgs(incorrectFile)
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
			name:    "file without errors",
			args:    args{preparedArgs1},
			wantErr: false,
		},
		{
			// formatting code that contain errors
			name:    "file with errors",
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
