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
	"fmt"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"testing"
)

const (
	sourceDir   = "sourceDir"
	fileName    = "file.txt"
	fileContent = "content"
)

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
	defer teardown()
	m.Run()
}

func setup() error {
	err := os.Mkdir(sourceDir, 0755)
	if err != nil {
		return err
	}
	filePath := filepath.Join(sourceDir, fileName)
	err = os.WriteFile(filePath, []byte(fileContent), 0600)
	return err
}

func teardown() error {
	return os.RemoveAll(sourceDir)
}

func TestReduceWhiteSpacesToSinge(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{name: "reduce white spaces to single", args: args{"--option1         option"}, want: "--option1 option"},
		{name: "nothing to reduce", args: args{"--option1 option"}, want: "--option1 option"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReduceWhiteSpacesToSinge(tt.args.s); got != tt.want {
				t.Errorf("ReduceWhiteSpacesToSinge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadFile(t *testing.T) {
	type args struct {
		pipelineId uuid.UUID
		path       string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "Read from existing file",
			args: args{
				pipelineId: uuid.New(),
				path:       filepath.Join(sourceDir, fileName),
			},
			want:    fileContent,
			wantErr: false,
		},
		{
			name: "Read from non-existent file",
			args: args{
				pipelineId: uuid.New(),
				path:       filepath.Join(sourceDir, "non-existent_file.txt"),
			},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadFile(tt.args.pipelineId, tt.args.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("ReadFile() got = %v, want %v", got, tt.want)
			}
		})
	}
}
