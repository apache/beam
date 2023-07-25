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

package fs_tool

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestWrongExtension_Error(t *testing.T) {
	errorMessage := "MOCK_ERROR"
	type fields struct {
		error string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "Get error of the WrongExtension",
			fields: fields{error: errorMessage},
			want:   fmt.Sprintf("File has wrong extension: %v", errorMessage),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &WrongExtension{
				error: tt.fields.error,
			}
			if got := e.Error(); got != tt.want {
				t.Errorf("Error() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCheckPathIsValid(t *testing.T) {
	type args struct {
		args []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    bool
		wantErr bool
	}{
		{
			name:    "File not exist",
			args:    args{args: []interface{}{"filePath.txt", ".txt"}},
			want:    false,
			wantErr: true,
		},
		{
			name:    "Incorrect extension",
			args:    args{args: []interface{}{filepath.Join(sourceDir, fileName), JavaSourceFileExtension}},
			want:    false,
			wantErr: true,
		},
		{
			name:    "CheckPathIsValid worked successfully",
			args:    args{args: []interface{}{filepath.Join(sourceDir, fileName), ".txt"}},
			want:    true,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CheckPathIsValid(tt.args.args...)
			if (err != nil) != tt.wantErr {
				t.Errorf("CheckPathIsValid() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("CheckPathIsValid() got = %v, want %v", got, tt.want)
			}
		})
	}
}
