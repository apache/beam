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
	"github.com/google/uuid"
	"reflect"
	"testing"
)

func Test_newPythonLifeCycle(t *testing.T) {
	pipelineId := uuid.New()
	workingDir := "workingDir"
	baseFileFolder := fmt.Sprintf("%s/%s/%s", workingDir, baseFileFolder, pipelineId)

	type args struct {
		pipelineId uuid.UUID
		workingDir string
	}
	tests := []struct {
		name string
		args args
		want *LifeCycle
	}{
		{
			// Test case with calling newPythonLifeCycle method with correct pipelineId and workingDir.
			// As a result, want to receive an expected go life cycle.
			name: "newPythonLifeCycle",
			args: args{
				pipelineId: pipelineId,
				workingDir: workingDir,
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder},
				Folder: Folder{
					BaseFolder:           baseFileFolder,
					SourceFileFolder:     baseFileFolder,
					ExecutableFileFolder: baseFileFolder,
				},
				Extension: Extension{
					SourceFileExtension:     pythonExecutableFileExtension,
					ExecutableFileExtension: pythonExecutableFileExtension,
				},
				pipelineId: pipelineId,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newPythonLifeCycle(tt.args.pipelineId, tt.args.workingDir)
			if !reflect.DeepEqual(got.folderGlobs, tt.want.folderGlobs) {
				t.Errorf("newPythonLifeCycle() folderGlobs = %v, want %v", got.folderGlobs, tt.want.folderGlobs)
			}
			if !reflect.DeepEqual(got.Folder, tt.want.Folder) {
				t.Errorf("newPythonLifeCycle() Folder = %v, want %v", got.Folder, tt.want.Folder)
			}
			if !reflect.DeepEqual(got.Extension, tt.want.Extension) {
				t.Errorf("newPythonLifeCycle() Extension = %v, want %v", got.Extension, tt.want.Extension)
			}
			if !reflect.DeepEqual(got.pipelineId, tt.want.pipelineId) {
				t.Errorf("newPythonLifeCycle() pipelineId = %v, want %v", got.pipelineId, tt.want.pipelineId)
			}
		})
	}
}
