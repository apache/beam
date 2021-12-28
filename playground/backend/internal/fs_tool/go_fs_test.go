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

func Test_newGoLifeCycle(t *testing.T) {
	pipelineId := uuid.New()
	workingDir := "workingDir"
	baseFileFolder := fmt.Sprintf("%s/%s/%s", workingDir, baseFileFolder, pipelineId)
	srcFileFolder := baseFileFolder + "/src"
	binFileFolder := baseFileFolder + "/bin"

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
			// Test case with calling newGoLifeCycle method with correct pipelineId and workingDir.
			// As a result, want to receive an expected go life cycle.
			name: "newGoLifeCycle",
			args: args{
				pipelineId: pipelineId,
				workingDir: workingDir,
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder, srcFileFolder, binFileFolder},
				Dto: LifeCycleDTO{
					Folder: Folder{
						BaseFolder:           baseFileFolder,
						SourceFileFolder:     srcFileFolder,
						ExecutableFileFolder: binFileFolder,
					},
					Extension: Extension{
						SourceFileExtension:     goSourceFileExtension,
						ExecutableFileExtension: goExecutableFileExtension,
					},
					PipelineId: pipelineId,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newGoLifeCycle(tt.args.pipelineId, tt.args.workingDir)
			if !reflect.DeepEqual(got.folderGlobs, tt.want.folderGlobs) {
				t.Errorf("newGoLifeCycle() folderGlobs = %v, want %v", got.folderGlobs, tt.want.folderGlobs)
			}
			if !reflect.DeepEqual(got.Dto.Folder, tt.want.Dto.Folder) {
				t.Errorf("newGoLifeCycle() Folder = %v, want %v", got.Dto.Folder, tt.want.Dto.Folder)
			}
			if !reflect.DeepEqual(got.Dto.Extension, tt.want.Dto.Extension) {
				t.Errorf("newGoLifeCycle() Extension = %v, want %v", got.Dto.Extension, tt.want.Dto.Extension)
			}
			if !reflect.DeepEqual(got.Dto.PipelineId, tt.want.Dto.PipelineId) {
				t.Errorf("newGoLifeCycle() pipelineId = %v, want %v", got.Dto.PipelineId, tt.want.Dto.PipelineId)
			}
		})
	}
}
