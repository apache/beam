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
	"beam.apache.org/playground/backend/internal/utils"
	"github.com/google/uuid"
	"path/filepath"
	"reflect"
	"testing"
)

func Test_newPythonLifeCycle(t *testing.T) {
	pipelineId := uuid.New()
	workingDir, _ := filepath.Abs("workingDir")
	baseFileFolder := filepath.Join(workingDir, pipelinesFolder, pipelineId.String())

	type args struct {
		pipelineId      uuid.UUID
		pipelinesFolder string
	}
	tests := []struct {
		name string
		args args
		want *LifeCycle
	}{
		{
			// Test case with calling newPythonLifeCycle method with correct pipelineId and workingDir.
			// As a result, want to receive an expected python life cycle.
			name: "NewPythonLifeCycle",
			args: args{
				pipelineId:      pipelineId,
				pipelinesFolder: filepath.Join(workingDir, pipelinesFolder),
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder},
				Paths: LifeCyclePaths{
					SourceFileName:                   pipelineId.String() + pythonExecutableFileExtension,
					AbsoluteSourceFileFolderPath:     baseFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(baseFileFolder, pipelineId.String()+pythonExecutableFileExtension),
					ExecutableFileName:               pipelineId.String() + pythonExecutableFileExtension,
					AbsoluteExecutableFileFolderPath: baseFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(baseFileFolder, pipelineId.String()+pythonExecutableFileExtension),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
					AbsoluteGraphFilePath:            filepath.Join(baseFileFolder, utils.GraphFileName),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newPythonLifeCycle(tt.args.pipelineId, tt.args.pipelinesFolder)
			if !reflect.DeepEqual(got.folderGlobs, tt.want.folderGlobs) {
				t.Errorf("newPythonLifeCycle() folderGlobs = %v, want %v", got.folderGlobs, tt.want.folderGlobs)
			}
			if !checkPathsEqual(got.Paths, tt.want.Paths) {
				t.Errorf("newPythonLifeCycle() Paths = %v, want %v", got.Paths, tt.want.Paths)
			}
		})
	}
}
