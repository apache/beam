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

func Test_newGoLifeCycle(t *testing.T) {
	pipelineId := uuid.New()
	workingDir, _ := filepath.Abs("workingDir")
	baseFileFolder := filepath.Join(workingDir, pipelinesFolder, pipelineId.String())
	srcFileFolder := filepath.Join(baseFileFolder, "src")
	binFileFolder := filepath.Join(baseFileFolder, "bin")

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
			// Test case with calling newGoLifeCycle method with correct pipelineId and workingDir.
			// As a result, want to receive an expected go life cycle.
			name: "NewGoLifeCycle",
			args: args{
				pipelineId:      pipelineId,
				pipelinesFolder: filepath.Join(workingDir, pipelinesFolder),
			},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder, srcFileFolder, binFileFolder},
				Paths: LifeCyclePaths{
					SourceFileName:                   pipelineId.String() + GoSourceFileExtension,
					AbsoluteSourceFileFolderPath:     srcFileFolder,
					AbsoluteSourceFilePath:           filepath.Join(srcFileFolder, pipelineId.String()+GoSourceFileExtension),
					ExecutableFileName:               pipelineId.String() + goExecutableFileExtension,
					AbsoluteExecutableFileFolderPath: binFileFolder,
					AbsoluteExecutableFilePath:       filepath.Join(binFileFolder, pipelineId.String()+goExecutableFileExtension),
					AbsoluteBaseFolderPath:           baseFileFolder,
					AbsoluteLogFilePath:              filepath.Join(baseFileFolder, logFileName),
					AbsoluteGraphFilePath:            filepath.Join(srcFileFolder, utils.GraphFileName),
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := newGoLifeCycle(tt.args.pipelineId, tt.args.pipelinesFolder)
			if !reflect.DeepEqual(got.folderGlobs, tt.want.folderGlobs) {
				t.Errorf("newGoLifeCycle() folderGlobs = %v, want %v", got.folderGlobs, tt.want.folderGlobs)
			}
			if !reflect.DeepEqual(got.Paths, tt.want.Paths) {
				t.Errorf("newGoLifeCycle() Paths = %v, want %v", got.Paths, tt.want.Paths)
			}
		})
	}
}
