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

func Test_newJavaLifeCycle(t *testing.T) {
	pipelineId := uuid.New()
	baseFileFolder := fmt.Sprintf("%s_%s", javaBaseFileFolder, pipelineId)
	srcFileFolder := baseFileFolder + "/src"
	binFileFolder := baseFileFolder + "/bin"

	type args struct {
		pipelineId uuid.UUID
	}
	tests := []struct {
		name string
		args args
		want *LifeCycle
	}{
		{
			name: "newJavaLifeCycle",
			args: args{pipelineId: pipelineId},
			want: &LifeCycle{
				folderGlobs: []string{baseFileFolder, srcFileFolder, binFileFolder},
				Folder: Folder{
					BaseFolder:       baseFileFolder,
					ExecutableFolder: srcFileFolder,
					CompiledFolder:   binFileFolder,
				},
				Extension: Extension{
					ExecutableExtension: javaExecutableFileExtension,
					CompiledExtension:   javaCompiledFileExtension,
				},
				pipelineId: pipelineId,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newJavaLifeCycle(tt.args.pipelineId); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("newJavaLifeCycle() = %v, want %v", got, tt.want)
			}
		})
	}
}
