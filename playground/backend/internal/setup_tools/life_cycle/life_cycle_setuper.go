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

package life_cycle

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"beam.apache.org/playground/backend/internal/logger"
	"github.com/google/uuid"
)

// Setup returns fs_tool.LifeCycle.
// Also, prepares files and folders needed to code processing according to sdk
func Setup(sdk pb.Sdk, code string, pipelineId uuid.UUID, workingDir string) (*fs_tool.LifeCycle, error) {
	// create file system service
	lc, err := fs_tool.NewLifeCycle(sdk, pipelineId, workingDir)
	if err != nil {
		logger.Errorf("%s: RunCode(): NewLifeCycle(): %s\n", pipelineId, err.Error())
		return nil, err
	}

	// create folders
	err = lc.CreateFolders()
	if err != nil {
		logger.Errorf("%s: RunCode(): CreateFolders(): %s\n", pipelineId, err.Error())
		return nil, err
	}

	// create file with code
	_, err = lc.CreateSourceCodeFile(code)
	if err != nil {
		logger.Errorf("%s: RunCode(): CreateSourceCodeFile(): %s\n", pipelineId, err.Error())
		return nil, err
	}
	return lc, nil
}
