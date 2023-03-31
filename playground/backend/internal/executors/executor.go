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

package executors

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/fs_tool"
	"context"
	"fmt"
	"os/exec"
)

type Executor interface {
	GetCompileCmd(ctx context.Context) (*exec.Cmd, error)
	GetRunCmd(ctx context.Context, pipelineOptions string) (*exec.Cmd, error)
	GetRunTestCmd(ctx context.Context) (*exec.Cmd, error)
}

type executor struct {
	paths *fs_tool.LifeCyclePaths
	sdk   pb.Sdk
}

// GetExecutor returns Executor instance for the given SDK
func GetExecutor(paths *fs_tool.LifeCyclePaths, sdk pb.Sdk) (Executor, error) {
	switch sdk {
	case pb.Sdk_SDK_JAVA:
		return getJavaExecutor(paths), nil
	case pb.Sdk_SDK_GO:
		return getGoExecutor(paths), nil
	case pb.Sdk_SDK_PYTHON:
		return getPythonExecutor(paths), nil
	case pb.Sdk_SDK_SCIO:
		return getScioExecutor(paths), nil
	default:
		return nil, fmt.Errorf("unsupported sdk '%s'", sdk.String())
	}
}
