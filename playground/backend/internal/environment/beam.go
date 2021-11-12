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

package environment

import (
	pb "beam.apache.org/playground/backend/internal/api/v1"
)

// ExecutorConfig contains all environment variables needed for compiling and execution of the code commands:
// - CompileCmd: command to compile files with code
// - RunCmd: command to run compiled code
// - CompileArgs: arguments which are needed to compile files with code
// - RunArgs: arguments which are needed to run compiled code
type ExecutorConfig struct {
	CompileCmd  string   `json:"compile_cmd"`
	RunCmd      string   `json:"run_cmd"`
	CompileArgs []string `json:"compile_args"`
	RunArgs     []string `json:"run_args"`
}

// NewExecutorConfig creates and returns ExecutorConfig
func NewExecutorConfig(compileCmd string, runCmd string, compileArgs []string, runArgs []string) *ExecutorConfig {
	return &ExecutorConfig{CompileCmd: compileCmd, RunCmd: runCmd, CompileArgs: compileArgs, RunArgs: runArgs}
}

// BeamEnvs contains all environments related of ApacheBeam. These will use to run pipelines
type BeamEnvs struct {
	ApacheBeamSdk  pb.Sdk
	ExecutorConfig *ExecutorConfig
}

// NewBeamEnvs is a BeamEnvs constructor
func NewBeamEnvs(apacheBeamSdk pb.Sdk, executorConfig *ExecutorConfig) *BeamEnvs {
	return &BeamEnvs{ApacheBeamSdk: apacheBeamSdk, ExecutorConfig: executorConfig}
}
