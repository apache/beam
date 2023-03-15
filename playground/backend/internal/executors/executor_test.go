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

const pipelineOptions = "--output t.txt"

// TODO: Fix this test
//func TestExecutor_GetCompileCmd(t *testing.T) {
//	type args struct {
//		paths  *fs_tool.LifeCyclePaths
//		sdkEnv *environment.BeamEnvs
//	}
//	tests := []struct {
//		name string
//		args args
//		want *exec.Cmd
//	}{
//		{
//			name: "TestCompile",
//			args: args{
//				paths: &fs_tool.LifeCyclePaths{
//					AbsoluteBaseFolderPath: "./",
//				},
//				sdkEnv: &environment.BeamEnvs{
//					ExecutorConfig: &environment.ExecutorConfig{
//						CompileCmd:  "testCommand",
//						CompileArgs: []string{"-d", "bin", "-parameters", "-classpath", "/opt/apache/beam/jars/beam-sdks-java-harness.jar"},
//					},
//				},
//			},
//			fields: fields{
//				compileArgs: CmdConfiguration{
//					fileNames:       []string{"filePath"},
//					workingDir:      "./",
//					commandName:     "testCommand",
//					commandArgs:     []string{"-d", "bin", "-parameters", "-classpath", "/opt/apache/beam/jars/beam-sdks-java-harness.jar"},
//					pipelineOptions: []string{""},
//				},
//			},
//			want: &exec.Cmd{
//				Path:         "testCommand",
//				Args:         []string{"javac", "-d", "bin", "-parameters", "-classpath", "/opt/apache/beam/jars/beam-sdks-java-harness.jar", "filePath"},
//				Env:          nil,
//				Dir:          "",
//				Stdin:        nil,
//				Stdout:       nil,
//				Stderr:       nil,
//				ExtraFiles:   nil,
//				SysProcAttr:  nil,
//				Process:      nil,
//				ProcessState: nil,
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			ex := &Executor{
//				compileArgs: tt.fields.compileArgs,
//				runArgs:     tt.fields.runArgs,
//			}
//			if got := GetCompileCmd(context.Background()); !reflect.DeepEqual(got.String(), tt.want.String()) {
//				t.Errorf("WithCompiler() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

// TODO: Fix the test
//func TestExecutor_Run(t *testing.T) {
//	type fields struct {
//		compileArgs CmdConfiguration
//		runArgs     CmdConfiguration
//		testArgs    CmdConfiguration
//		validators  []validators.Validator
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   *exec.Cmd
//	}{
//		{
//			name: "TestRun",
//			fields: fields{
//				runArgs: CmdConfiguration{
//					fileNames:   []string{"HelloWorld"},
//					workingDir:  "./",
//					commandName: "runCommand",
//					commandArgs: []string{"-cp", "bin:/opt/apache/beam/jars/beam-sdks-java-harness.jar:" +
//						"/opt/apache/beam/jars/beam-runners-direct.jar:/opt/apache/beam/jars/slf4j-jdk14.jar"},
//					pipelineOptions: []string{""},
//				},
//			},
//			want: &exec.Cmd{
//				Path: "runCommand",
//				Args: []string{"java", "-cp", "bin:/opt/apache/beam/jars/beam-sdks-java-harness.jar:" +
//					"/opt/apache/beam/jars/beam-runners-direct.jar:/opt/apache/beam/jars/slf4j-jdk14.jar", "HelloWorld"},
//				Env:          nil,
//				Dir:          "",
//				Stdin:        nil,
//				Stdout:       nil,
//				Stderr:       nil,
//				ExtraFiles:   nil,
//				SysProcAttr:  nil,
//				Process:      nil,
//				ProcessState: nil,
//			},
//		},
//		{
//			name: "TestRun with pipelineOptions",
//			fields: fields{
//				runArgs: CmdConfiguration{
//					fileNames:   []string{"HelloWorld"},
//					workingDir:  "./",
//					commandName: "runCommand",
//					commandArgs: []string{"-cp", "bin:/opt/apache/beam/jars/beam-sdks-java-harness.jar:" +
//						"/opt/apache/beam/jars/beam-runners-direct.jar:/opt/apache/beam/jars/slf4j-jdk14.jar"},
//					pipelineOptions: []string{pipelineOptions},
//				},
//			},
//			want: &exec.Cmd{
//				Path: "runCommand",
//				Args: []string{"java", "-cp", "bin:/opt/apache/beam/jars/beam-sdks-java-harness.jar:" +
//					"/opt/apache/beam/jars/beam-runners-direct.jar:/opt/apache/beam/jars/slf4j-jdk14.jar", "HelloWorld", pipelineOptions},
//				Env:          nil,
//				Dir:          "",
//				Stdin:        nil,
//				Stdout:       nil,
//				Stderr:       nil,
//				ExtraFiles:   nil,
//				SysProcAttr:  nil,
//				Process:      nil,
//				ProcessState: nil,
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			ex := &Executor{
//				compileArgs: tt.fields.compileArgs,
//				runArgs:     tt.fields.runArgs,
//				testArgs:    tt.fields.testArgs,
//			}
//			if got := ex.Run(context.Background()); !reflect.DeepEqual(got.String(), tt.want.String()) {
//				t.Errorf("WithRunner() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

// TODO: Fix the test
//func TestExecutor_RunTest(t *testing.T) {
//	type fields struct {
//		compileArgs CmdConfiguration
//		runArgs     CmdConfiguration
//		testArgs    CmdConfiguration
//		validators  []validators.Validator
//	}
//	type args struct {
//		ctx context.Context
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		args   args
//		want   *exec.Cmd
//	}{
//		{
//			name: "TestRunTest",
//			fields: fields{
//				testArgs: CmdConfiguration{
//					fileNames:       []string{"HelloWorld"},
//					workingDir:      "./",
//					commandName:     "testCommand",
//					commandArgs:     []string{"-cp", "option1:option2"},
//					pipelineOptions: []string{""},
//				},
//			},
//			args: args{context.Background()},
//			want: &exec.Cmd{
//				Path:         "testCommand",
//				Args:         []string{"java", "-cp", "option1:option2", "HelloWorld"},
//				Env:          nil,
//				Dir:          "./",
//				Stdin:        nil,
//				Stdout:       nil,
//				Stderr:       nil,
//				ExtraFiles:   nil,
//				SysProcAttr:  nil,
//				Process:      nil,
//				ProcessState: nil,
//			},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			ex := &Executor{
//				compileArgs: tt.fields.compileArgs,
//				runArgs:     tt.fields.runArgs,
//				testArgs:    tt.fields.testArgs,
//			}
//			if got := ex.RunTest(tt.args.ctx); !reflect.DeepEqual(got.String(), tt.want.String()) {
//				t.Errorf("RunTest() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}

// TODO: Fix test
//func Test_replaceLogPlaceholder(t *testing.T) {
//	execConfig := *pythonSdkEnv.ExecutorConfig
//	execConfig.RunArgs = []string{"arg1", javaLogConfigFilePlaceholder}
//	type args struct {
//		paths          *fs_tool.LifeCyclePaths
//		executorConfig *environment.ExecutorConfig
//	}
//	tests := []struct {
//		name string
//		args args
//		want []string
//	}{
//		{
//			name: "Test to check the replacement of log work with prepared running args",
//			args: args{
//				paths:          pythonPaths,
//				executorConfig: &execConfig,
//			},
//			want: []string{"arg1", filepath.Join(pythonPaths.AbsoluteBaseFolderPath, javaLogConfigFileName)},
//		},
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := ReplaceLogPlaceholder(tt.args.paths, tt.args.executorConfig); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("ReplaceLogPlaceholder() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
