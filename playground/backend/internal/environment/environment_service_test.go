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
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	pb "beam.apache.org/playground/backend/internal/api/v1"
)

const (
	javaConfig       = "{\n  \"compile_cmd\": \"javac\",\n  \"run_cmd\": \"java\",\n  \"test_cmd\": \"java\",\n  \"compile_args\": [\n    \"-d\",\n    \"bin\",\n    \"-parameters\",\n    \"-classpath\"\n  ],\n  \"run_args\": [\n    \"-cp\",\n    \"bin:\"\n  ],\n  \"test_args\": [\n    \"-cp\",\n    \"bin:\",\n    \"org.junit.runner.JUnitCore\"\n  ]\n}"
	goConfig         = "{\n  \"compile_cmd\": \"go\",\n  \"run_cmd\": \"\",\n  \"test_cmd\": \"go\",\n  \"compile_args\": [\n    \"build\",\n    \"-o\",\n    \"bin\"\n  ],\n  \"run_args\": [\n  ],\n  \"test_args\": [\n    \"test\",\n    \"-v\"\n  ]\n}\n"
	pythonConfig     = "{\n  \"compile_cmd\": \"\",\n  \"run_cmd\": \"python3\",\n  \"test_cmd\": \"pytest\",\n  \"compile_args\": [],\n  \"run_args\": [],\n  \"test_args\": []\n}\n"
	scioConfig       = "{\n  \"compile_cmd\": \"\",\n  \"run_cmd\": \"sbt\",\n  \"test_cmd\": \"sbt\",\n  \"compile_args\": [],\n  \"run_args\": [\n    \"runMain\"\n  ],\n  \"test_args\": []\n}\n"
	defaultProjectId = ""
	dirPermission    = 0600
)

var executorConfig *ExecutorConfig

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
	defer teardown()
	m.Run()
}

func setup() error {
	err := os.MkdirAll(configFolderName, fs.ModePerm)
	if err != nil {
		return err
	}
	javaConfigPath := filepath.Join(configFolderName, pb.Sdk_SDK_JAVA.String()+jsonExt)
	err = os.WriteFile(javaConfigPath, []byte(javaConfig), dirPermission)
	goConfigPath := filepath.Join(configFolderName, pb.Sdk_SDK_GO.String()+jsonExt)
	err = os.WriteFile(goConfigPath, []byte(goConfig), dirPermission)
	pythonConfigPath := filepath.Join(configFolderName, pb.Sdk_SDK_PYTHON.String()+jsonExt)
	err = os.WriteFile(pythonConfigPath, []byte(pythonConfig), dirPermission)
	scioConfigPath := filepath.Join(configFolderName, pb.Sdk_SDK_SCIO.String()+jsonExt)
	err = os.WriteFile(scioConfigPath, []byte(scioConfig), dirPermission)
	if err != nil {
		return err
	}
	os.Clearenv()

	jars, err := ConcatBeamJarsToString()
	if err != nil {
		return err
	}
	executorConfig = NewExecutorConfig(
		"javac", "java", "java",
		[]string{"-d", "bin", "-parameters", "-classpath", jars},
		[]string{"-cp", "bin:" + jars},
		[]string{"-cp", "bin:" + jars, "org.junit.runner.JUnitCore"},
	)
	return nil
}

func teardown() {
	err := os.RemoveAll(configFolderName)
	if err != nil {
		panic(fmt.Errorf("error during test setup: %s", err.Error()))
	}
}

func setOsEnvs(envsToSet map[string]string) error {
	for key, value := range envsToSet {
		if err := os.Setenv(key, value); err != nil {
			return err
		}

	}
	return nil
}

func TestNewEnvironment(t *testing.T) {
	executorConfig := NewExecutorConfig("javac", "java", "java", []string{""}, []string{""}, []string{""})
	preparedModDir := ""
	tests := []struct {
		name string
		want *Environment
	}{
		{name: "Create env service with default envs", want: &Environment{
			NetworkEnvs: *NewNetworkEnvs(defaultIp, defaultPort, defaultProtocol),
			BeamSdkEnvs: *NewBeamEnvs(defaultSdk, defaultBeamVersion, executorConfig, preparedModDir, 0),
			ApplicationEnvs: *NewApplicationEnvs(
				"/app",
				defaultLaunchSite,
				defaultProjectId,
				defaultPipelinesFolder,
				defaultSDKConfigPath,
				defaultPropertyPath,
				defaultKafkaEmulatorExecutablePath,
				defaultDatasetsPath,
				defaultCleanupSnippetsFunctionsUrl,
				defaultPutSnippetFunctionsUrl,
				defaultIncrementSnippetViewsFunctionsUrl,
				&CacheEnvs{
					defaultCacheType,
					defaultCacheAddress,
					defaultCacheKeyExpirationTime,
				},
				defaultPipelineExecuteTimeout,
				defaultCacheRequestTimeout,
			),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEnvironment(
				*NewNetworkEnvs(defaultIp, defaultPort, defaultProtocol),
				*NewBeamEnvs(defaultSdk, defaultBeamVersion, executorConfig, preparedModDir, 0),
				*NewApplicationEnvs(
					"/app",
					defaultLaunchSite,
					defaultProjectId,
					defaultPipelinesFolder,
					defaultSDKConfigPath,
					defaultPropertyPath,
					defaultKafkaEmulatorExecutablePath,
					defaultDatasetsPath,
					defaultCleanupSnippetsFunctionsUrl,
					defaultPutSnippetFunctionsUrl,
					defaultIncrementSnippetViewsFunctionsUrl,
					&CacheEnvs{
						defaultCacheType,
						defaultCacheAddress,
						defaultCacheKeyExpirationTime,
					},
					defaultPipelineExecuteTimeout,
					defaultCacheRequestTimeout,
				)); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewEnvironment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getSdkEnvsFromOsEnvs(t *testing.T) {
	workingDir := "./"
	preparedModDir := ""
	tests := []struct {
		name      string
		want      *BeamEnvs
		envsToSet map[string]string
		wantErr   bool
	}{
		{
			name:      "Not specified beam sdk key in os envs",
			want:      NewBeamEnvs(pb.Sdk_SDK_UNSPECIFIED, defaultBeamVersion, nil, preparedModDir, defaultNumOfParallelJobs),
			envsToSet: map[string]string{},
			wantErr:   false,
		},
		{
			name:      "Default beam envs",
			want:      NewBeamEnvs(pb.Sdk_SDK_UNSPECIFIED, defaultBeamVersion, nil, preparedModDir, defaultNumOfParallelJobs),
			envsToSet: map[string]string{beamSdkKey: "SDK_UNSPECIFIED"},
			wantErr:   false,
		},
		{
			name:      "Specific sdk key in os envs",
			want:      NewBeamEnvs(pb.Sdk_SDK_JAVA, defaultBeamVersion, executorConfig, preparedModDir, defaultNumOfParallelJobs),
			envsToSet: map[string]string{beamSdkKey: "SDK_JAVA"},
			wantErr:   false,
		},
		{
			name:      "Wrong sdk key in os envs",
			want:      NewBeamEnvs(pb.Sdk_SDK_UNSPECIFIED, defaultBeamVersion, nil, preparedModDir, defaultNumOfParallelJobs),
			envsToSet: map[string]string{beamSdkKey: "SDK_J"},
			wantErr:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setOsEnvs(tt.envsToSet); err != nil {
				t.Fatalf("couldn't setup os env")
			}
			got, err := ConfigureBeamEnvs(workingDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConfigureBeamEnvs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConfigureBeamEnvs() got = %v, want %v", got, tt.want)
			}
		})
	}
	os.Clearenv()
}

func Test_getNetworkEnvsFromOsEnvs(t *testing.T) {
	tests := []struct {
		name      string
		want      *NetworkEnvs
		envsToSet map[string]string
		wantErr   bool
	}{
		{
			name: "Default values",
			want: NewNetworkEnvs(defaultIp, defaultPort, defaultProtocol),
		},
		{
			name:      "Values from os envs",
			want:      NewNetworkEnvs("12.12.12.21", 1234, "TCP"),
			envsToSet: map[string]string{serverIpKey: "12.12.12.21", serverPortKey: "1234", protocolTypeKey: "TCP"},
		},
		{
			name:      "Not int port in os env, should be default",
			want:      nil,
			envsToSet: map[string]string{serverIpKey: "12.12.12.21", serverPortKey: "1a34"},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setOsEnvs(tt.envsToSet); err != nil {
				t.Fatalf("couldn't setup os env")
			}
			got, err := GetNetworkEnvsFromOsEnvs()
			if (err != nil) != tt.wantErr {
				t.Errorf("getNetworkEnvsFromOsEnvs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getNetworkEnvsFromOsEnvs() got = %v, want %v", got, tt.want)
			}
		})
	}
	os.Clearenv()
}

func Test_getApplicationEnvsFromOsEnvs(t *testing.T) {
	hour := "1h"
	convertedTime, _ := time.ParseDuration(hour)
	tests := []struct {
		name      string
		want      *ApplicationEnvs
		wantErr   bool
		envsToSet map[string]string
	}{
		{
			name: "Working dir is provided",
			want: NewApplicationEnvs(
				"/app",
				defaultLaunchSite,
				defaultProjectId,
				defaultPipelinesFolder,
				defaultSDKConfigPath,
				defaultPropertyPath,
				defaultKafkaEmulatorExecutablePath,
				defaultDatasetsPath,
				defaultCleanupSnippetsFunctionsUrl,
				defaultPutSnippetFunctionsUrl,
				defaultIncrementSnippetViewsFunctionsUrl,
				&CacheEnvs{
					defaultCacheType,
					defaultCacheAddress,
					defaultCacheKeyExpirationTime,
				},
				defaultPipelineExecuteTimeout,
				defaultCacheRequestTimeout,
			),
			wantErr:   false,
			envsToSet: map[string]string{workingDirKey: "/app", launchSiteKey: defaultLaunchSite, projectIdKey: defaultProjectId},
		},
		{
			name:    "Working dir isn't provided",
			want:    nil,
			wantErr: true,
		},
		{
			name: "CacheKeyExpirationTimeKey is set with the correct value",
			want: NewApplicationEnvs(
				"/app",
				defaultLaunchSite,
				defaultProjectId,
				defaultPipelinesFolder,
				defaultSDKConfigPath,
				defaultPropertyPath,
				defaultKafkaEmulatorExecutablePath,
				defaultDatasetsPath,
				defaultCleanupSnippetsFunctionsUrl,
				defaultPutSnippetFunctionsUrl,
				defaultIncrementSnippetViewsFunctionsUrl,
				&CacheEnvs{
					defaultCacheType,
					defaultCacheAddress,
					convertedTime,
				},
				defaultPipelineExecuteTimeout,
				defaultCacheRequestTimeout),
			wantErr:   false,
			envsToSet: map[string]string{workingDirKey: "/app", cacheKeyExpirationTimeKey: hour},
		},
		{
			name: "CacheKeyExpirationTimeKey is set with the incorrect value",
			want: NewApplicationEnvs(
				"/app",
				defaultLaunchSite,
				defaultProjectId,
				defaultPipelinesFolder,
				defaultSDKConfigPath,
				defaultPropertyPath,
				defaultKafkaEmulatorExecutablePath,
				defaultDatasetsPath,
				defaultCleanupSnippetsFunctionsUrl,
				defaultPutSnippetFunctionsUrl,
				defaultIncrementSnippetViewsFunctionsUrl,
				&CacheEnvs{
					defaultCacheType,
					defaultCacheAddress,
					defaultCacheKeyExpirationTime,
				},
				defaultPipelineExecuteTimeout,
				defaultCacheRequestTimeout,
			),
			wantErr:   false,
			envsToSet: map[string]string{workingDirKey: "/app", cacheKeyExpirationTimeKey: "1"},
		},
		{
			name: "CacheKeyExpirationTimeKey is set with the correct value",
			want: NewApplicationEnvs(
				"/app",
				defaultLaunchSite,
				defaultProjectId,
				defaultPipelinesFolder,
				defaultSDKConfigPath,
				defaultPropertyPath,
				defaultKafkaEmulatorExecutablePath,
				defaultDatasetsPath,
				defaultCleanupSnippetsFunctionsUrl,
				defaultPutSnippetFunctionsUrl,
				defaultIncrementSnippetViewsFunctionsUrl,
				&CacheEnvs{
					defaultCacheType,
					defaultCacheAddress,
					defaultCacheKeyExpirationTime,
				},
				convertedTime,
				defaultCacheRequestTimeout,
			),
			wantErr:   false,
			envsToSet: map[string]string{workingDirKey: "/app", pipelineExecuteTimeoutKey: hour},
		},
		{
			name: "PipelineExecuteTimeoutKey is set with the incorrect value",
			want: NewApplicationEnvs(
				"/app",
				defaultLaunchSite,
				defaultProjectId,
				defaultPipelinesFolder,
				defaultSDKConfigPath,
				defaultPropertyPath,
				defaultKafkaEmulatorExecutablePath,
				defaultDatasetsPath,
				defaultCleanupSnippetsFunctionsUrl,
				defaultPutSnippetFunctionsUrl,
				defaultIncrementSnippetViewsFunctionsUrl,
				&CacheEnvs{
					defaultCacheType,
					defaultCacheAddress,
					defaultCacheKeyExpirationTime,
				},
				defaultPipelineExecuteTimeout,
				defaultCacheRequestTimeout,
			),
			wantErr:   false,
			envsToSet: map[string]string{workingDirKey: "/app", pipelineExecuteTimeoutKey: "1"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setOsEnvs(tt.envsToSet); err != nil {
				t.Fatalf("couldn't setup os env")
			}
			got, err := GetApplicationEnvsFromOsEnvs()
			if (err != nil) != tt.wantErr {
				t.Errorf("getApplicationEnvsFromOsEnvs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getApplicationEnvsFromOsEnvs() got = %v, want %v", got, tt.want)
			}
			os.Clearenv()
		})
	}
	os.Clearenv()
}

func Test_createExecutorConfig(t *testing.T) {
	type args struct {
		apacheBeamSdk pb.Sdk
		configPath    string
	}
	tests := []struct {
		name    string
		args    args
		want    *ExecutorConfig
		wantErr bool
	}{
		{
			name:    "Create executor configuration from json file",
			args:    args{apacheBeamSdk: pb.Sdk_SDK_JAVA, configPath: filepath.Join(configFolderName, pb.Sdk_SDK_JAVA.String()+jsonExt)},
			want:    executorConfig,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := createExecutorConfig(tt.args.apacheBeamSdk, tt.args.configPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("createExecutorConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createExecutorConfig() got = %v\n, want %v\n", got, tt.want)
			}
		})
	}
}

func Test_getConfigFromJson(t *testing.T) {
	type args struct {
		configPath string
	}
	tests := []struct {
		name    string
		args    args
		want    *ExecutorConfig
		wantErr bool
	}{
		{
			name:    "Get object from json",
			args:    args{filepath.Join(configFolderName, pb.Sdk_SDK_JAVA.String()+jsonExt)},
			want:    NewExecutorConfig("javac", "java", "java", []string{"-d", "bin", "-parameters", "-classpath"}, []string{"-cp", "bin:"}, []string{"-cp", "bin:", "org.junit.runner.JUnitCore"}),
			wantErr: false,
		},
		{
			name:    "Error if wrong json path",
			args:    args{filepath.Join("wrong_folder", pb.Sdk_SDK_JAVA.String()+jsonExt)},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getConfigFromJson(tt.args.configPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("getConfigFromJson() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getConfigFromJson() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfigureBeamEnvs(t *testing.T) {
	workingDir := "./"
	modDir := "/modDir"
	goExecutorConfig := NewExecutorConfig(
		"go",
		"",
		"go",
		[]string{"build", "-o", "bin"},
		[]string{},
		[]string{"test", "-v"},
	)
	pythonExecutorConfig := NewExecutorConfig(
		"",
		"python3",
		"pytest",
		[]string{},
		[]string{},
		[]string{},
	)
	scioExecutorConfig := NewExecutorConfig(
		"",
		"sbt",
		"sbt",
		[]string{},
		[]string{"runMain"},
		[]string{},
	)
	type args struct {
		workingDir string
	}
	tests := []struct {
		name      string
		args      args
		want      *BeamEnvs
		wantErr   bool
		envsToSet map[string]string
	}{
		{
			name:      "PREPARED_MOD_DIR is not specified in the environment for GO sdk",
			args:      args{workingDir: workingDir},
			want:      nil,
			wantErr:   true,
			envsToSet: map[string]string{beamSdkKey: "SDK_GO"},
		},
		{
			name:      "BeamSdkKey set to GO sdk",
			args:      args{workingDir: workingDir},
			want:      NewBeamEnvs(pb.Sdk_SDK_GO, defaultBeamVersion, goExecutorConfig, modDir, defaultNumOfParallelJobs),
			wantErr:   false,
			envsToSet: map[string]string{beamSdkKey: "SDK_GO", preparedModDirKey: modDir},
		},
		{
			name:      "Error during creating executable config",
			args:      args{workingDir: "/app"},
			want:      nil,
			wantErr:   true,
			envsToSet: map[string]string{beamSdkKey: "SDK_PYTHON"},
		},
		{
			name:      "BeamSdkKey set to Python sdk",
			want:      NewBeamEnvs(pb.Sdk_SDK_PYTHON, defaultBeamVersion, pythonExecutorConfig, modDir, defaultNumOfParallelJobs),
			wantErr:   false,
			envsToSet: map[string]string{beamSdkKey: "SDK_PYTHON"},
		},
		{
			name:      "BeamSdkKey set to SCIO sdk",
			want:      NewBeamEnvs(pb.Sdk_SDK_SCIO, defaultBeamVersion, scioExecutorConfig, modDir, defaultNumOfParallelJobs),
			wantErr:   false,
			envsToSet: map[string]string{beamSdkKey: "SDK_SCIO"},
		},
		{
			name:      "NumOfParallelJobsKey is set with a positive number",
			want:      NewBeamEnvs(pb.Sdk_SDK_PYTHON, defaultBeamVersion, pythonExecutorConfig, modDir, 1),
			wantErr:   false,
			envsToSet: map[string]string{beamSdkKey: "SDK_PYTHON", numOfParallelJobsKey: "1"},
		},
		{
			name:      "NumOfParallelJobsKey is set with a negative number",
			want:      NewBeamEnvs(pb.Sdk_SDK_PYTHON, defaultBeamVersion, pythonExecutorConfig, modDir, defaultNumOfParallelJobs),
			wantErr:   false,
			envsToSet: map[string]string{beamSdkKey: "SDK_PYTHON", numOfParallelJobsKey: "-1"},
		},
		{
			name:      "NumOfParallelJobsKey is set with incorrect value",
			want:      NewBeamEnvs(pb.Sdk_SDK_PYTHON, defaultBeamVersion, pythonExecutorConfig, modDir, defaultNumOfParallelJobs),
			wantErr:   false,
			envsToSet: map[string]string{beamSdkKey: "SDK_PYTHON", numOfParallelJobsKey: "incorrectValue"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setOsEnvs(tt.envsToSet); err != nil {
				t.Fatalf("couldn't setup os env")
			}
			got, err := ConfigureBeamEnvs(tt.args.workingDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConfigureBeamEnvs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ConfigureBeamEnvs() got = %v, want %v", got, tt.want)
			}
		})
	}
}
