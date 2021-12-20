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
	playground "beam.apache.org/playground/backend/internal/api/v1"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

const (
	javaConfig = "{\n  \"compile_cmd\": \"javac\",\n  \"run_cmd\": \"java\",\n  \"test_cmd\": \"java\",\n  \"compile_args\": [\n    \"-d\",\n    \"bin\",\n    \"-classpath\"\n  ],\n  \"run_args\": [\n    \"-cp\",\n    \"bin:\"\n  ],\n  \"test_args\": [\n    \"-cp\",\n    \"bin:\",\n    \"JUnit\"\n  ]\n}"
	jarsPath   = "/opt/apache/beam/jars/*"
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
	filePath := filepath.Join(configFolderName, defaultSdk.String()+jsonExt)
	err = os.WriteFile(filePath, []byte(javaConfig), 0600)
	if err != nil {
		return err
	}
	os.Clearenv()

	executorConfig = NewExecutorConfig(
		"javac", "java", "java",
		[]string{"-d", "bin", "-classpath", jarsPath},
		[]string{"-cp", "bin:" + jarsPath},
		[]string{"-cp", "bin:" + jarsPath, "JUnit"},
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
		{name: "create env service with default envs", want: &Environment{
			NetworkEnvs:     *NewNetworkEnvs(defaultIp, defaultPort, defaultProtocol),
			BeamSdkEnvs:     *NewBeamEnvs(defaultSdk, executorConfig, preparedModDir, 0),
			ApplicationEnvs: *NewApplicationEnvs("/app", &CacheEnvs{defaultCacheType, defaultCacheAddress, defaultCacheKeyExpirationTime}, defaultPipelineExecuteTimeout),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEnvironment(
				*NewNetworkEnvs(defaultIp, defaultPort, defaultProtocol),
				*NewBeamEnvs(defaultSdk, executorConfig, preparedModDir, 0),
				*NewApplicationEnvs("/app", &CacheEnvs{defaultCacheType, defaultCacheAddress, defaultCacheKeyExpirationTime}, defaultPipelineExecuteTimeout)); !reflect.DeepEqual(got, tt.want) {
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
			name:      "not specified beam sdk key in os envs",
			want:      nil,
			envsToSet: map[string]string{},
			wantErr:   true,
		},
		{
			name:      "default beam envs",
			want:      NewBeamEnvs(defaultSdk, executorConfig, preparedModDir, defaultCountOfPossibleCodeProcessing),
			envsToSet: map[string]string{beamSdkKey: "SDK_JAVA"},
			wantErr:   false,
		},
		{
			name:      "specific sdk key in os envs",
			want:      NewBeamEnvs(defaultSdk, executorConfig, preparedModDir, defaultCountOfPossibleCodeProcessing),
			envsToSet: map[string]string{beamSdkKey: "SDK_JAVA"},
			wantErr:   false,
		},
		{
			name:      "wrong sdk key in os envs",
			want:      nil,
			envsToSet: map[string]string{beamSdkKey: "SDK_J"},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setOsEnvs(tt.envsToSet); err != nil {
				t.Fatalf("couldn't setup os env")
			}
			got, err := ConfigureBeamEnvs(workingDir)
			if (err != nil) != tt.wantErr {
				t.Errorf("getSdkEnvsFromOsEnvs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSdkEnvsFromOsEnvs() got = %v, want %v", got, tt.want)
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
			name: "default values",
			want: NewNetworkEnvs(defaultIp, defaultPort, defaultProtocol),
		},
		{
			name:      "values from os envs",
			want:      NewNetworkEnvs("12.12.12.21", 1234, "TCP"),
			envsToSet: map[string]string{serverIpKey: "12.12.12.21", serverPortKey: "1234", protocolTypeKey: "TCP"},
		},
		{
			name:      "not int port in os env, should be default",
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
	tests := []struct {
		name      string
		want      *ApplicationEnvs
		wantErr   bool
		envsToSet map[string]string
	}{
		{name: "working dir is provided", want: NewApplicationEnvs("/app", &CacheEnvs{defaultCacheType, defaultCacheAddress, defaultCacheKeyExpirationTime}, defaultPipelineExecuteTimeout), wantErr: false, envsToSet: map[string]string{workingDirKey: "/app"}},
		{name: "working dir isn't provided", want: nil, wantErr: true},
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
		apacheBeamSdk playground.Sdk
		configPath    string
	}
	tests := []struct {
		name    string
		args    args
		want    *ExecutorConfig
		wantErr bool
	}{
		{
			name:    "create executor configuration from json file",
			args:    args{apacheBeamSdk: defaultSdk, configPath: filepath.Join(configFolderName, defaultSdk.String()+jsonExt)},
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
				t.Errorf("createExecutorConfig() got = %v, want %v", got, tt.want)
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
			name:    "get object from json",
			args:    args{filepath.Join(configFolderName, defaultSdk.String()+jsonExt)},
			want:    NewExecutorConfig("javac", "java", "java", []string{"-d", "bin", "-classpath"}, []string{"-cp", "bin:"}, []string{"-cp", "bin:", "JUnit"}),
			wantErr: false,
		},
		{
			name:    "error if wrong json path",
			args:    args{filepath.Join("wrong_folder", defaultSdk.String()+jsonExt)},
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
