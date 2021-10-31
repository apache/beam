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
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	serverIpKey                   = "SERVER_IP"
	serverPortKey                 = "SERVER_PORT"
	beamSdkKey                    = "BEAM_SDK"
	workingDirKey                 = "APP_WORK_DIR"
	cacheTypeKey                  = "CACHE_TYPE"
	cacheAddressKey               = "CACHE_ADDRESS"
	beamPathKey                   = "BEAM_PATH"
	beamRunnerKey                 = "BEAM_RUNNER"
	SLF4jKey                      = "SLF4J"
	cacheKeyExpirationTimeKey     = "KEY_EXPIRATION_TIME"
	pipelineExecuteTimeoutKey     = "PIPELINE_EXPIRATION_TIMEOUT"
	defaultIp                     = "localhost"
	defaultPort                   = 8080
	defaultSdk                    = pb.Sdk_SDK_JAVA
	defaultBeamSdkPath            = "/opt/apache/beam/jars/beam-sdks-java-harness.jar"
	defaultCacheType              = "local"
	defaultCacheAddress           = "localhost:6379"
	defaultCacheKeyExpirationTime = time.Minute * 15
	defaultPipelineExecuteTimeout = time.Minute * 10
	defaultBeamRunner             = "/opt/apache/beam/jars/beam-runners-direct.jar"
	defaultSLF4j                  = "/opt/apache/beam/jars/slf4j-jdk14.jar"
	jsonExt                       = ".json"
	configFolderName              = "configs"
)

// Environment operates with environment structures: NetworkEnvs, BeamEnvs, ApplicationEnvs
type Environment struct {
	NetworkEnvs     NetworkEnvs
	BeamSdkEnvs     BeamEnvs
	ApplicationEnvs ApplicationEnvs
}

// NewEnvironment is a constructor for Environment.
// Default values:
// LogWriters: by default using os.Stdout
// NetworkEnvs: by default using defaultIp and defaultPort from constants
// BeamEnvs: by default using pb.Sdk_SDK_JAVA
// ApplicationEnvs: required field not providing by default value
func NewEnvironment(networkEnvs NetworkEnvs, beamEnvs BeamEnvs, appEnvs ApplicationEnvs) *Environment {
	svc := Environment{}
	svc.NetworkEnvs = networkEnvs
	svc.BeamSdkEnvs = beamEnvs
	svc.ApplicationEnvs = appEnvs

	return &svc
}

//GetApplicationEnvsFromOsEnvs lookups in os environment variables and takes value for app working dir. If not exists - return error
func GetApplicationEnvsFromOsEnvs() (*ApplicationEnvs, error) {
	pipelineExecuteTimeout := defaultPipelineExecuteTimeout
	cacheExpirationTime := defaultCacheKeyExpirationTime
	cacheType := getEnv(cacheTypeKey, defaultCacheType)
	cacheAddress := getEnv(cacheAddressKey, defaultCacheAddress)

	if value, present := os.LookupEnv(cacheKeyExpirationTimeKey); present {
		if converted, err := time.ParseDuration(value); err == nil {
			cacheExpirationTime = converted
		} else {
			log.Printf("couldn't convert provided cache expiration time. Using default %s\n", defaultCacheKeyExpirationTime)
		}
	}
	if value, present := os.LookupEnv(pipelineExecuteTimeoutKey); present {
		if converted, err := time.ParseDuration(value); err == nil {
			pipelineExecuteTimeout = converted
		} else {
			log.Printf("couldn't convert provided pipeline execute timeout. Using default %s\n", defaultPipelineExecuteTimeout)
		}
	}

	if value, present := os.LookupEnv(workingDirKey); present {
		return NewApplicationEnvs(value, NewCacheEnvs(cacheType, cacheAddress, cacheExpirationTime), pipelineExecuteTimeout), nil
	}
	return nil, errors.New("APP_WORK_DIR env should be provided with os.env")
}

// GetNetworkEnvsFromOsEnvs lookups in os environment variables and takes value for ip and port. If not exists - using default
func GetNetworkEnvsFromOsEnvs() (*NetworkEnvs, error) {
	ip := getEnv(serverIpKey, defaultIp)
	port := defaultPort
	var err error
	if value, present := os.LookupEnv(serverPortKey); present {
		port, err = strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
	}
	return NewNetworkEnvs(ip, port), nil
}

// ConfigureBeamEnvs lookups in os environment variables and takes value for Apache Beam SDK. If not exists - using default
func ConfigureBeamEnvs(workDir string) (*BeamEnvs, error) {
	sdk := pb.Sdk_SDK_UNSPECIFIED
	if value, present := os.LookupEnv(beamSdkKey); present {

		switch value {
		case pb.Sdk_SDK_JAVA.String():
			sdk = pb.Sdk_SDK_JAVA
		case pb.Sdk_SDK_GO.String():
			sdk = pb.Sdk_SDK_GO
		case pb.Sdk_SDK_PYTHON.String():
			sdk = pb.Sdk_SDK_PYTHON
		case pb.Sdk_SDK_SCIO.String():
			sdk = pb.Sdk_SDK_SCIO
		}
	}
	if sdk == pb.Sdk_SDK_UNSPECIFIED {
		return nil, errors.New("env BEAM_SDK must be specified in the environment variables")
	}
	configPath := filepath.Join(workDir, configFolderName, sdk.String()+jsonExt)
	executorConfig, err := createExecutorConfig(sdk, configPath)
	if err != nil {
		return nil, err
	}
	return NewBeamEnvs(sdk, executorConfig), nil
}

//createExecutorConfig creates ExecutorConfig object that corresponds to specific apache beam sdk
func createExecutorConfig(apacheBeamSdk pb.Sdk, configPath string) (*ExecutorConfig, error) {
	executorConfig, err := getConfigFromJson(configPath)
	if err != nil {
		return nil, err
	}
	switch apacheBeamSdk {
	case pb.Sdk_SDK_JAVA:
		executorConfig.CompileArgs = append(executorConfig.CompileArgs, getEnv(beamPathKey, defaultBeamSdkPath))
		jars := strings.Join([]string{
			getEnv(beamPathKey, defaultBeamSdkPath),
			getEnv(beamRunnerKey, defaultBeamRunner),
			getEnv(SLF4jKey, defaultSLF4j),
		}, ":")
		executorConfig.RunArgs[1] += jars
	case pb.Sdk_SDK_GO:
		return nil, errors.New("not yet supported")
	case pb.Sdk_SDK_PYTHON:
		return nil, errors.New("not yet supported")
	case pb.Sdk_SDK_SCIO:
		return nil, errors.New("not yet supported")
	}
	return executorConfig, nil
}

//getConfigFromJson reads a json file to ExecutorConfig struct
func getConfigFromJson(configPath string) (*ExecutorConfig, error) {
	file, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	executorConfig := ExecutorConfig{}
	err = json.Unmarshal(file, &executorConfig)
	if err != nil {
		return nil, err
	}
	return &executorConfig, err
}

//getEnv returns a environment variable or default value
func getEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}
