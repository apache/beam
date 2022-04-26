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
	"beam.apache.org/playground/backend/internal/logger"
	"encoding/json"
	"errors"
	"fmt"
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
	preparedModDirKey             = "PREPARED_MOD_DIR"
	numOfParallelJobsKey          = "NUM_PARALLEL_JOBS"
	cacheTypeKey                  = "CACHE_TYPE"
	cacheAddressKey               = "CACHE_ADDRESS"
	beamPathKey                   = "BEAM_PATH"
	cacheKeyExpirationTimeKey     = "KEY_EXPIRATION_TIME"
	pipelineExecuteTimeoutKey     = "PIPELINE_EXPIRATION_TIMEOUT"
	protocolTypeKey               = "PROTOCOL_TYPE"
	launchSiteKey                 = "LAUNCH_SITE"
	projectIdKey                  = "GOOGLE_CLOUD_PROJECT"
	pipelinesFolderKey            = "PIPELINES_FOLDER_NAME"
	defaultPipelinesFolder        = "executable_files"
	defaultLaunchSite             = "local"
	defaultProtocol               = "HTTP"
	defaultIp                     = "localhost"
	defaultPort                   = 8080
	defaultSdk                    = pb.Sdk_SDK_JAVA
	defaultBeamJarsPath           = "/opt/apache/beam/jars/*"
	defaultCacheType              = "local"
	defaultCacheAddress           = "localhost:6379"
	defaultCacheKeyExpirationTime = time.Minute * 15
	defaultPipelineExecuteTimeout = time.Minute * 10
	jsonExt                       = ".json"
	configFolderName              = "configs"
	defaultNumOfParallelJobs      = 20
	bucketNameKey                 = "BUCKET_NAME"
	defaultBucketName             = "playground-precompiled-objects"
)

// Environment operates with environment structures: NetworkEnvs, BeamEnvs, ApplicationEnvs
// Environment contains all environment variables which are used by the application
type Environment struct {
	NetworkEnvs     NetworkEnvs
	BeamSdkEnvs     BeamEnvs
	ApplicationEnvs ApplicationEnvs
}

// NewEnvironment is a constructor for Environment.
// Default values:
// LogWriters: by default using os.Stdout
// NetworkEnvs: by default using defaultIp, defaultPort and defaultProtocol from constants
// BeamEnvs: by default using pb.Sdk_SDK_JAVA
// ApplicationEnvs: required field not providing by default value
func NewEnvironment(networkEnvs NetworkEnvs, beamEnvs BeamEnvs, appEnvs ApplicationEnvs) *Environment {
	svc := Environment{}
	svc.NetworkEnvs = networkEnvs
	svc.BeamSdkEnvs = beamEnvs
	svc.ApplicationEnvs = appEnvs

	return &svc
}

// GetApplicationEnvsFromOsEnvs returns ApplicationEnvs.
// Lookups in os environment variables and tries to take values for all (exclude working dir) ApplicationEnvs parameters.
// In case some value doesn't exist sets default values:
// 	- pipeline execution timeout: 10 minutes
//	- cache expiration time: 15 minutes
//	- type of cache: local
//	- cache address: localhost:6379
// If os environment variables don't contain a value for app working dir - returns error.
func GetApplicationEnvsFromOsEnvs() (*ApplicationEnvs, error) {
	pipelineExecuteTimeout := defaultPipelineExecuteTimeout
	cacheExpirationTime := defaultCacheKeyExpirationTime
	cacheType := getEnv(cacheTypeKey, defaultCacheType)
	cacheAddress := getEnv(cacheAddressKey, defaultCacheAddress)
	launchSite := getEnv(launchSiteKey, defaultLaunchSite)
	projectId := os.Getenv(projectIdKey)
	pipelinesFolder := getEnv(pipelinesFolderKey, defaultPipelinesFolder)
	bucketName := getEnv(bucketNameKey, defaultBucketName)

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
		return NewApplicationEnvs(value, launchSite, projectId, pipelinesFolder, NewCacheEnvs(cacheType, cacheAddress, cacheExpirationTime), pipelineExecuteTimeout, bucketName), nil
	}
	return nil, errors.New("APP_WORK_DIR env should be provided with os.env")
}

// GetNetworkEnvsFromOsEnvs returns NetworkEnvs.
// Lookups in os environment variables and takes values for ip and port.
// In case some value doesn't exist sets default values:
//  - ip:	localhost
//  - port: 8080
func GetNetworkEnvsFromOsEnvs() (*NetworkEnvs, error) {
	ip := getEnv(serverIpKey, defaultIp)
	port := defaultPort
	protocol := getEnv(protocolTypeKey, defaultProtocol)
	var err error
	if value, present := os.LookupEnv(serverPortKey); present {
		port, err = strconv.Atoi(value)
		if err != nil {
			return nil, err
		}
	}
	return NewNetworkEnvs(ip, port, protocol), nil
}

// ConfigureBeamEnvs returns BeamEnvs.
// Lookups in os environment variables and takes value for Apache Beam SDK.
// If os environment variables don't contain a value for Apache Beam SDK - returns error.
// Configures ExecutorConfig with config file.
func ConfigureBeamEnvs(workDir string) (*BeamEnvs, error) {
	sdk := pb.Sdk_SDK_UNSPECIFIED
	preparedModDir, modDirExist := os.LookupEnv(preparedModDirKey)

	numOfParallelJobs := defaultNumOfParallelJobs
	if value, present := os.LookupEnv(numOfParallelJobsKey); present {
		convertedValue, err := strconv.Atoi(value)
		if err != nil {
			logger.Errorf("Incorrect value for %s. Should be integer. Will be used default value: %d", numOfParallelJobsKey, defaultNumOfParallelJobs)
		} else {
			if convertedValue <= 0 {
				logger.Errorf("Incorrect value for %s. Should be a positive integer value but it is %d. Will be used default value: %d", numOfParallelJobsKey, convertedValue, defaultNumOfParallelJobs)
			} else {
				numOfParallelJobs = convertedValue
			}
		}
	}

	if value, present := os.LookupEnv(beamSdkKey); present {

		switch value {
		case pb.Sdk_SDK_JAVA.String():
			sdk = pb.Sdk_SDK_JAVA
		case pb.Sdk_SDK_GO.String():
			sdk = pb.Sdk_SDK_GO
			if !modDirExist {
				return nil, errors.New("env PREPARED_MOD_DIR must be specified in the environment variables for GO sdk")
			}
		case pb.Sdk_SDK_PYTHON.String():
			sdk = pb.Sdk_SDK_PYTHON
		case pb.Sdk_SDK_SCIO.String():
			sdk = pb.Sdk_SDK_SCIO
		}
	}
	if sdk == pb.Sdk_SDK_UNSPECIFIED {
		return NewBeamEnvs(sdk, nil, preparedModDir, numOfParallelJobs), nil
	}
	configPath := filepath.Join(workDir, configFolderName, sdk.String()+jsonExt)
	executorConfig, err := createExecutorConfig(sdk, configPath)
	if err != nil {
		return nil, err
	}
	return NewBeamEnvs(sdk, executorConfig, preparedModDir, numOfParallelJobs), nil
}

// createExecutorConfig creates ExecutorConfig that corresponds to specific Apache Beam SDK.
// Configures ExecutorConfig with config file which is located at configPath.
func createExecutorConfig(apacheBeamSdk pb.Sdk, configPath string) (*ExecutorConfig, error) {
	executorConfig, err := getConfigFromJson(configPath)
	if err != nil {
		return nil, err
	}
	switch apacheBeamSdk {
	case pb.Sdk_SDK_JAVA:
		args, err := ConcatBeamJarsToString()
		if err != nil {
			return nil, fmt.Errorf("error during proccessing jars: %s", err.Error())
		}
		executorConfig.CompileArgs = append(executorConfig.CompileArgs, args)
		executorConfig.RunArgs[1] = fmt.Sprintf("%s%s", executorConfig.RunArgs[1], args)
		executorConfig.TestArgs[1] = fmt.Sprintf("%s%s", executorConfig.TestArgs[1], args)
	case pb.Sdk_SDK_GO:
		// Go sdk doesn't need any additional arguments from the config file
	case pb.Sdk_SDK_PYTHON:
		// Python sdk doesn't need any additional arguments from the config file
	case pb.Sdk_SDK_SCIO:
		// Scala sdk doesn't need any additional arguments from the config file
	}
	return executorConfig, nil
}

func ConcatBeamJarsToString() (string, error) {
	jars, err := filepath.Glob(getEnv(beamPathKey, defaultBeamJarsPath))
	if err != nil {
		return "", err
	}
	args := strings.Join(jars, ":")
	return args, nil
}

// getConfigFromJson reads a json file to ExecutorConfig
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

// getEnv returns an environment variable or default value
func getEnv(key, defaultValue string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return defaultValue
}
