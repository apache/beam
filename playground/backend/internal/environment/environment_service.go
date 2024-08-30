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

	pb "beam.apache.org/playground/backend/internal/api/v1"
	"beam.apache.org/playground/backend/internal/logger"
)

const (
	serverIpKey                              = "SERVER_IP"
	serverPortKey                            = "SERVER_PORT"
	beamSdkKey                               = "BEAM_SDK"
	beamVersionKey                           = "BEAM_VERSION"
	workingDirKey                            = "APP_WORK_DIR"
	preparedModDirKey                        = "PREPARED_MOD_DIR"
	numOfParallelJobsKey                     = "NUM_PARALLEL_JOBS"
	cacheTypeKey                             = "CACHE_TYPE"
	cacheAddressKey                          = "CACHE_ADDRESS"
	beamPathKey                              = "BEAM_PATH"
	cacheKeyExpirationTimeKey                = "KEY_EXPIRATION_TIME"
	pipelineExecuteTimeoutKey                = "PIPELINE_EXPIRATION_TIMEOUT"
	protocolTypeKey                          = "PROTOCOL_TYPE"
	launchSiteKey                            = "LAUNCH_SITE"
	projectIdKey                             = "GOOGLE_CLOUD_PROJECT"
	pipelinesFolderKey                       = "PIPELINES_FOLDER_NAME"
	defaultPipelinesFolder                   = "executable_files"
	defaultLaunchSite                        = "local"
	defaultProtocol                          = "HTTP"
	defaultIp                                = "localhost"
	defaultPort                              = 8080
	defaultSdk                               = pb.Sdk_SDK_UNSPECIFIED
	defaultBeamVersion                       = "<unknown>"
	defaultBeamJarsPath                      = "/opt/apache/beam/jars/*"
	defaultDatasetsPath                      = "/opt/playground/backend/datasets"
	defaultKafkaEmulatorExecutablePath       = "/opt/playground/backend/kafka-emulator/beam-playground-kafka-emulator.jar"
	defaultCacheType                         = "local"
	defaultCacheAddress                      = "localhost:6379"
	defaultCacheKeyExpirationTime            = time.Minute * 15
	defaultPipelineExecuteTimeout            = time.Minute * 10
	jsonExt                                  = ".json"
	configFolderName                         = "configs"
	defaultNumOfParallelJobs                 = 20
	SDKConfigPathKey                         = "SDK_CONFIG"
	defaultSDKConfigPath                     = "../sdks.yaml"
	propertyPathKey                          = "PROPERTY_PATH"
	datasetsPathKey                          = "DATASETS_PATH"
	kafkaEmulatorExecutablePathKey           = "KAFKA_EMULATOR_EXECUTABLE_PATH"
	defaultPropertyPath                      = "."
	cacheRequestTimeoutKey                   = "CACHE_REQUEST_TIMEOUT"
	defaultCacheRequestTimeout               = time.Second * 5
	cleanupSnippetsFunctionsUrlKey           = "CLEANUP_SNIPPETS_FUNCTIONS_URL"
	defaultCleanupSnippetsFunctionsUrl       = "http://cleanup_snippets:8080/"
	putSnippetFunctionsUrlKey                = "PUT_SNIPPET_FUNCTIONS_URL"
	defaultPutSnippetFunctionsUrl            = "http://put_snippet:8080/"
	incrementSnippetViewsFunctionsUrlKey     = "INCREMENT_SNIPPET_VIEWS_FUNCTIONS_URL"
	defaultIncrementSnippetViewsFunctionsUrl = "http://increment_snippet_views:8080/"
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
//   - pipeline execution timeout: 10 minutes
//   - cache expiration time: 15 minutes
//   - type of cache: local
//   - cache address: localhost:6379
//
// If os environment variables don't contain a value for app working dir - returns error.
func GetApplicationEnvsFromOsEnvs() (*ApplicationEnvs, error) {
	pipelineExecuteTimeout := getEnvAsDuration(pipelineExecuteTimeoutKey, defaultPipelineExecuteTimeout, "couldn't convert provided pipeline execute timeout. Using default %s\n")
	cacheExpirationTime := getEnvAsDuration(cacheKeyExpirationTimeKey, defaultCacheKeyExpirationTime, "couldn't convert provided cache expiration time. Using default %s\n")
	cacheType := getEnv(cacheTypeKey, defaultCacheType)
	cacheAddress := getEnv(cacheAddressKey, defaultCacheAddress)
	launchSite := getEnv(launchSiteKey, defaultLaunchSite)
	projectId := os.Getenv(projectIdKey)
	pipelinesFolder := getEnv(pipelinesFolderKey, defaultPipelinesFolder)
	sdkConfigPath := getEnv(SDKConfigPathKey, defaultSDKConfigPath)
	propertyPath := getEnv(propertyPathKey, defaultPropertyPath)
	datasetsPath := getEnv(datasetsPathKey, defaultDatasetsPath)
	kafkaEmulatorExecutablePath := getEnv(kafkaEmulatorExecutablePathKey, defaultKafkaEmulatorExecutablePath)
	cacheRequestTimeout := getEnvAsDuration(cacheRequestTimeoutKey, defaultCacheRequestTimeout, "couldn't convert provided cache request timeout. Using default %s\n")
	cleanupSnippetsFunctionsUrl := getEnv(cleanupSnippetsFunctionsUrlKey, defaultCleanupSnippetsFunctionsUrl)
	putSnippetFunctionsUrl := getEnv(putSnippetFunctionsUrlKey, defaultPutSnippetFunctionsUrl)
	incrementSnippetViewsFunctionsUrl := getEnv(incrementSnippetViewsFunctionsUrlKey, defaultIncrementSnippetViewsFunctionsUrl)

	if value, present := os.LookupEnv(workingDirKey); present {
		return NewApplicationEnvs(
			value,
			launchSite,
			projectId,
			pipelinesFolder,
			sdkConfigPath,
			propertyPath,
			kafkaEmulatorExecutablePath,
			datasetsPath,
			cleanupSnippetsFunctionsUrl,
			putSnippetFunctionsUrl,
			incrementSnippetViewsFunctionsUrl,
			NewCacheEnvs(
				cacheType,
				cacheAddress,
				cacheExpirationTime,
			),
			pipelineExecuteTimeout,
			cacheRequestTimeout,
		), nil
	}
	return nil, errors.New("APP_WORK_DIR env should be provided with os.env")
}

// GetNetworkEnvsFromOsEnvs returns NetworkEnvs.
// Lookups in os environment variables and takes values for ip and port.
// In case some value doesn't exist sets default values:
//   - ip:	localhost
//   - port: 8080
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
	numOfParallelJobs := getEnvAsInt(numOfParallelJobsKey, defaultNumOfParallelJobs)

	beamVersion := getEnv(beamVersionKey, defaultBeamVersion)

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
		return NewBeamEnvs(sdk, beamVersion, nil, preparedModDir, numOfParallelJobs), nil
	}
	configPath := filepath.Join(workDir, configFolderName, sdk.String()+jsonExt)
	executorConfig, err := createExecutorConfig(sdk, configPath)
	if err != nil {
		return nil, err
	}
	return NewBeamEnvs(sdk, beamVersion, executorConfig, preparedModDir, numOfParallelJobs), nil
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
			return nil, fmt.Errorf("error during processing jars: %s", err.Error())
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

// getEnvAsInt returns an environment variable or default value as integer
func getEnvAsInt(key string, defaultValue int) int {
	if value, present := os.LookupEnv(key); present {
		convertedValue, err := strconv.Atoi(value)
		if err != nil {
			logger.Errorf("Incorrect value for %s. Should be integer. Will be used default value: %d", key, defaultValue)
			return defaultValue
		} else {
			if convertedValue <= 0 {
				logger.Errorf("Incorrect value for %s. Should be a positive integer value but it is %d. Will be used default value: %d", key, convertedValue, defaultValue)
				return defaultValue
			} else {
				return convertedValue
			}
		}
	}
	return defaultValue
}

// getEnvAsDuration returns an environment variable or default value as duration
func getEnvAsDuration(key string, defaultValue time.Duration, errMsg string) time.Duration {
	if value, present := os.LookupEnv(key); present {
		if converted, err := time.ParseDuration(value); err == nil {
			return converted
		} else {
			log.Printf(errMsg, defaultValue)
		}
	}
	return defaultValue
}
