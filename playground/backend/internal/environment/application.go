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
	"time"
)

// NetworkEnvs contains all environment variables that need to run server.
type NetworkEnvs struct {
	ip       string
	port     int
	protocol string
}

// NewNetworkEnvs constructor for NetworkEnvs
func NewNetworkEnvs(ip string, port int, protocol string) *NetworkEnvs {
	return &NetworkEnvs{ip: ip, port: port, protocol: protocol}
}

// Address returns concatenated ip and port through ':'
func (serverEnvs NetworkEnvs) Address() string {
	return fmt.Sprintf("%s:%d", serverEnvs.ip, serverEnvs.port)
}

// Protocol returns the protocol over which the server will listen
func (serverEnvs *NetworkEnvs) Protocol() string {
	return serverEnvs.protocol
}

// CacheEnvs contains all environment variables that needed to use cache
type CacheEnvs struct {
	// cacheType is type of cache (local/redis)
	cacheType string

	// this is a string with hostname:port of the cache server for redis caches
	address string

	// keyExpirationTime is expiration time for cache keys
	keyExpirationTime time.Duration
}

// CacheType returns cache type
func (ce *CacheEnvs) CacheType() string {
	return ce.cacheType
}

// Address returns address to connect to remote cache service
func (ce *CacheEnvs) Address() string {
	return ce.address
}

// KeyExpirationTime returns expiration time for cache keys
func (ce *CacheEnvs) KeyExpirationTime() time.Duration {
	return ce.keyExpirationTime
}

// NewCacheEnvs constructor for CacheEnvs
func NewCacheEnvs(cacheType, cacheAddress string, cacheExpirationTime time.Duration) *CacheEnvs {
	return &CacheEnvs{
		cacheType:         cacheType,
		address:           cacheAddress,
		keyExpirationTime: cacheExpirationTime,
	}
}

// ApplicationEnvs contains all environment variables that needed to run backend processes
type ApplicationEnvs struct {
	// workingDir is a root working directory of application.
	// This directory is different from the `pwd` of the application. It is a working directory passed as
	// a parameter where the application stores code, executables, etc.
	workingDir string

	// cacheEnvs contains environment variables for cache
	cacheEnvs *CacheEnvs

	// pipelineExecuteTimeout is timeout for code processing
	pipelineExecuteTimeout time.Duration

	// launchSite is a launch site of application
	launchSite string

	// projectId is the Google Сloud project id
	projectId string

	// pipelinesFolder is name of folder in which the pipelines resources are stored
	pipelinesFolder string

	// schemaVersion is the database schema version
	schemaVersion int

	// sdkConfigPath is a sdk configuration file
	sdkConfigPath string

	// propertyPath is the application properties path
	propertyPath string

	// kafkaEmulatorExecutablePath is the path to Kafka emulator executable file
	kafkaEmulatorExecutablePath string

	// datasetsPath is the datasets path used for Kafka examples
	datasetsPath string

	// cacheRequestTimeout is timeout to request data from cache
	cacheRequestTimeout time.Duration

	// cleanupSnippetsFunctionsUrl is the url to cleanup snippets functions
	cleanupSnippetsFunctionsUrl string

	// putSnippetFunctionUrl is the url to put snippet function
	putSnippetFunctionsUrl string

	// incrementSnippetViewsFunctionsUrl is the url to increment snippet views
	incrementSnippetViewsFunctionsUrl string
}

// NewApplicationEnvs constructor for ApplicationEnvs
func NewApplicationEnvs(
	workingDir,
	launchSite,
	projectId,
	pipelinesFolder,
	sdkConfigPath,
	propertyPath,
	kafkaEmulatorExecutablePath,
	datasetsPath,
	cleanupSnippetsFunctionsUrl,
	putSnippetFunctionsUrl,
	incrementSnippetViewsFunctionsUrl string,
	cacheEnvs *CacheEnvs,
	pipelineExecuteTimeout, cacheRequestTimeout time.Duration,
) *ApplicationEnvs {
	return &ApplicationEnvs{
		workingDir:                        workingDir,
		cacheEnvs:                         cacheEnvs,
		pipelineExecuteTimeout:            pipelineExecuteTimeout,
		launchSite:                        launchSite,
		projectId:                         projectId,
		pipelinesFolder:                   pipelinesFolder,
		sdkConfigPath:                     sdkConfigPath,
		propertyPath:                      propertyPath,
		datasetsPath:                      datasetsPath,
		kafkaEmulatorExecutablePath:       kafkaEmulatorExecutablePath,
		cacheRequestTimeout:               cacheRequestTimeout,
		cleanupSnippetsFunctionsUrl:       cleanupSnippetsFunctionsUrl,
		putSnippetFunctionsUrl:            putSnippetFunctionsUrl,
		incrementSnippetViewsFunctionsUrl: incrementSnippetViewsFunctionsUrl,
	}
}

// WorkingDir returns root working directory of application
func (ae *ApplicationEnvs) WorkingDir() string {
	return ae.workingDir
}

// CacheEnvs returns cache environments
func (ae *ApplicationEnvs) CacheEnvs() *CacheEnvs {
	return ae.cacheEnvs
}

// PipelineExecuteTimeout returns timeout for code processing
func (ae *ApplicationEnvs) PipelineExecuteTimeout() time.Duration {
	return ae.pipelineExecuteTimeout
}

// LaunchSite returns launch site of application
func (ae *ApplicationEnvs) LaunchSite() string {
	return ae.launchSite
}

// GoogleProjectId returns Google Сloud project id
func (ae *ApplicationEnvs) GoogleProjectId() string {
	return ae.projectId
}

// PipelinesFolder returns name of folder in which the pipelines resources are stored
func (ae *ApplicationEnvs) PipelinesFolder() string {
	return ae.pipelinesFolder
}

// SchemaVersion returns the database schema version
func (ae *ApplicationEnvs) SchemaVersion() int {
	return ae.schemaVersion
}

// SdkConfigPath returns sdk configuration file
func (ae *ApplicationEnvs) SdkConfigPath() string {
	return ae.sdkConfigPath
}

// PropertyPath returns the application properties path
func (ae *ApplicationEnvs) PropertyPath() string {
	return ae.propertyPath
}

// SetSchemaVersion sets the database schema version
func (ae *ApplicationEnvs) SetSchemaVersion(schemaVersion int) {
	ae.schemaVersion = schemaVersion
}

// CacheRequestTimeout returns timeout to request data from cache
func (ae *ApplicationEnvs) CacheRequestTimeout() time.Duration {
	return ae.cacheRequestTimeout
}

// DatasetsPath returns paths to datasets used for Kafka examples
func (ae *ApplicationEnvs) DatasetsPath() string {
	return ae.datasetsPath
}

func (ae *ApplicationEnvs) KafkaExecutablePath() string {
	return ae.kafkaEmulatorExecutablePath
}

// CleanupSnippetsFunctionsUrl returns the url to cleanup snippets functions
func (ae *ApplicationEnvs) CleanupSnippetsFunctionsUrl() string {
	return ae.cleanupSnippetsFunctionsUrl
}

// PutSnippetFunctionUrl returns the url to put snippet functions
func (ae *ApplicationEnvs) PutSnippetFunctionsUrl() string {
	return ae.putSnippetFunctionsUrl
}

// IncrementSnippetViewsFunctionsUrl returns the url to increment snippet views
func (ae *ApplicationEnvs) IncrementSnippetViewsFunctionsUrl() string {
	return ae.incrementSnippetViewsFunctionsUrl
}
