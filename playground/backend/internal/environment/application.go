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
	ip   string
	port int
}

// NewNetworkEnvs constructor for NetworkEnvs
func NewNetworkEnvs(ip string, port int) *NetworkEnvs {
	return &NetworkEnvs{ip: ip, port: port}
}

// Address returns concatenated ip and port through ':'
func (serverEnvs NetworkEnvs) Address() string {
	return fmt.Sprintf("%s:%d", serverEnvs.ip, serverEnvs.port)
}

//CacheEnvs contains all environment variables that needed to use cache
type CacheEnvs struct {
	cacheType         string
	address           string
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

// KeyExpirationTime returns cacheExpirationTime
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

//ApplicationEnvs contains all environment variables that needed to run backend processes
type ApplicationEnvs struct {
	workingDir             string
	cacheEnvs              *CacheEnvs
	pipelineExecuteTimeout time.Duration
}

// NewApplicationEnvs constructor for ApplicationEnvs
func NewApplicationEnvs(workingDir string, cacheEnvs *CacheEnvs, pipelineExecuteTimeout time.Duration) *ApplicationEnvs {
	return &ApplicationEnvs{
		workingDir:             workingDir,
		cacheEnvs:              cacheEnvs,
		pipelineExecuteTimeout: pipelineExecuteTimeout,
	}
}

// WorkingDir returns workingDir
func (ae *ApplicationEnvs) WorkingDir() string {
	return ae.workingDir
}

func (ae *ApplicationEnvs) CacheEnvs() CacheEnvs {
	return *ae.cacheEnvs
}

// PipelineExecuteTimeout returns pipelineExecuteTimeout
func (ae *ApplicationEnvs) PipelineExecuteTimeout() time.Duration {
	return ae.pipelineExecuteTimeout
}
