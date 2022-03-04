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
	"reflect"
	"testing"
	"time"
)

func TestNetworkEnvs_Address(t *testing.T) {
	type fields struct {
		ip   string
		port int
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name:   "Ip and port concatenated through ':'",
			fields: fields{ip: defaultIp, port: defaultPort},
			want:   fmt.Sprintf("%s:%d", defaultIp, defaultPort),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverEnvs := NetworkEnvs{
				ip:   tt.fields.ip,
				port: tt.fields.port,
			}
			if got := serverEnvs.Address(); got != tt.want {
				t.Errorf("Address() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCacheEnvs_CacheType(t *testing.T) {
	type fields struct {
		cacheType         string
		address           string
		keyExpirationTime time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Get cache type",
			fields: fields{
				cacheType:         "MOCK_CACHE_TYPE",
				address:           "MOCK_ADDRESS",
				keyExpirationTime: 0,
			},
			want: "MOCK_CACHE_TYPE",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ce := &CacheEnvs{
				cacheType:         tt.fields.cacheType,
				address:           tt.fields.address,
				keyExpirationTime: tt.fields.keyExpirationTime,
			}
			if got := ce.CacheType(); got != tt.want {
				t.Errorf("CacheType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCacheEnvs_Address(t *testing.T) {
	type fields struct {
		cacheType         string
		address           string
		keyExpirationTime time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Get cache address",
			fields: fields{
				cacheType:         "MOCK_CACHE_TYPE",
				address:           "MOCK_ADDRESS",
				keyExpirationTime: 0,
			},
			want: "MOCK_ADDRESS",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ce := &CacheEnvs{
				cacheType:         tt.fields.cacheType,
				address:           tt.fields.address,
				keyExpirationTime: tt.fields.keyExpirationTime,
			}
			if got := ce.Address(); got != tt.want {
				t.Errorf("Address() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCacheEnvs_KeyExpirationTime(t *testing.T) {
	type fields struct {
		cacheType         string
		address           string
		keyExpirationTime time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Duration
	}{
		{
			name: "Get expiration time for cache keys",
			fields: fields{
				cacheType:         "MOCK_CACHE_TYPE",
				address:           "MOCK_ADDRESS",
				keyExpirationTime: 0,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ce := &CacheEnvs{
				cacheType:         tt.fields.cacheType,
				address:           tt.fields.address,
				keyExpirationTime: tt.fields.keyExpirationTime,
			}
			if got := ce.KeyExpirationTime(); got != tt.want {
				t.Errorf("KeyExpirationTime() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplicationEnvs_WorkingDir(t *testing.T) {
	type fields struct {
		workingDir             string
		cacheEnvs              *CacheEnvs
		pipelineExecuteTimeout time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Get working directory",
			fields: fields{
				workingDir:             "MOCK_WORKING_DIR",
				cacheEnvs:              &CacheEnvs{},
				pipelineExecuteTimeout: 0,
			},
			want: "MOCK_WORKING_DIR",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ae := &ApplicationEnvs{
				workingDir:             tt.fields.workingDir,
				cacheEnvs:              tt.fields.cacheEnvs,
				pipelineExecuteTimeout: tt.fields.pipelineExecuteTimeout,
			}
			if got := ae.WorkingDir(); got != tt.want {
				t.Errorf("WorkingDir() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplicationEnvs_CacheEnvs(t *testing.T) {
	type fields struct {
		workingDir             string
		cacheEnvs              *CacheEnvs
		pipelineExecuteTimeout time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   *CacheEnvs
	}{
		{
			name: "Get cache environment variables",
			fields: fields{
				workingDir:             "MOCK_WORKING_DIR",
				cacheEnvs:              &CacheEnvs{},
				pipelineExecuteTimeout: 0,
			},
			want: &CacheEnvs{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ae := &ApplicationEnvs{
				workingDir:             tt.fields.workingDir,
				cacheEnvs:              tt.fields.cacheEnvs,
				pipelineExecuteTimeout: tt.fields.pipelineExecuteTimeout,
			}
			if got := ae.CacheEnvs(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CacheEnvs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplicationEnvs_PipelineExecuteTimeout(t *testing.T) {
	type fields struct {
		workingDir             string
		cacheEnvs              *CacheEnvs
		pipelineExecuteTimeout time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Duration
	}{
		{
			name: "Get pipeline execute timeout",
			fields: fields{
				workingDir:             "MOCK_WORKING_DIR",
				cacheEnvs:              &CacheEnvs{},
				pipelineExecuteTimeout: 0,
			},
			want: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ae := &ApplicationEnvs{
				workingDir:             tt.fields.workingDir,
				cacheEnvs:              tt.fields.cacheEnvs,
				pipelineExecuteTimeout: tt.fields.pipelineExecuteTimeout,
			}
			if got := ae.PipelineExecuteTimeout(); got != tt.want {
				t.Errorf("PipelineExecuteTimeout() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplicationEnvs_LaunchSite(t *testing.T) {
	type fields struct {
		workingDir             string
		cacheEnvs              *CacheEnvs
		pipelineExecuteTimeout time.Duration
		launchSite             string
		googleProjectId        string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			// Test case with calling LaunchSite method.
			// As a result, want to receive an expected launch site.
			name: "Get launch site",
			fields: fields{
				workingDir:             "",
				cacheEnvs:              &CacheEnvs{},
				pipelineExecuteTimeout: 0,
				launchSite:             "local",
			},
			want: "local",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ae := &ApplicationEnvs{
				workingDir:             tt.fields.workingDir,
				cacheEnvs:              tt.fields.cacheEnvs,
				pipelineExecuteTimeout: tt.fields.pipelineExecuteTimeout,
				launchSite:             tt.fields.launchSite,
				projectId:              tt.fields.googleProjectId,
			}
			if got := ae.LaunchSite(); got != tt.want {
				t.Errorf("LaunchSite() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplicationEnvs_GoogleProjectId(t *testing.T) {
	type fields struct {
		workingDir             string
		cacheEnvs              *CacheEnvs
		pipelineExecuteTimeout time.Duration
		googleProjectId        string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			// Test case with calling GoogleProjectId method.
			// As a result, want to receive an expected project id.
			name: "Get google project id",
			fields: fields{
				workingDir:             "",
				cacheEnvs:              &CacheEnvs{},
				pipelineExecuteTimeout: 0,
				googleProjectId:        "MOCK_GOOGLE_PROJECT_ID",
			},
			want: "MOCK_GOOGLE_PROJECT_ID",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ae := &ApplicationEnvs{
				workingDir:             tt.fields.workingDir,
				cacheEnvs:              tt.fields.cacheEnvs,
				pipelineExecuteTimeout: tt.fields.pipelineExecuteTimeout,
				projectId:              tt.fields.googleProjectId,
			}
			if got := ae.GoogleProjectId(); got != tt.want {
				t.Errorf("GoogleProjectId() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApplicationEnvs_PipelinesFolder(t *testing.T) {
	type fields struct {
		workingDir             string
		cacheEnvs              *CacheEnvs
		pipelineExecuteTimeout time.Duration
		pipelinesFolder        string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			// Test case with calling PipelinesFolder method.
			// As a result, want to receive an expected name of pipelines folder.
			name: "Get name of pipeline folder",
			fields: fields{
				workingDir:             "",
				cacheEnvs:              &CacheEnvs{},
				pipelineExecuteTimeout: 0,
				pipelinesFolder:        "MOCK_PIPELINES_FOLDER",
			},
			want: "MOCK_PIPELINES_FOLDER",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ae := &ApplicationEnvs{
				workingDir:             tt.fields.workingDir,
				cacheEnvs:              tt.fields.cacheEnvs,
				pipelineExecuteTimeout: tt.fields.pipelineExecuteTimeout,
				pipelinesFolder:        tt.fields.pipelinesFolder,
			}
			if got := ae.PipelinesFolder(); got != tt.want {
				t.Errorf("PipelinesFolder() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNetworkEnvs_Protocol(t *testing.T) {
	protocol := "HTTP"
	type fields struct {
		ip       string
		port     int
		protocol string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "Get server protocol",
			fields: fields{
				ip:       "",
				port:     0,
				protocol: protocol,
			},
			want: protocol,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverEnvs := &NetworkEnvs{
				ip:       tt.fields.ip,
				port:     tt.fields.port,
				protocol: tt.fields.protocol,
			}
			if got := serverEnvs.Protocol(); got != tt.want {
				t.Errorf("Protocol() = %v, want %v", got, tt.want)
			}
		})
	}
}
