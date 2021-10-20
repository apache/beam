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
)

func TestServerEnvs_GetAddress(t *testing.T) {
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
			name:   "ip and port concatenated through ':'",
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

func TestNewApplicationEnvs(t *testing.T) {
	type args struct {
		workingDir string
	}
	tests := []struct {
		name string
		args args
		want *ApplicationEnvs
	}{
		{name: "constructor for application envs", args: args{workingDir: "/app"}, want: &ApplicationEnvs{workingDir: "/app"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewApplicationEnvs(tt.args.workingDir); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewApplicationEnvs() = %v, want %v", got, tt.want)
			}
		})
	}
}
