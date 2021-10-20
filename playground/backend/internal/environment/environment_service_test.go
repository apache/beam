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
	pb "beam.apache.org/playground/backend/internal/api"
	"os"
	"reflect"
	"testing"
)

func setOsEnvs(envsToSet map[string]string) error {
	for key, value := range envsToSet {
		if err := os.Setenv(key, value); err != nil {
			return err
		}

	}
	return nil
}

func TestNewEnvironment(t *testing.T) {
	tests := []struct {
		name string
		want *Environment
	}{
		{name: "create env service with default envs", want: &Environment{
			NetworkEnvs:     *NewNetworkEnvs(defaultIp, defaultPort),
			BeamSdkEnvs:     *NewBeamEnvs(pb.Sdk_SDK_JAVA),
			ApplicationEnvs: *NewApplicationEnvs("/app"),
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewEnvironment(*NewNetworkEnvs(defaultIp, defaultPort), *NewBeamEnvs(pb.Sdk_SDK_JAVA), *NewApplicationEnvs("/app")); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewEnvironment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getSdkEnvsFromOsEnvs(t *testing.T) {
	tests := []struct {
		name      string
		want      *BeamEnvs
		envsToSet map[string]string
	}{
		{name: "default sdk envs", want: NewBeamEnvs(defaultSdk)},
		{name: "right sdk key in os envs", want: NewBeamEnvs(pb.Sdk_SDK_JAVA), envsToSet: map[string]string{"BEAM_SDK": "SDK_JAVA"}},
		{name: "wrong sdk key in os envs", want: NewBeamEnvs(defaultSdk), envsToSet: map[string]string{"BEAM_SDK": "SDK_J"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setOsEnvs(tt.envsToSet); err != nil {
				t.Fatalf("couldn't setup os env")
			}
			if got := getSdkEnvsFromOsEnvs(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getSdkEnvsFromOsEnvs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getServerEnvsFromOsEnvs(t *testing.T) {
	tests := []struct {
		name      string
		want      *NetworkEnvs
		envsToSet map[string]string
	}{
		{name: "default values", want: NewNetworkEnvs(defaultIp, defaultPort)},
		{name: "values from os envs", want: NewNetworkEnvs("12.12.12.21", 1234), envsToSet: map[string]string{serverIpKey: "12.12.12.21", serverPortKey: "1234"}},
		{name: "not int port in os env, should be default", want: NewNetworkEnvs("12.12.12.21", defaultPort), envsToSet: map[string]string{serverIpKey: "12.12.12.21", serverPortKey: "1a34"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setOsEnvs(tt.envsToSet); err != nil {
				t.Fatalf("couldn't setup os env")
			}
			if got := getServerEnvsFromOsEnvs(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getServerEnvsFromOsEnvs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getApplicationEnvsFromOsEnvs(t *testing.T) {
	tests := []struct {
		name      string
		want      *ApplicationEnvs
		wantErr   bool
		envsToSet map[string]string
	}{
		{name: "working dir is provided", want: NewApplicationEnvs("/app"), wantErr: false, envsToSet: map[string]string{workingDirKey: "/app"}},
		{name: "working dir isn't provided", want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := setOsEnvs(tt.envsToSet); err != nil {
				t.Fatalf("couldn't setup os env")
			}
			got, err := getApplicationEnvsFromOsEnvs()
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
}
