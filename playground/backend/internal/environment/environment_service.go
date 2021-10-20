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
	"errors"
	"google.golang.org/grpc/grpclog"
	"os"
	"strconv"
)

const (
	serverIpKey   = "SERVER_IP"
	serverPortKey = "SERVER_PORT"
	beamSdkKey    = "BEAM_SDK"
	workingDirKey = "APP_WORK_DIR"
	defaultIp     = "localhost"
	defaultPort   = 8080
	defaultSdk    = pb.Sdk_SDK_JAVA
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

//getApplicationEnvsFromOsEnvs lookups in os environment variables and takes value for app working dir. If not exists - return error
func getApplicationEnvsFromOsEnvs() (*ApplicationEnvs, error) {
	if value, present := os.LookupEnv(workingDirKey); present {
		return NewApplicationEnvs(value), nil
	}
	return nil, errors.New("APP_WORK_DIR env should be provided with os.env")
}

// getServerEnvsFromOsEnvs lookups in os environment variables and takes value for ip and port. If not exists - using default
func getServerEnvsFromOsEnvs() *NetworkEnvs {
	ip := defaultIp
	port := defaultPort
	if value, present := os.LookupEnv(serverIpKey); present {
		ip = value
	}

	if value, present := os.LookupEnv(serverPortKey); present {
		if converted, err := strconv.Atoi(value); err == nil {
			port = converted
		} else {
			grpclog.Errorf("couldn't convert provided port. Using default %s\n", defaultPort)
		}
	}

	return NewNetworkEnvs(ip, port)
}

// getServerEnvsFromOsEnvs lookups in os environment variables and takes value for Apache Beam SDK. If not exists - using default
func getSdkEnvsFromOsEnvs() *BeamEnvs {
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
		grpclog.Infof("couldn't get sdk from %s os env, using default: %s", beamSdkKey, defaultSdk)
		sdk = defaultSdk
	}

	return NewBeamEnvs(sdk)
}
