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

package integration

import (
	"flag"
	"fmt"
)

const (
	GrpcServiceEndpointFlag = "grpc_service_endpoint"
	HttpServiceEndpointFlag = "http_service_endpoint"

	moreInfoUrl = "https://github.com/apache/beam/tree/master/.test-infra/mock-apis#writing-integration-tests"
)

var (
	moreInfo = fmt.Sprintf("See %s for more information on how to get the relevant value for your test.", moreInfoUrl)

	requiredFlags = []string{
		GrpcServiceEndpointFlag,
		HttpServiceEndpointFlag,
	}
)

// The following flags apply to one or more integration tests and used via
// go test ./src/main/go/test/integration/...
var (
	// GRPCServiceEndpoint is the address of the deployed service.
	GRPCServiceEndpoint = flag.String(GrpcServiceEndpointFlag, "",
		"The endpoint to target gRPC calls to a service. "+moreInfo)

	// HTTPServiceEndpoint is the address of the deployed service.
	HTTPServiceEndpoint = flag.String(HttpServiceEndpointFlag, "",
		"The endpoint to target HTTP calls to a service. "+moreInfo)
)
