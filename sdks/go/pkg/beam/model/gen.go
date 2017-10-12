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

package model

// TODO(herohde) 9/1/2017: for now, install protoc as described on grpc.io before running go generate.

//go:generate protoc -I../../../../../model/job-management/src/main/proto ../../../../../model/job-management/src/main/proto/beam_artifact_api.proto --go_out=jobmanagement_v1,plugins=grpc:jobmanagement_v1
//go:generate protoc -I../../../../../model/fn-execution/src/main/proto ../../../../../model/fn-execution/src/main/proto/beam_provision_api.proto --go_out=fnexecution_v1,plugins=grpc:fnexecution_v1
