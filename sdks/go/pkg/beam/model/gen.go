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

// Package model contains the portable Beam model contracts.
package model

// TODO(herohde) 9/1/2017: for now, install protoc as described on grpc.io before running go generate.
// TODO(lostluck) 2019/05/03: Figure out how to avoid manually keeping these in sync with the
// generator. protoc-gen-go can be declared as a versioned module dependency too.
// TODO(danoliveira) 2021/08/11: GRPC generated files are generated without a license which needs
// to be added in manually. Figure out how to get the licenses applying properly.

// Until file is automatically generated, keep the listed proto files in alphabetical order.

//go:generate ../../../scripts/genproto.sh model
