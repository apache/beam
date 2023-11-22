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

// Package v1 contains the proto generated code.
// See https://github.com/apache/beam/tree/master/.test-infra/mock-apis.
package v1

// Copy proto generated files from .test-infra/mock-apis.
// Go generate does not accept wildcard paths and each file needed to be listed
// individually. Run as: go generate ./go/test/integration/io/rrio/echo/v1
//go:generate cp ../../../../../../../../.test-infra/mock-apis/src/main/go/internal/proto/echo/v1/echo.pb.go .
//go:generate cp ../../../../../../../../.test-infra/mock-apis/src/main/go/internal/proto/echo/v1/echo_grpc.pb.go .
