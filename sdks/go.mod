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

// This module contains all Go code used for Beam's SDKs. This file is placed
// in this directory in order to cover the go code required for Java and Python
// containers, as well as the entire Go SDK. Placing this file in the repository
// root is not possible because it causes conflicts with a pre-existing vendor
// directory.
module github.com/apache/beam/sdks/v2

go 1.16

require (
	cloud.google.com/go/bigquery v1.28.0
	cloud.google.com/go/compute v1.5.0 // indirect
	cloud.google.com/go/datastore v1.6.0
	cloud.google.com/go/iam v0.2.0 // indirect
	cloud.google.com/go/pubsub v1.18.0
	cloud.google.com/go/storage v1.21.0
	github.com/docker/go-connections v0.4.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang/protobuf v1.5.2 // TODO(danoliveira): Fully replace this with google.golang.org/protobuf
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.7
	github.com/google/uuid v1.3.0
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.10.4
	github.com/linkedin/goavro v2.1.0+incompatible
	github.com/nightlyone/lockfile v1.0.0
	github.com/proullon/ramsql v0.0.0-20211120092837-c8d0a408b939
	github.com/spf13/cobra v1.3.0
	github.com/testcontainers/testcontainers-go v0.12.0
	github.com/xitongsys/parquet-go v1.6.2
	github.com/xitongsys/parquet-go-source v0.0.0-20220315005136-aec0fe3e777c
	golang.org/x/net v0.0.0-20220225172249-27dd8689420f
	golang.org/x/oauth2 v0.0.0-20220223155221-ee480838109b
	golang.org/x/sys v0.0.0-20220227234510-4e6760a101f9
	golang.org/x/text v0.3.7
	google.golang.org/api v0.70.0
	google.golang.org/genproto v0.0.0-20220302033224-9aa15565e42a
	google.golang.org/grpc v1.44.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/linkedin/goavro.v1 v1.0.5 // indirect
	gopkg.in/retry.v1 v1.0.3
	gopkg.in/yaml.v2 v2.4.0
)
