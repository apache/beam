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
	cloud.google.com/go/bigquery v1.17.0
	cloud.google.com/go/datastore v1.5.0
	cloud.google.com/go/pubsub v1.11.0-beta.schemas
	cloud.google.com/go/storage v1.15.0
	github.com/docker/go-connections v0.4.0
	github.com/go-sql-driver/mysql v1.6.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // TODO(danoliveira): Fully replace this with google.golang.org/protobuf
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/go-cmp v0.5.6
	github.com/google/martian/v3 v3.2.1 // indirect
	github.com/google/uuid v1.3.0
	github.com/kr/text v0.2.0 // indirect
	github.com/lib/pq v1.10.4
	github.com/linkedin/goavro v2.1.0+incompatible
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/nightlyone/lockfile v1.0.0
	github.com/spf13/cobra v1.2.1
	github.com/testcontainers/testcontainers-go v0.12.0
	golang.org/x/net v0.0.0-20211108170745-6635138e15ea
	golang.org/x/oauth2 v0.0.0-20210628180205-a41e5a781914
	golang.org/x/sys v0.0.0-20211109184856-51b60fd695b3
	golang.org/x/text v0.3.6
	google.golang.org/api v0.45.0
	google.golang.org/genproto v0.0.0-20210728212813-7823e685a01f
	google.golang.org/grpc v1.39.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/linkedin/goavro.v1 v1.0.5 // indirect
	gopkg.in/yaml.v2 v2.4.0
)
