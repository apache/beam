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

module beam.apache.org/playground/backend

go 1.16

require (
	cloud.google.com/go/datastore v1.6.0
	cloud.google.com/go/logging v1.4.2
	cloud.google.com/go/storage v1.23.0
	github.com/go-redis/redis/v8 v8.11.4
	github.com/go-redis/redismock/v8 v8.0.6
	github.com/google/uuid v1.3.0
	github.com/improbable-eng/grpc-web v0.14.1
	github.com/rs/cors v1.8.0
	github.com/spf13/viper v1.12.0
	go.uber.org/goleak v1.1.12
	google.golang.org/api v0.85.0
	google.golang.org/grpc v1.47.0
	google.golang.org/protobuf v1.28.0
	gopkg.in/yaml.v3 v3.0.1
)
