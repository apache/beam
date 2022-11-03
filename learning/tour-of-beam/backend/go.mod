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

module beam.apache.org/learning/tour-of-beam/backend

go 1.16

require (
	github.com/GoogleCloudPlatform/functions-framework-go v1.5.3
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go/datastore v1.8.0
	cloud.google.com/go/firestore v1.7.0 // indirect
	firebase.google.com/go/v4 v4.9.0
	github.com/stretchr/testify v1.8.0
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1
)
