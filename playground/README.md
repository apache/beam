<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Playground

The Beam Playground is a web application to run Beam code snippets in a modern
browser.  This directory holds code to build, test, and deploy the frontend
and backend services.

# Requirements

The following requirements are needed for development, testing, and deploying.

- [go 1.16+](https://golang.org)
- [flutter](https://flutter.dev/)
- Go protobuf dependencies (See [Go gRPC Quickstart](https://grpc.io/docs/languages/go/quickstart/))
- Dart protobuf dependencies (See [Dart gRPC Quickstart](https://grpc.io/docs/languages/dart/))
- [buf](https://docs.buf.build/installation)

# Available Gradle Tasks

## Perform overall pre-commit checks

```
cd beam
./gradlew playgroundPrecommit
```

## To see available gradle tasks for playground:

```
cd beam
./gradlew playground:tasks
```

## Re-generate protobuf

```
cd beam
./gradlew playground:generateProto
```
