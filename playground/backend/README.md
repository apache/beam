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

# Apache Beam Playground

## About

Apache Beam is an open-source, unified model for defining parallel processing pipelines for batch and streaming data.
It provides a portable API layer for building sophisticated data-parallel processing pipelines that may be executed
across a diversity of execution engines, or runners.

Beam Playground helps facilitate trying out and adopting Apache Beam by providing a very quick way for prospective Beam
users to see and run examples of Apache Beam pipelines in a web interface that requires no setup.

## Requirements

- [Go](https://golang.org/dl/) installed (1.16 version).
- [Protocol Buffer Compiler](https://grpc.io/docs/protoc-installation/)

## Getting Started

This section describes what is needed to run the backend application.
- Generating models from proto file
- Go commands to run/test application locally

### Generating models from proto file

Beam Playground uses gRPC for communication between client and server. For using gRPC there is a
`playground/proto/playground.proto` file which contains the definition of all models. To use models in the code they
should be generated firstly using the `playground/proto/playground.proto` file.

Run the following command from `/playground` directory:

```
protoc --go_out=backend/pkg/api --go_opt=paths=source_relative \
--go-grpc_out=backend/pkg/api --go-grpc_opt=paths=source_relative --proto_path=playground/v1 \
playground.proto
```

As a result you will receive 2 files into `backend/pkg/api` folder with models (`playground.pb.go`) and client/server
structures (`playground_grpc.pb.go`).
More information about using gRPC you can find [here](https://grpc.io/docs/languages/).

### Go commands to run/test application locally

The following command is used to build and serve the backend locally:

```
go run ./cmd/server/server.go
```

Run the following command to generate a release build file:

```
go build ./cmd/server/server.go
```

Playground tests may be run using this command:

```
go test ./test/... -v
```

The full list of commands can be found [here](https://pkg.go.dev/cmd/go).

## Running the server app

To run the server using Docker images there are `Docker` files in the `containers` folder for Go and Java languages.
Each of them processes the corresponding SDK, so the backend with Go SDK will work with Go examples/katas/tests only.

One more way to run the server is to run it locally how it is described above.

## Calling the server from another client

To call the server from another client – models and client code should be generated using the
`playground/playground/v1/playground.proto` file. More information about generating models and client's code using `.proto`
files for each language can be found [here](https://grpc.io/docs/languages/).

## Deployment

TBD

## How to Contribute

TBD
