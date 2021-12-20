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

## Getting Started

See [playground/README.md](../README.md) for details on requirements and setup.

This section describes what is needed to run the backend application.
- Go commands to run/test the backend locally
- Set up environment variables to run the backend locally
- Running the backend via Docker

### Go commands to run/test application locally

The following command is used to build and serve the backend locally:

```
$ cd backend
$ go run ./cmd/server/server.go
```

Run the following command to generate a release build file:

```
$ cd backend
$ go build ./cmd/server/server.go
```

Playground tests may be run using this command:

```
$ cd backend
$ go test ... -v
```

The full list of commands can be found [here](https://pkg.go.dev/cmd/go).

### Set up environment variables to run the backend locally

These environment variables should be set to run the backend locally:

- BEAM_SDK - is the SDK which backend could process (`SDK_GO` / `SDK_JAVA` / `SDK_PYTHON`)
- APP_WORK_DIR - is the directory where all folders will be placed to process each code processing request
- PREPARED_MOD_DIR - is the directory where prepared go.mod and go.sum files are placed. It is used only for Go SDK

There are also environment variables which have default value and developer do not need to set them up (optional):

- SERVER_IP - is the IP address of the backend server (default value = `localhost`)
- SERVER_PORT - is the PORT of the backend server (default value = `8080`)
- CACHE_TYPE - is a type of the cache service which is used for the backend server. If it is set as a `remote`, then the
  backend server will use Redis to keep all cache values (default value = `local`)
- CACHE_ADDRESS - is an address of the Redis server. It is used only when `CACHE_TYPE=remote` (default value
  = `localhost:6379`)
- BEAM_PATH - it is the place where all required for the Java SDK libs are placed
  (default value = `/opt/apache/beam/jars/*`)
- KEY_EXPIRATION_TIME - is the expiration time of the keys in the cache (default value = `15 min`)
- PIPELINE_EXPIRATION_TIMEOUT - is the expiration time of the code processing (default value = `15 min`)
- PROTOCOL_TYPE - is the type of the backend server protocol. It could be `TCP` or `HTTP` (default value = `HTTP`)

### Running the server app via Docker

To run the server using Docker images there are `Docker` files in the `containers` folder for Java, Python and Go
languages. Each of them processes the corresponding SDK, so the backend with Go SDK will work with Go
examples/katas/tests only.

One more way to run the server is to run it locally how it is described above.

## Calling the server from another client

To call the server from another client – models and client code should be generated using the
`playground/api/v1/api.proto` file. More information about generating models and client's code using `.proto`
files for each language can be found [here](https://grpc.io/docs/languages/).
