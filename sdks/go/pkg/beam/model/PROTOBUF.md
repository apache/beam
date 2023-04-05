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

# Rebuilding generated protobuf code

If you make changes to .proto files, you will need to rebuild the generated Go
code. You may also need to rebuild the generated code if the required versions
of the [google.golang.org/protobuf](https://github.com/protocolbuffers/protobuf-go)
or [google.golang.org/grpc](https://github.com/grpc/grpc-go) modules defined
in [go.mod](https://github.com/apache/beam/blob/master/sdks/go.mod) change.

First, follow this one-time setup:

1. Download [the protobuf compiler](https://github.com/google/protobuf/releases).
   The simplest approach is to download one of the prebuilt binaries (named
   `protoc`) and extract it somewhere in your machine's `$PATH`.

To generate the code:

1. Navigate to this directory (`pkg/beam/model`).
2. Check [go.mod](https://github.com/apache/beam/blob/master/sdks/go.mod) and
   make note of which versions of [google.golang.org/protobuf](https://github.com/protocolbuffers/protobuf-go)
   and [google.golang.org/grpc](https://github.com/grpc/grpc-go) are required.
3. Verify the versions in ../../../scripts/genproto.sh are correct
4. `go generate`

## Generated Go code fails to build

If the generated .pb.go code contains build errors, it indicates a version
mismatch somewhere between the packages required in go.mod, the installed Go
executables, and the prebuilt `protoc` binary (which should match the
google.golang.org/protobuf version). Following the steps above with matching
version numbers should fix the error.
