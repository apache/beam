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

If you make changes to .proto files, you will need to rebuild the generated Go code.

First, follow this one-time setup:

1. Download [the protobuf compiler](https://github.com/google/protobuf/releases).
   The simplest approach is to download one of the prebuilt binaries (named
   `protoc`) and extract it somewhere in your machine's `$PATH`.
1. Add `$GOBIN` to your `$PATH`. (Note: If `$GOBIN` is not set, add `$GOPATH/bin`
   instead.)

To generate the code:

1. Navigate to this directory (`pkg/beam/model`).
1. `go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`
1. `go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest`
1. `go generate`

## Generated Go code fails to build

Occasionally, after following the steps above and updating the generated .pb.go
files, they may fail to build. This usually indicates a version mismatch in the
[google.golang.org/protobuf](https://github.com/protocolbuffers/protobuf-go)
package. Specifically, the version of protoc-gen-go installed in your `$GOBIN`
(used during `go generate`) differs from the version of
google.golang.org/protobuf used for building Beam (specified in
[go.mod](https://github.com/apache/beam/blob/master/sdks/go.mod)).

The preferred way to fix this issue is to update the fixed Beam version of
google.golang.org/protobuf to a recent one that works. This can be done by
changing the required version number for google.golang.org/protobuf in
[go.mod](https://github.com/apache/beam/blob/master/sdks/go.mod).

If that fails due to dependency issues, an alternate approach is to downgrade
your installed version of protoc-gen-go to match the version in go.mod, with
the following command.

```bash
# Replace <version> with the version of google.golang.org/protobuf in go.mod.
go install google.golang.org/protobuf/cmd/protoc-gen-go@<version>
```
