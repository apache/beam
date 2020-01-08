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
   The simplest approach is to download one of the prebuilt binaries and extract
   it somewhere in your machine's `$PATH`.
1. A properly installed Go development environment per [the official
   instructions](https://golang.org/doc/install). `$GOPATH` must be set properly.
   If it's not set, follow
   [these instructions](https://github.com/golang/go/wiki/SettingGOPATH).
1. Add `$GOBIN` to your `$PATH`. (Note: If `$GOBIN` is not set, add `$GOPATH/bin`
   instead.)

To generate the code:

1. Navigate to this directory (`pkg/beam/model`).
1. `go get -u github.com/golang/protobuf/protoc-gen-go`
1. `go generate`

## Generated Go code fails to build

Occasionally, after following the steps above and updating the generated .pb.go
files, they may fail to build. This usually indicates a version mismatch in the
[golang/protobuf](https://github.com/golang/protobuf) package. Specifically,
the version of protoc-gen-go in the local Go workspace (used during
`go generate`) differs from the cached version of golang/protobuf used for
building Beam (specified in [gogradle.lock](https://github.com/apache/beam/blob/master/sdks/go/gogradle.lock)).

The preferred way to fix this issue is to update the fixed Beam version of
golang/protobuf to a recent commit. This can be done by manually changing the
commit hash for golang/protobuf in [gogradle.lock](https://github.com/apache/beam/blob/master/sdks/go/gogradle.lock).

If that fails due to dependency issues, an alternate approach is to downgrade
the local version of protoc-gen-go to match the commit in gogradle.lock, with
the following commands.

```bash
# Replace <commit hash> with the commit of golang/protobuf in gogradle.lock.
go get -d -u github.com/golang/protobuf/protoc-gen-go
git -C "$(go env GOPATH)"/src/github.com/golang/protobuf checkout <commit hash>
go install github.com/golang/protobuf/protoc-gen-go
```
> **Note:** This leaves the local repository of protoc-gen-go in a detached
> head state, which may cause problems when updating it in the future. To fix
> this, navigate to the protoc-gen-go directory and run `git checkout master`.
