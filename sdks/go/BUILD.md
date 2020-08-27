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

# Go build

This document describes the [Go](https://golang.org) code layout and build integration
with Gradle. The setup is non-trivial, because the Go toolchain expects a
certain layout and Gradle support is limited.

Goals:

 1. Go code can be built and tested using Gradle w/o special requirements.
 1. Go tools such as `go build`, `go test` and `go generate` work as usual.
 1. Go code can be pulled with `go get` from `github.com/apache/beam` for users.
 1. Go programs can used in docker container images.

In short, the goals are to make both worlds work well.

### Gradle integration

The Go toolchain expects the package name to match the directory structure,
which in turn must be rooted in `github.com/apache/beam` for `go get` to work.
This directory prefix is beyond the repo itself and we must copy the Go source
code into such a layout to invoke the tool chain. We use a single directory
`sdks/go` for all shared library code and export it as a zip file during the
build process to be used by various tools, such as `sdks/java/container`.
This scheme balances the convenience of combined Go setup with the desire
for a unified layout across languages. Python seems to do the same.

The container build adds a small twist to the build integration, because
container images use linux/amd64 but the development setup might not. We
therefore additionally cross-compile Go binaries for inclusion into container
images where needed, generally placed in `target/linux_amd64`.

### Go development setup

Developers must clone their git repository into:
```
$GOPATH/src/github.com/apache

```
to match the package structure expected by the code imports. Go users can just
`go get` the code directly. For example:
```
go get github.com/apache/beam/sdks/go/...
```
Developers must invoke Go for cross-compilation manually, if desired.

If you make changes to .proto files, you will need to rebuild the generated code.
Consult `pkg/beam/model/PROTOBUF.md`.
