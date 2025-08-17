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
with Gradle.

Goals:

 1. Go code can be built and tested using Gradle w/o special requirements.
 1. Go tools such as `go build`, `go test` and `go generate` work as usual.
 1. Go code can be pulled with `go get` from `github.com/apache/beam` for users.
 1. Go programs can used in docker container images.

In short, the goals are to make both worlds work well.

## Go Modules

Beam publishes a single Go Module for SDK developement and usage, in the `sdks` directory.
This puts all Go code necessary for user pipeline development and for execution
under the same module.
This includes container bootloader code in the Java and Python SDK directories.

Pipeline authors will require a dependency on `github.com/apache/beam/sdks/v2` in their
`go.mod` files to use beam.

### Gradle integration

To integrate with Gradle, we use a gradle plugin called GoGradle.
However, we disable GoGradle vendoring in favour of using Go Modules
for dependency management.
GoGradle handles invoking the go toolchain or gradle and jenkins,
using the same dependencies as SDK contributors and users.
For the rare Go binary, such as the container boot loaders, it should be
possible to build the same binary with both Gradle and the usual Go tool.

The container build adds a small twist to the build integration, because
container images use linux/amd64 but the development setup might not. We
therefore additionally cross-compile Go binaries for inclusion into container
images where needed, generally placed in `target/linux_amd64`.

### Go development setup

To develop the SDK, it should be sufficient to clone the repository, make
changes and execute tests from within the module directory (`<repo>/sdks/...`).

Go users can just `go get` the code directly. For example:

```bash
go get github.com/apache/beam/sdks/v2/go/pkg/beam
```

Developers must invoke Go for cross-compilation manually, if desired.

If you make changes to .proto files, you will need to rebuild the generated code.
Consult `pkg/beam/model/PROTOBUF.md`.

If you make changes to .tmpl files, then add the specialize tool to your path.
You can install specialize using:

```bash
go get github.com/apache/beam/sdks/v2/go/cmd/specialize
```

Add it to your path:

```bash
export PATH=$PATH:$GOROOT/bin:$GOPATH/bin
```
