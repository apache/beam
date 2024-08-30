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

# Prism - Stand Alone binary

This binary is the Apache Beam Go Prism Runner as a stand alone binary.
See the [Prism README](https://github.com/apache/beam/tree/master/sdks/go/pkg/beam/runners/prism) for
the current state of Prism as a runner.

# Usage

Ensure you have a recent version of Go: https://go.dev/doc/install

Until Beam v2.49.0 is released:

`go install "github.com/apache/beam/sdks/v2/go/cmd/prism@master"`

After that release

`go install "github.com/apache/beam/sdks/v2/go/cmd/prism@latest"`

Then calling `prism` on the command line will start up a JobManagement server on port 8073, and a web UI on 8074. Submit portable Beam jobs to the runner in Loopback mode (`--environment_type=LOOPBACK`) so the runner can provided bundles to the SDK process.

For the Go SDK, when in your pipeline's main binary directory, this can look like:

```
go run *.go --runner=universal --endpoint=localhost:8073 --environment_type=LOOPBACK
```
