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
To do that, you will need:

* [The protobuf compiler](https://github.com/google/protobuf/releases)
* A proper Go development setup per `BUILD.md` (variables GOPATH and GOBIN set properly)
* `go get -u github.com/golang/protobuf/protoc-gen-go`

> **Note:** Newer releases of the protobuf compiler may be incompatible with the
> protobuf version in Beam. For guaranteed compatibility, use the latest release
> available from the date of the golang/protobuf release used by Beam. (Currently
> v3.5.2)

If all this setup is complete, simply run `go generate` in the current directory
(`pkg/beam/model`).
