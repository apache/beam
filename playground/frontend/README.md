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
It provides a portable API layer for building sophisticated data-parallel processing pipelines that may be executed across a diversity of execution engines, or runners.

## Getting Started

Website development requires [Flutter](https://flutter.dev/docs/get-started/install) installed.

Create /lib/generated folder and run the next command to generate grpc files from proto:

`$ protoc playground.proto --dart_out=grpc:lib/generated --proto_path=$(pwd)/../playground/v1`

The following command is used to build and serve the website locally:

`$ flutter run`

Run the following command to generate a release build:

`$flutter build web`

Playground tests may be run using next commands:

`$ flutter pub run build_runner build`

`$ flutter test`

Dart code should follow next [code style](https://dart-lang.github.io/linter/lints/index.html). Code may be analyzed using this command:

`$ flutter analyze`

The full list of command can be found [here](https://flutter.dev/docs/reference/flutter-cli)
