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

Apache Beam is an open-source, unified model for defining parallel processing pipelines for batch
and streaming data. It provides a portable API layer for building sophisticated data-parallel
processing pipelines that may be executed across a diversity of execution engines, or runners.

## Getting Started

### Run

See [playground/README.md](../README.md) for details on requirements and setup.

The following command is used to build and serve the website locally:

`$ flutter run`

### Build

Run the following command to generate a release build:

`$flutter build web`

### Tests

Playground tests may be run using next commands:

`$ flutter pub run build_runner build`

`$ flutter test`

### Code style

Dart code should follow next [code style](https://dart-lang.github.io/linter/lints/index.html). Code
may be analyzed using this command:

`$ flutter analyze`

Code can be automatically reformatted using:

`$ flutter format ./lib`

### Configuration

The app could be configured using gradle task (e.g. api url)

```
cd beam
./gradlew :playground:frontend:createConfig
```

For more information see See [CONTRIBUTE.md](CONTRIBUTE.md)

### Additional

The full list of command can be found [here](https://flutter.dev/docs/reference/flutter-cli)

## Contribution guide

If you'd like to contribute to the Apache Beam Playground website, read
our [contribution guide](CONTRIBUTE.md) where you can find detailed instructions on how to work with
the website.
