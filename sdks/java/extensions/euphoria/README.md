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


# Euphoria Java 8 DSL

Easy to use Java 8 DSL for the Beam Java SDK. Provides a high-level abstraction of Beam transformations, which is both easy to read and write. Can be used as a complement to existing Beam pipelines (convertible back and forth).

Integration of Euphoria API to Beam is in **progress** ([BEAM-3900](https://issues.apache.org/jira/browse/BEAM-3900)).

## How to build

Euphoria is located in `dsl-euphoria` branch. To build `euphoria` subprojects use command:

```
./gradlew :sdks:java:extensions:euphoria:build
```

## Documentation
Documentation is located at [euphoria.md](../../../../website/src/documentation/sdks/euphoria.md)
