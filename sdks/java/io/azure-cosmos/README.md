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

# Cosmos DB Core SQL API

Compile all module azure-cosmos

```shell
gradle sdks:java:io:azure-cosmos:build
```

Valite code:
```shell
gradle rat
gradle sdks:java:io:azure-cosmos:spotbugsMain
```

## Test

Run TEST for this module (Cosmos DB Core SQL API):

```shell
gradle sdks:java:io:azure-cosmos:test
```


## Publish in Maven Local

Publish this module

```shell
# apache beam core
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/core/  publishToMavenLocal

# apache beam azure-cosmos
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/io/azure-cosmos/  publishToMavenLocal
```

Publish all modules of apache beam

```shell
gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p sdks/java/  publishToMavenLocal

gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p runners/  publishToMavenLocal

gradle -Ppublishing -PdistMgmtSnapshotsUrl=~/.m2/repository/ -p model/ publishToMavenLocal
```


