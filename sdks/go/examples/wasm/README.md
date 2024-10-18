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
# Overview

wasm is an **_EXPERIMENTAL_** simple example that loads and executes a wasm file function.
greet.wasm, Cargo.toml and greet.rs were copied from the example provided by the wazero library:
https://github.com/tetratelabs/wazero/blob/v1.0.0-pre.3/examples/allocation/rust/greet.go

# Usage

To run this example in various runners, the following assumes:

```
OUTPUT=<path to output i.e. /tmp/example/output>
git clone https://github.com/apache/beam
BEAM_HOME=$(pwd)/beam
cd $BEAM_HOME/sdks
```

## Local Runner execution

To execute this example on the local runner:

```shell
go run ./go/examples/wasm --output=$OUTPUT
```

## Flink Portable Runner

The following describes how to execute this example on the flink portable runner.

### REQUIREMENTS:

- [Docker](https://docker.io); MacOS users may consider alternative: https://github.com/abiosoft/colima
- Google Cloud Storage (https://cloud.google.com/storage) or S3 (https://aws.amazon.com/s3/) bucket;
NOTE this example was only tested on Google Cloud Storage

#### 0. Set OUTPUT to a Cloud storage bucket path

The example below shows Google Cloud Storage.
```
OUTPUT=gs://<my bucket>/greet
```

#### 1. Find the latest flink runner version

```shell
cd $BEAM_HOME
./gradlew :runners:flink:properties --property flink_versions
```

Expected output should include the following, from which you acquire the latest flink runner version.

```shell
'flink_versions: 1.17,1.18,1.19,1.20'
```

#### 2. Set to the latest flink runner version i.e. 1.20

```shell
FLINK_VERSION=1.20
```

#### 3. In a separate terminal, start the flink runner (It should take a few minutes on the first execution)
```shell
cd $BEAM_HOME
./gradlew :runners:flink:$FLINK_VERSION:job-server:runShadow
```

Note the JobService host and port from the output, similar to:

```shell
INFO: JobService started on localhost:8099
```

#### 4. Set the JOB_SERVICE variable from the aforementioned output

```shell
JOB_SERVICE=localhost:8099
```

#### 5. Execute this example using the portable flink runner

```shell
cd $BEAM_HOME/sdks
go run ./go/examples/wasm --runner=universal --endpoint=$JOB_SERVICE --output=$OUTPUT --environment_config=apache/beam_go_sdk:latest
```

## Dataflow Runner

The following describes how to execute this example on Dataflow.

### REQUIREMENTS:

- Google Cloud Storage (https://cloud.google.com/storage) bucket

#### 1. Set OUTPUT to a Cloud storage bucket path

The example below shows Google Cloud Storage.

```shell
OUTPUT=gs://<my bucket>/greet
```

#### 2. Set additional variables

```shell
PROJECT=<project id>
REGION=<region>
STAGING=gs://<my bucket>/staging
NETWORK=<network>
SUBNETWORK=regions/$REGION/subnetworks/<subnetwork>
```

#### 3. Execute this example using Dataflow

```
cd $BEAM_HOME/sdks
go run ./go/examples/wasm --runner=dataflow --output=$OUTPUT --environment_config=apache/beam_go_sdk:latest \
    --project=$PROJECT --region=$REGION --network=$NETWORK --subnetwork=$SUBNETWORK --staging_location=$STAGING
```
