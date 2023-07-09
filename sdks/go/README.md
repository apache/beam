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

# Go SDK Overview

The Apache Beam Go SDK is the Beam Model implemented in the [Go Programming Language](https://go.dev/).
It is based on the following initial [design](https://s.apache.org/beam-go-sdk-design-rfc).
Below describes requirements, how to run examples, execute tests, and contribute to the Go SDK.

_A note on Beam specific terminology used in this README._

_This README uses minimally necessary Beam related terminology to help you determine requirements, usage and
contribution to the Go SDK. A [definitions](#definitions) section below provides short definitions for you to achieve
these aims._

# Requirements

Aside from the obvious [go](https:/go.dev), you will need to clone the
[Beam Repository](https://github.com/apache/beam) on your local machine.

To keep terminal commands clear in this README, the following is assumed:

```
export BEAM_ROOT=path/to/where/you/clone/beam/repository
```

Only required to run examples, execute tests, and contribute to the Go SDK, [git clone](https://git-scm.com/docs/git-clone)
the Beam repository:

```sh
git clone https://github.com/apache/beam.git $BEAM_ROOT
```

or if you [fork](https://docs.github.com/en/get-started/quickstart/fork-a-repo) the Beam repository into your GitHub
account with `<username>`.

```sh
git clone git@github.com:<username>/beam.git $BEAM_ROOT
```


# Run Examples

## Additional Requirements

In addition to the [common requirements](#requirements) listed above, the following lists anything additional for
running most of the [examples in this repository](examples).

### Google Cloud Setup

Most examples require Google Cloud related resources that serve as data [sources](#source) and [sinks](#sink).
Follow prerequisites listed as setup in https://beam.apache.org/documentation/runners/dataflow for the Java SDK
(It's optional to run the Java example for validation in that referenced documentation but not required).

## Usage

Running examples follows normal Go convention, parameterized by Go flags.

### 1. Navigate to the go.mod directory

Open a terminal and navigate into the go.mod containing directory of the Beam repository. (See above for what
`$BEAM_ROOT` means). Notice that you are entering the `$BEAM_ROOT/sdks` and not `$BEAM_ROOT/sdks/go`.

```sh
cd $BEAM_ROOT/sdks
```

### 2. Execute

Per Go convention, execute an example as you would any `main` executable. Below illustrates how to execute the
[examples/wordcount](examples/wordcount) example on specific [Runners](#runner) and apply to all [examples](examples).

#### Run Example on Local Runner

To execute [examples/wordcount](examples/wordcount) on the Go [local runner](#local-runner):

```sh
cd $BEAM_ROOT/sdks
go run go/examples/wordcount/wordcount.go --output=/tmp/result.txt
```

You should see an output similar to:

```sh
[{6: KV<string,int>/GW/KV<bytes,int[varintz]>}]
[{10: KV<int,string>/GW/KV<int[varintz],bytes>}]
2018/03/21 09:39:03 Pipeline:
2018/03/21 09:39:03 Nodes: {1: []uint8/GW/bytes}
{2: string/GW/bytes}
{3: string/GW/bytes}
{4: string/GW/bytes}
{5: string/GW/bytes}
{6: KV<string,int>/GW/KV<bytes,int[varintz]>}
{7: CoGBK<string,int>/GW/CoGBK<bytes,int[varintz]>}
{8: KV<string,int>/GW/KV<bytes,int[varintz]>}
{9: string/GW/bytes}
{10: KV<int,string>/GW/KV<int[varintz],bytes>}
{11: CoGBK<int,string>/GW/CoGBK<int[varintz],bytes>}
Edges: 1: Impulse [] -> [Out: []uint8 -> {1: []uint8/GW/bytes}]
2: ParDo [In(Main): []uint8 <- {1: []uint8/GW/bytes}] -> [Out: T -> {2: string/GW/bytes}]
3: ParDo [In(Main): string <- {2: string/GW/bytes}] -> [Out: string -> {3: string/GW/bytes}]
4: ParDo [In(Main): string <- {3: string/GW/bytes}] -> [Out: string -> {4: string/GW/bytes}]
5: ParDo [In(Main): string <- {4: string/GW/bytes}] -> [Out: string -> {5: string/GW/bytes}]
6: ParDo [In(Main): T <- {5: string/GW/bytes}] -> [Out: KV<T,int> -> {6: KV<string,int>/GW/KV<bytes,int[varintz]>}]
7: CoGBK [In(Main): KV<string,int> <- {6: KV<string,int>/GW/KV<bytes,int[varintz]>}] -> [Out: CoGBK<string,int> -> {7: CoGBK<string,int>/GW/CoGBK<bytes,int[varintz]>}]
8: Combine [In(Main): int <- {7: CoGBK<string,int>/GW/CoGBK<bytes,int[varintz]>}] -> [Out: KV<string,int> -> {8: KV<string,int>/GW/KV<bytes,int[varintz]>}]
9: ParDo [In(Main): KV<string,int> <- {8: KV<string,int>/GW/KV<bytes,int[varintz]>}] -> [Out: string -> {9: string/GW/bytes}]
10: ParDo [In(Main): T <- {9: string/GW/bytes}] -> [Out: KV<int,T> -> {10: KV<int,string>/GW/KV<int[varintz],bytes>}]
11: CoGBK [In(Main): KV<int,string> <- {10: KV<int,string>/GW/KV<int[varintz],bytes>}] -> [Out: CoGBK<int,string> -> {11: CoGBK<int,string>/GW/CoGBK<int[varintz],bytes>}]
12: ParDo [In(Main): CoGBK<int,string> <- {11: CoGBK<int,string>/GW/CoGBK<int[varintz],bytes>}] -> []
2018/03/21 09:39:03 Reading from gs://apache-beam-samples/shakespeare/kinglear.txt
2018/03/21 09:39:04 Writing to /tmp/result.txt
```

The debugging output is currently quite verbose and likely to change. The output is a local
file in this case:

```
$ head /tmp/result.txt
while: 2
darkling: 1
rail'd: 1
ford: 1
bleed's: 1
hath: 52
Remain: 1
disclaim: 1
sentence: 1
purse: 6
```

#### Run Example on the Dataflow Runner

To run [examples/wordcount](examples/wordcount) on the [Dataflow Runner](#dataflow-runner) run the following. See
[pkg/beam/runners/dataflow/dataflow.go](pkg/beam/runners/dataflow/dataflow.go) and
[examples/wordcount/wordcount.go](examples/wordcount/wordcount.go) for a descriptions of required and optional flags.

1. Set your
[Google Cloud project](https://cloud.google.com/resource-manager/docs/cloud-platform-resource-hierarchy#projects):
    ```
    GCP_PROJECT=$(gcloud config get-value project)
    ```

2. Set your [Google Cloud Compute region](https://cloud.google.com/compute/docs/regions-zones):
    ```
    GCP_REGION=us-central1
    ```

3. Create and set your [Google Cloud Storage Bucket](https://cloud.google.com/storage/docs/buckets):
    (_Note this is WITHOUT the `gs://`_)
    ```
    GCS_BUCKET=<your-google-cloud-storage-bucket-name>
    ```

4. Run the word count example
   (see
   [Google Cloud Documentation](https://cloud.google.com/dataflow/docs/quickstarts/create-pipeline-go#run_the_pipeline_on_the_service)
   for more details):
    ```
    go run go/examples/wordcount/wordcount.go --runner=dataflow \
        --sdk_container_image=apache/beam_go_sdk:latest \
        --project=$GCP_PROJECT \
        --region=$GCP_REGION \
        --staging_location=gs://$GCS_BUCKET/staging \
        --output=gs://$GCS_BUCKET/output
    ```
   
    You should see:
    ```
    2023/07/09 10:57:13 Submitted job: <job-id>
    2023/07/09 10:57:13 Console: https://console.cloud.google.com/dataflow/jobs/us-central1/<job-id>?project=<project>
    2023/07/09 11:02:56 Job state: JOB_STATE_PENDING ...
    2023/07/09 11:03:26 Job still running ...
    ```

5. After seeing `Job <job-id> succeeded!` you can inspect the resulting output.

    Run the following command to inspect the resulting output.

    ```
    gcloud storage cat "gs://$GCS_BUCKET/output*" | head
    ```
   
    You should see something similar to the following:
    ```
    feature: 1
    block: 1
    Cried: 1
    scatter'd: 1
    she: 44
    sudden: 1
    silly: 1
    More: 6
    out: 68
    believe: 3
    ```

#### Troubleshooting tips

If you get the following error:
```
googleapi: Error 400: User project specified in the request is invalid.
```

Try the following:
1. Make sure you followed the [Google Cloud Setup](#google-cloud-setup) above.
2. Configure the [gcloud](https://cloud.google.com/sdk/docs/install-sdk) with your project:
   ```
   gcloud config set project <your-project>
   ```
3. Consider re-running BOTH `gcloud auth login` and `gcloud auth application-default login`
   commands.

## Testing

### Requirements (For runner validations)

Below lists additional requirements to execute tests in this repository on your local machine.

#### 1. Java and Python

You **do not** need to know or care about Java or Python to use the Go SDK for your data processing goals.

Java is **only** required to execute any [gradle](https://gradle.org/) commands configured in the
[Beam](https://github.com/apache/beam) repository. It will be obvious whether you will execute gradle commands in
sections below. You **do not** need to install [gradle](https://gradle.org/) and simply use the enclosed
`$BEAM_ROOT/gradlew` executable available at the root of the [Beam](https://github.com/apache/beam) repository.

Python is **only** required if you need to validate tests against the
[Portable Python Runner](#portable-python-runner). If you are not testing against this
runner, ignore the Python requirement.

#### 2. Docker

[Docker](https://www.docker.com/) is required for certain but not all runners [See definition](#runner).

As of this writing, [colima](https://github.com/abiosoft/colima), a preferred docker alternative for some developers,
did not work.

#### 3. Flock

[sdks/go/run_with_go_version.sh](run_with_go_version.sh) requires the use of
[flock](https://github.com/discoteq/flock).

### Execution

#### 1. Navigate to the go.mod directory

Open a terminal and navigate into the go.mod containing directory of the Beam repository. (See above for what
`$BEAM_ROOT` means). Notice that you are entering the `$BEAM_ROOT/sdks` and not `$BEAM_ROOT/sdks/go`.

```sh
cd $BEAM_ROOT/sdks
```

#### 2. Run Go test

Run go test as you would any Go project.

For unit tests in the exported `pkg/beam` package:
```
go test ./go/pkg/beam...
```

For integration, load, and regression tests:
```
go test ./go/test/...
```

### Runner validations

The following documents various [Runner](#runner) validation tests related to test execution of the Go SDK
**in this repository** (in contrast to your own Go SDK dependent repository and projects).
You'll see some documentation that deviates from Go test execution convention to run gradle commands.

As a Go developer, you might ask yourself, "Why gradle instead of 'go test' 😬?"
Under the hood, various [Runner](#runner) architecture involves resources and non-Go languages that Go SDK runner
validations require. The cost of deviating from the `go test` convention, comes with the benefit of allowing you
as a Beam contributor to focus on the Go SDK and ignore the underlying architecture specifics. The gradle commands
conveniently automate and encapsulate these details.

#### 1. Navigate to the Beam root directory

Open a terminal and navigate into the root directory of the Beam repository. (See [Beam Repository](#beam-repository)
for what `$BEAM_ROOT` means).

```sh
cd $BEAM_ROOT
```

#### Execute tests

To execute tests that validate against the [Dataflow Runner](#dataflow-runner):

```
./gradlew :sdks:go:test:dataflowValidatesRunner
```

To execute tests that validate against the [Flink Runner](#flink-runner):

Note that the following command takes several minutes to run (~15min).

```
./gradlew :sdks:go:test:flinkValidatesRunner
```

To execute tests that validate against the [Portable Python Runner](#portable-python-runner):

```
./gradlew :sdks:go:test:ulrValidatesRunner
```

# Build

See [BUILD.md](./BUILD.md) for how to build Go code in general. See
[container documentation](https://beam.apache.org/documentation/runtime/environments/#building-container-images) for how
to build and push the Go SDK harness container image.

# Issues

Please use the [`sdk-go`](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Asdk-go) component for any
bugs or feature requests.

# Contributing to the Go SDK

See [contribution guide](https://beam.apache.org/contribute/contribution-guide/#code) to create branches, and submit
pull requests as normal.

# Definitions

## Local Runner

A [Runner](#runner) that runs pipeline code on your local machine, in contrast to other runners such as
the [Dataflow Runner](#dataflow-runner)

## Dataflow Runner

A [Runner](#runner) that runs pipeline code on [Dataflow](https://cloud.google.com/dataflow).

## Flink Runner

A [Runner](#runner) that runs pipeline code on the [Flink Runner]()

## Portable Python Runner

A [Runner](#runner) that runs pipeline code using the
[Python Portable Runner](../python/apache_beam/runners/portability/portable_runner.py)

## Runner

A Runner runs your pipeline code. You write your pipeline using the Go programming language and execute on a Runner.
See [examples](#run-examples) for how this looks in practice using the `--runner` flag.
[https://beam.apache.org/documentation/runners/capability-matrix](https://beam.apache.org/documentation/runners/capability-matrix)
provides a list of available Beam Runners.

## Source

A resource from which you read data in a Beam pipeline. Examples include a file directory from which you read
files, a database from which you acquire data via a SQL query, or an API from which you consume response
payloads.

## Sink

A resource to which you write data in a Beam pipeline. Examples include a hard disk directory to which you create and
write file data, a database to which you insert or update records, or an API to which you post remote call payloads.