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

# Go SDK

The Apache Beam Go SDK is the Beam Model implemented in the [Go Programming Language](https://go.dev/).
It is based on the following initial [design](https://s.apache.org/beam-go-sdk-design-rfc).

## How to run the examples

**Prerequisites**: to use Google Cloud sources and sinks (default for
most examples), follow the setup
[here](https://beam.apache.org/documentation/runners/dataflow/). You can
verify that it works by running the corresponding Java example.

The examples are normal Go programs and are most easily run directly.
They are parameterized by Go flags.
For example, to run wordcount on the Go direct runner do:

```
$ pwd
[...]/sdks/go
$ go run examples/wordcount/wordcount.go --output=/tmp/result.txt
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

To run wordcount on dataflow runner do:

```
$  go run wordcount.go --runner=dataflow --project=<YOUR_GCP_PROJECT> --region=<YOUR_GCP_REGION> --staging_location=<YOUR_GCS_LOCATION>/staging --worker_harness_container_image=<YOUR_SDK_HARNESS_IMAGE_LOCATION> --output=<YOUR_GCS_LOCATION>/output
```

The output is a GCS file in this case:

```
$ gsutil cat <YOUR_GCS_LOCATION>/output* | head
Blanket: 1
blot: 1
Kneeling: 3
cautions: 1
appears: 4
Deserved: 1
nettles: 1
OSWALD: 53
sport: 3
Crown'd: 1
```


See [BUILD.md](./BUILD.md) for how to build Go code in general. See
[container documentation](https://beam.apache.org/documentation/runtime/environments/#building-container-images) for how to build and push the Go SDK harness container image.

## Issues

Please use the [`sdk-go`](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Asdk-go) component for any bugs or feature requests.

## Contributing to the Go SDK

### New to developing Go?
https://tour.golang.org : The Go Tour gives you the basics of the language, interactively no installation required.

https://github.com/campoy/go-tooling-workshop is a great start on learning good (optional) development tools for Go.

### Developing Go Beam SDK on Github

The Go SDK uses Go Modules for dependency management so it's as simple as cloning
the repo, making necessary changes and running tests.

Executing all unit tests for the SDK is possible from the `<beam root>\sdks\go` directory and running `go test ./...`.

To test your change as Jenkins would execute it from a PR, from the
beam root directory, run:
 * `./gradlew :sdks:go:goTest` executes the unit tests.
 * `./gradlew :sdks:go:test:prismValidatesRunner` validates the SDK against the Go Prism runner as a stand alone binary, with containers.
 * `./gradlew :sdks:go:test:ulrValidatesRunner` validates the SDK against the Portable Python runner.
 * `./gradlew :sdks:go:test:flinkValidatesRunner` validates the SDK against the Flink runner.

Follow the [contribution guide](https://beam.apache.org/contribute/contribution-guide/#code) to create branches, and submit pull requests as normal.


