---
title: "Portability Framework Roadmap"
aliases:
    - /contribute/portability/
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Portability Framework Roadmap

## Overview

Interoperability between SDKs and runners is a key aspect of Apache
Beam. Previously, the reality was that most runners supported the
Java SDK only, because each SDK-runner combination required non-trivial
work on both sides. Most runners are also currently written in Java,
which makes support of non-Java SDKs far more expensive. The
_portability framework_ rectified this situation and provided
full interoperability across the Beam ecosystem.

The portability framework introduces well-defined, language-neutral
data structures and protocols between the SDK and runner. This interop
layer -- called the _portability API_ -- ensures that SDKs and runners
can work with each other uniformly, reducing the interoperability
burden for both SDKs and runners to a constant effort.  It notably
ensures that _new_ SDKs automatically work with existing runners and
vice versa.  The framework introduces a new runner, the _Universal
Local Runner (ULR)_, as a practical reference implementation that
complements the direct runners. Finally, it enables cross-language
pipelines (sharing I/O or transformations across SDKs) and
user-customized [execution environments](/documentation/runtime/environments/)
("custom containers").

The portability API consists of a set of smaller contracts that
isolate SDKs and runners for job submission, management and
execution. These contracts use protobufs and [gRPC](https://grpc.io) for broad language
support.

 * **Job submission and management**: The _Runner API_ defines a
   language-neutral pipeline representation with transformations
   specifying the execution environment as a docker container
   image. The latter both allows the execution side to set up the
   right environment as well as opens the door for custom containers
   and cross-environment pipelines. The _Job API_ allows pipeline
   execution and configuration to be managed uniformly.

 * **Job execution**: The _SDK harness_ is a SDK-provided
   program responsible for executing user code and is run separately
   from the runner.  The _Fn API_ defines an execution-time binary
   contract between the SDK harness and the runner that describes how
   execution tasks are managed and how data is transferred. In
   addition, the runner needs to handle progress and monitoring in an
   efficient and language-neutral way. SDK harness initialization
   relies on the _Provision_ and _Artifact APIs_ for obtaining staged
   files, pipeline options and environment information. Docker
   provides isolation between the runner and SDK/user environments to
   the benefit of both as defined by the _container contract_. The
   containerization of the SDK gives it (and the user, unless the SDK
   is closed) full control over its own environment without risk of
   dependency conflicts. The runner has significant freedom regarding
   how it manages the SDK harness containers.

The goal is that all (non-direct) runners and SDKs eventually support
the portability API, perhaps exclusively.

If you are interested in digging in to the designs, you can find
them on the [Beam developers' wiki](https://cwiki.apache.org/confluence/display/BEAM/Design+Documents).
Another overview can be found [here](https://docs.google.com/presentation/d/1Yg8Xm4fb-oRjiLQjwLt5153hpwwTLclZrVOKP2hQifo/edit#slide=id.g42e4c9aad6_1_3070).

## Status

All SDKs currently support the portability framework.
There is also a Python Universal Local Runner and shared java runners library.
Performance is good and multi-language pipelines are supported.
Currently, the Flink and Spark runners support portable pipeline execution
(which is used by default for SDKs other than Java),
as does Dataflow when using the [Dataflow Runner v2](https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline#dataflow-runner-v2).
See the
[Portability support table](https://s.apache.org/apache-beam-portability-support-table)
for details.


## Issues

The portability effort touches every component, so the "portability"
label is used to identify all portability-related issues. Pure
design or proto definitions should use the "beam-model" component. A
common pattern for new portability features is that the overall
feature is in "beam-model" with subtasks for each SDK and runner in
their respective components.

**Issues:** [query](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Aportability)

Prerequisites: [Docker](https://docs.docker.com/compose/install/), [Python](https://docs.python-guide.org/starting/install3/linux/), [Java 8](https://openjdk.java.net/install/)

### Running Python wordcount on Flink {#python-on-flink}

The Beam Flink runner can run Python pipelines in batch and streaming modes.
Please see the [Flink Runner page](/documentation/runners/flink/) for more information on
how to run portable pipelines on top of Flink.

### Running Python wordcount on Spark {#python-on-spark}

The Beam Spark runner can run Python pipelines in batch mode.
Please see the [Spark Runner page](/documentation/runners/spark/) for more information on
how to run portable pipelines on top of Spark.

Python streaming mode is not yet supported on Spark.

## SDK Harness Configuration {#sdk-harness-config}

See [here](/documentation/runtime/sdk-harness-config/) for more information on SDK harness deployment options
and [here](https://docs.google.com/presentation/d/1Cso0XP9dmj77OD9Bd53C1M3W1sPJF0ZnA20gzb2BPhE/edit?usp=sharing)
for what goes into writing a portable SDK.

