---
layout: section
title: "Portability Framework"
permalink: /contribute/portability/
section_menu: section-menu/contribute.html
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

# Portability Framework

* TOC
{:toc}

## Overview

Interoperability between SDKs and runners is a key aspect of Apache
Beam. So far, however, the reality is that most runners support the
Java SDK only, because each SDK-runner combination requires non-trivial
work on both sides. All runners are also currently written in Java,
which makes support of non-Java SDKs far more expensive. The
_portability framework_ aims to rectify this situation and provide
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
user-customized execution environments ("custom containers").

The portability API consists of a set of smaller contracts that
isolate SDKs and runners for job submission, management and
execution. These contracts use protobufs and gRPC for broad language
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

## Design

The [model protos](https://github.com/apache/beam/tree/master/model)
contain all aspects of the portability API and is the truth on the
ground. The proto definitions supercede any design documents. The main
design documents are the following:

 * [Runner API](https://s.apache.org/beam-runner-api). Pipeline
   representation and discussion on primitive/composite transforms and
   optimizations.

 * [Job API](https://s.apache.org/beam-job-api). Job submission and
   management protocol.

 * [Fn API](https://s.apache.org/beam-fn-api). Execution-side control
   and data protocols and overview.

 * [Container
   contract](https://s.apache.org/beam-fn-api-container-contract).
   Execution-side docker container invocation and provisioning
   protocols. See
   [CONTAINERS.md](https://github.com/apache/beam/blob/master/sdks/CONTAINERS.md)
   for how to build container images.

In discussion:

 * [Cross
   language](https://s.apache.org/beam-mixed-language-pipelines). Options
   and tradeoffs for how to handle various kinds of
   multi-language/multi-SDK pipelines.

## Development

The portability framework is a substantial effort that touches every
Beam component. In addition to the sheer magnitude, a major challenge
is engineering an interop layer that does not significantly compromise
performance due to the additional serialization overhead of a
language-neutral protocol.

### Roadmap

The proposed project phases are roughly as follows and are not
strictly sequential, as various components will likely move at
different speeds. Additionally, there have been (and continues to be)
supporting refactorings that are not always tracked as part of the
portability effort. Work already done is not tracked here either.

 * **P1 [MVP]**: Implement the fundamental plumbing for portable SDKs
   and runners for batch and streaming, including containers and the
   ULR
   [[BEAM-2899](https://issues.apache.org/jira/browse/BEAM-2899)]. Each
   SDK and runner should use the portability framework at least to the
   extent that wordcount
   [[BEAM-2896](https://issues.apache.org/jira/browse/BEAM-2896)] and
   windowed wordcount
   [[BEAM-2941](https://issues.apache.org/jira/browse/BEAM-2941)] run
   portably.

 * **P2 [Feature complete]**: Design and implement portability support
   for remaining execution-side features, so that any pipeline from
   any SDK can run portably on any runner. These features include side
   inputs
   [[BEAM-2863](https://issues.apache.org/jira/browse/BEAM-2863)], User state [[BEAM-2862](https://issues.apache.org/jira/browse/BEAM-2862)], User
   timers
   [[BEAM-2925](https://issues.apache.org/jira/browse/BEAM-2925)],
   Splittable DoFn
   [[BEAM-2896](https://issues.apache.org/jira/browse/BEAM-2896)] and
   more.  Each SDK and runner should use the portability framework at
   least to the extent that the mobile gaming examples
   [[BEAM-2940](https://issues.apache.org/jira/browse/BEAM-2940)] run
   portably.

 * **P3 [Performance]**: Measure and tune performance of portable
   pipelines using benchmarks such as Nexmark. Features such as
   progress reporting
   [[BEAM-2940](https://issues.apache.org/jira/browse/BEAM-2940)],
   combiner lifting
   [[BEAM-2937](https://issues.apache.org/jira/browse/BEAM-2937)] and
   fusion are expected to be needed.

 * **P4 [Cross language]**: Design and implement cross-language
   pipeline support, including how the ecosystem of shared transforms
   should work.

### Issues

The portability effort touches every component, so the "portability"
label is used to identify all portability-related issues. Pure
design or proto definitions should use the "beam-model" component. A
common pattern for new portability features is that the overall
feature is in "beam-model" with subtasks for each SDK and runner in
their respective components.

**JIRA:** [query](https://issues.apache.org/jira/issues/?filter=12341256)

### Status

MVP in progress (near completion for Flink runner). See the
[Portability support table](https://s.apache.org/apache-beam-portability-support-table)
for details.

#### Running Python wordcount on Flink

The Flink runner is currently the only runner to support portable pipeline execution.
To run a basic Python wordcount (in batch mode) with embedded Flink:

1. Run once to build the SDK harness container: `./gradlew :beam-sdks-python-container:docker`
2. Start the Flink portable JobService endpoint: `./gradlew :beam-runners-flink_2.11-job-server:runShadow`
3. Submit the wordcount pipeline to above endpoint: `./gradlew :beam-sdks-python:portableWordCount -PjobEndpoint=localhost:8099`

To run the pipeline in streaming mode: `./gradlew :beam-sdks-python:portableWordCount -PjobEndpoint=localhost:8099 -Pstreaming`

To run on a separate [Flink cluster](https://ci.apache.org/projects/flink/flink-docs-release-1.5/quickstart/setup_quickstart.html):

1. Start local Flink cluster
2. Create shaded JobService jar: `./gradlew :beam-runners-flink_2.11-job-server:installShadowDist`
3. Optional optimization step: Place the generated JobServer Jar `beam/runners/flink/job-server/build/libs/beam-runners-flink_2.11-job-server-2.7.0-SNAPSHOT.jar` in `flink/lib` and change class loading order for Flink by adding `classloader.resolve-order: parent-first` to `conf/flink-conf.yaml`.
4. Start JobService with Flink web service endpoint: `./gradlew :beam-runners-flink_2.11-job-server:runShadow -PflinkMasterUrl=localhost:8081`
5. Submit the pipeline.

