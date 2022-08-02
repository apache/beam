---
title:  "Release 0.4.0 adds a runner for Apache Apex"
date:   2017-01-09 10:00:01 -0700
categories:
  - blog
aliases:
  - /blog/2017/01/09/added-apex-runner.html
authors:
  - thw
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

The latest release 0.4.0 of [Apache Beam](/) adds a new runner for [Apache Apex](https://apex.apache.org/). We are excited to reach this initial milestone and are looking forward to continued collaboration between the Beam and Apex communities to advance the runner.

<!--more-->

Beam evolved from the Google Dataflow SDK and as incubator project has quickly adapted the Apache way, grown the community and attracts increasing interest from users that hope to benefit from a conceptual strong unified programming model that is portable between different big data processing frameworks (see [Streaming-101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) and [Streaming-102](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102)). Multiple Apache projects already provide runners for Beam (see [runners and capabilities matrix](/documentation/runners/capability-matrix/)).

Apex is a stream processing framework for low-latency, high-throughput, stateful and reliable processing of complex analytics pipelines on clusters. Apex was developed since 2012 and is used in production by large companies for real-time and batch processing at scale.

The initial revision of the runner was focussed on broad coverage of the Beam model on a functional level. That means, there will be follow up work in several areas to take the runner from functional to scalable and high performance to match the capabilities of Apex and its native API. The runner capabilities matrix shows that the Apex capabilities are well aligned with the Beam model. Specifically, the ability to track computational state in a fault tolerant and efficient manner is needed to broadly support the windowing concepts, including event time based processing.

## Stateful Stream Processor

Apex was built as stateful stream processor from the ground up. Operators checkpoint state in a distributed and asynchronous manner that produces a consistent snapshot for the entire processing graph, which can be used for recovery. Apex also supports such recovery in an incremental, or fine grained, manner. This means only the portion of the DAG that is actually affected by a failure will be recovered while the remaining pipeline continues processing (this can be leveraged to implement use cases with special needs, such as speculative execution to achieve SLA on the processing latency). The state checkpointing along with idempotent processing guarantee is the basis for exactly-once results support in Apex.

## Translation to Apex DAG

A Beam runner needs to implement the translation from the Beam model to the underlying frameworks execution model. In the case of Apex, the runner will translate the pipeline into the native (compositional, low level) DAG API (which is also the base for a number of other API that are available to specify applications that run on Apex). The DAG consists of operators (functional building blocks that are connected with streams. The runner provides the execution layer. In the case of Apex it is distributed stream processing, operators process data event by event. The minimum set of operators covers Beam’s primitive transforms: `ParDo.Bound`,  `ParDo.BoundMulti`, `Read.Unbounded`, `Read.Bounded`, `GroupByKey`, `Flatten.FlattenPCollectionList` etc.

## Execution and Testing

In this release, the Apex runner executes the pipelines in embedded mode, where, similar to the direct runner, everything is executed in a single JVM. See [quickstart](/get-started/quickstart/) on how to run the Beam examples with the Apex runner.

Embedded mode is useful for development and debugging. Apex in production runs distributed on Apache Hadoop YARN clusters. An example how a Beam pipeline can be embedded into an Apex application package to run on YARN can be found [here](https://github.com/tweise/apex-samples/tree/master/beam-apex-wordcount) and support for direct launch in the runner is currently being worked on.

The Beam project has a strong focus on development process and tooling, including testing. For the runners, there is a comprehensive test suite with more than 200 integration tests that are executed against each runner to ensure they don’t break as changes are made. The tests cover the capabilities of the matrix and thus are a measure of completeness and correctness of the runner implementations. The suite was very helpful when developing the Apex runner.

## Outlook

The next step is to take the Apex runner from functional to ready for real applications that run distributed, leveraging the scalability and performance features of Apex, similar to its native API. This includes chaining of ParDos, partitioning, optimizing combine operations etc. To get involved, please see [JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20and%20component%20%3D%20runner-apex%20and%20resolution%20%3D%20unresolved) and join the Beam community.
