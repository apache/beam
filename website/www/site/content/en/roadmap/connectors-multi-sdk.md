---
title: "Multi-SDK Connector Efforts"
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

Connector-related efforts that will benefit multiple SDKs.

# Splittable DoFn
Splittable DoFn is the next generation sources framework for Beam that will
replace current frameworks for developing bounded and unbounded sources.
Splittable DoFn is being developed along side current Beam portability
efforts. See [Beam portability framework roadmap](https://beam.apache.org/roadmap/portability/) for more details.

# Cross-language transforms

_Last updated on May 2020._

As an added benefit of Beam portability effort, we are able to utilize Beam transforms across SDKs. This has many benefits.

* Connector sharing across SDKs. For example,
  + Beam pipelines written using Python and Go SDKs will be able to utilize the vast selection of connectors that are currently implemented for Java SDK.
  + Java SDK will be able to utilize connectors for systems that only offer a Python API.
  + Go SDK, will be able to utilize connectors currently available for Java and Python SDKs.
* Ease of developing and maintaining Beam transforms - in general, with cross-language transforms, Beam transform authors will be able to implement new Beam transforms using a
language of choice and utilize these transforms from other languages reducing the maintenance and support overheads.
* [Beam SQL](https://beam.apache.org/documentation/dsls/sql/overview/), that is currently only available to Java SDK, will become available to Python and Go SDKs.
* [Beam TFX transforms](https://www.tensorflow.org/tfx/transform/get_started), that are currently only available to Beam Python SDK pipelines will become available to Java and Go SDKs.

## Completed and Ongoing Efforts

Many efforts related to cross-language transforms are currently in flux. Some of the completed and ongoing efforts are given below.

### Cross-language transforms API and expansion service

Work related to developing/updating the cross-language transforms API for Java/Python/Go SDKs and work related to cross-language transform expansion services.

* Basic API for Java SDK - completed
* Basic API for Python SDK - completed
* Basic API for Go SDK - In progress
* Basic cross-language transform expansion service for Java and Python SDKs - completed
* Artifact staging - mostly completed - [email thread](https://lists.apache.org/thread.html/6fcee7047f53cf1c0636fb65367ef70842016d57effe2e5795c4137d@%3Cdev.beam.apache.org%3E), [doc](https://docs.google.com/document/d/1XaiNekAY2sptuQRIXpjGAyaYdSc-wlJ-VKjl04c8N48/edit#heading=h.900gc947qrw8)

### Support for Flink runner

Work related to making cross-language transforms available for Flink runner.

* Basic support for executing cross-language transforms on portable Flink runner - completed

### Support for Dataflow runner

Work related to making cross-language transforms available for Dataflow runner.

* Basic support for executing cross-language transforms on Dataflow runner
  + This work requires updates to Dataflow service's job submission and job execution logic. This is currently being developed at Google.

### Support for Direct runner

Work related to making cross-language transforms available on Direct runner

* Basic support for executing cross-language transforms on Pyton Direct runner - completed
* Basic support for executing cross-language transforms on Java Direct runner - Not started

### Connector/transform support

Ongoing and planned work related to making existing connectors/transforms available to other SDKs through the cross-language transforms framework.

* Java JdbcIO - completed - [BEAM-10135](https://issues.apache.org/jira/browse/BEAM-10135), [BEAM-10136](https://issues.apache.org/jira/browse/BEAM-10136)
* Java KafkaIO - completed - [BEAM-7029](https://issues.apache.org/jira/browse/BEAM-7029)
* Java KinesisIO - In progress - [BEAM-10137](https://issues.apache.org/jira/browse/BEAM-10137), [BEAM-10138](https://issues.apache.org/jira/browse/BEAM-10138)
* Java PubSubIO - In progress - [BEAM-7738](https://issues.apache.org/jira/browse/BEAM-7738)
* Java SnowflakeIO - In progress - [BEAM-9897](https://issues.apache.org/jira/browse/BEAM-9897), [BEAM-9898](https://issues.apache.org/jira/browse/BEAM-9898)
* Java SpannerIO - In progress - [BEAM-10139](https://issues.apache.org/jira/browse/BEAM-10139), [BEAM-10140](https://issues.apache.org/jira/browse/BEAM-10140)
* Java SQL - completed - [BEAM-8603](https://issues.apache.org/jira/browse/BEAM-8603)

### Portable Beam schema

Portable Beam schema support will provide a generalized mechanism for serializing and transferring data across language boundaries which will be extremely useful for pipelines that employ cross-language transforms.

* Make row coder a standard coder and implement in python - completed - [BEAM-7886](https://issues.apache.org/jira/browse/BEAM-7886)

### Integration/Performance testing

* Add an integration test suite for cross-language transforms on Flink runner - In progress - [BEAM-6683](https://issues.apache.org/jira/browse/BEAM-6683)

### Documentation

Work related to adding documenting on cross-language transforms to Beam Website.

* Document cross-language transforms API for Java/Python - Not started
* Document API for making existing transforms available as cross-language transforms for Java/Python - Not started
