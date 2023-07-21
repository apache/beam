---
title: "Beam Overview"
aliases:
  - /use/beam-overview/
  - /docs/use/beam-overview/
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

# Apache Beam Overview

Apache Beam is an open source, unified model for defining both batch and streaming data-parallel processing pipelines. Using one of the open source Beam SDKs, you build a program that defines the pipeline. The pipeline is then executed by one of Beam's supported **distributed processing back-ends**, which include [Apache Flink](https://flink.apache.org), [Apache Spark](https://spark.apache.org), and [Google Cloud Dataflow](https://cloud.google.com/dataflow).

Beam is particularly useful for [embarrassingly parallel](https://en.wikipedia.org/wiki/Embarassingly_parallel) data processing tasks, in which the problem can be decomposed into many smaller bundles of data that can be processed independently and in parallel. You can also use Beam for Extract, Transform, and Load (ETL) tasks and pure data integration. These tasks are useful for moving data between different storage media and data sources, transforming data into a more desirable format, or loading data onto a new system.

<div style="display: flex; justify-content: center">
  <img src="/images/learner_graph.png" width="800px" alt="Learner Graph">
</div>

## Apache Beam SDKs

The Beam SDKs provide a unified programming model that can represent and transform data sets of any size, whether the input is a finite data set from a batch data source, or an infinite data set from a streaming data source. The Beam SDKs use the same classes to represent both bounded and unbounded data, and the same transforms to operate on that data. You use the Beam SDK of your choice to build a program that defines your data processing pipeline.

Beam currently supports the following language-specific SDKs:

- [Apache Beam Java SDK](/documentation/sdks/java) ![Java logo](/images/logos/sdks/java.png)
- [Apache Beam Python SDK](/documentation/sdks/python) ![Python logo](/images/logos/sdks/python.png)
- [Apache Beam Go SDK](/documentation/sdks/go) <img src="/images/logos/sdks/go.png" height="45px" alt="Go logo">

A Scala <img src="/images/logos/sdks/scala.png" height="45px" alt="Scala logo"> interface is also available as [Scio](https://github.com/spotify/scio).

## Apache Beam Pipeline Runners

The Beam Pipeline Runners translate the data processing pipeline you define with your Beam program into the API compatible with the distributed processing back-end of your choice. When you run your Beam program, you'll need to specify an [appropriate runner](/documentation/runners/capability-matrix) for the back-end where you want to execute your pipeline.

Beam currently supports the following runners:

- [Direct Runner](/documentation/runners/direct)
- [Apache Flink Runner](/documentation/runners/flink) <img src="/images/logos/runners/flink.png" height="50px" alt="Apache Flink logo">
- [Apache Nemo Runner](/documentation/runners/nemo)
- [Apache Samza Runner](/documentation/runners/samza) <img src="/images/logos/runners/samza.png" height="40px" alt="Apache Samza logo">
- [Apache Spark Runner](/documentation/runners/spark) <img src="/images/logos/runners/spark.png" height="50px" alt="Apache Spark logo">
- [Google Cloud Dataflow Runner](/documentation/runners/dataflow) <img src="/images/logos/runners/dataflow.png" height="50px" alt="Google Cloud Dataflow logo">
- [Hazelcast Jet Runner](/documentation/runners/jet) <img src="/images/logos/runners/jet.png" height="40px" alt="Hazelcast Jet logo">
- [Twister2 Runner](/documentation/runners/twister2) <img src="/images/logos/runners/twister2.png" height="50px" alt="Twister2 logo">

**Note:** You can always execute your pipeline locally for testing and debugging purposes.

## Get Started

Get started using Beam for your data processing tasks.

> If you already know [Apache Spark](https://spark.apache.org/),
> check our [Getting started from Apache Spark](/get-started/from-spark) page.

1. Take the [Tour of Beam](https://tour.beam.apache.org/) as an online interactive learning experience.

1. Follow the Quickstart for the [Java SDK](/get-started/quickstart-java), the [Python SDK](/get-started/quickstart-py), or the [Go SDK](/get-started/quickstart-go).

1. See the [WordCount Examples Walkthrough](/get-started/wordcount-example) for examples that introduce various features of the SDKs.

1. Take a self-paced tour through our [Learning Resources](/documentation/resources/learning-resources).

1. Dive into the [Documentation](/documentation/) section for in-depth concepts and reference materials for the Beam model, SDKs, and runners.

1. Dive into the [cookbook examples](https://github.com/GoogleCloudPlatform/dataflow-cookbook) for learning how to run Beam on Dataflow.

## Contribute

Beam is an <a href="https://www.apache.org" target="_blank">Apache Software Foundation</a> project, available under the Apache v2 license. Beam is an open source community and contributions are greatly appreciated! If you'd like to contribute, please see the [Contribute](/contribute/) section.
