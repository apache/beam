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

# Apache Beam

[Apache Beam](http://beam.apache.org/) is a unified model for defining both batch and streaming data-parallel processing pipelines, as well as a set of language-specific SDKs for constructing pipelines and Runners for executing them on distributed processing backends, including [Apache Flink](http://flink.apache.org/), [Apache Spark](http://spark.apache.org/), [Google Cloud Dataflow](http://cloud.google.com/dataflow/), and [Hazelcast Jet](https://jet.hazelcast.org/).

## Status

[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.beam/beam-sdks-java-core/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.beam")
[![PyPI version](https://badge.fury.io/py/apache-beam.svg)](https://badge.fury.io/py/apache-beam)
[![Go version](https://pkg.go.dev/badge/github.com/apache/beam/sdks/v2/go.svg)](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go)
[![Python coverage](https://codecov.io/gh/apache/beam/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/beam)
[![Build python source distribution and wheels](https://github.com/apache/beam/workflows/Build%20python%20source%20distribution%20and%20wheels/badge.svg?branch=master&event=schedule)](https://github.com/apache/beam/actions?query=workflow%3A%22Build+python+source+distribution+and+wheels%22+branch%3Amaster+event%3Aschedule)
[![Python tests](https://github.com/apache/beam/workflows/Python%20tests/badge.svg?branch=master&event=schedule)](https://github.com/apache/beam/actions?query=workflow%3A%22Python+Tests%22+branch%3Amaster+event%3Aschedule)
[![Java tests](https://github.com/apache/beam/workflows/Java%20Tests/badge.svg?branch=master&event=schedule)](https://github.com/apache/beam/actions?query=workflow%3A%22Java+Tests%22+branch%3Amaster+event%3Aschedule)

## Overview

Beam provides a general approach to expressing [embarrassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) data processing pipelines and supports three categories of users, each of which have relatively disparate backgrounds and needs.

1. _End Users_: Writing pipelines with an existing SDK, running it on an existing runner. These users want to focus on writing their application logic and have everything else just work.
2. _SDK Writers_: Developing a Beam SDK targeted at a specific user community (Java, Python, Scala, Go, R, graphical, etc). These users are language geeks and would prefer to be shielded from all the details of various runners and their implementations.
3. _Runner Writers_: Have an execution environment for distributed processing and would like to support programs written against the Beam Model. Would prefer to be shielded from details of multiple SDKs.

### The Beam Model

The model behind Beam evolved from several internal Google data processing projects, including [MapReduce](http://research.google.com/archive/mapreduce.html), [FlumeJava](http://research.google.com/pubs/pub35650.html), and [Millwheel](http://research.google.com/pubs/pub41378.html). This model was originally known as the “[Dataflow Model](http://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)”.

To learn more about the Beam Model (though still under the original name of Dataflow), see the World Beyond Batch: [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) and [Streaming 102](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102) posts on O’Reilly’s Radar site, and the [VLDB 2015 paper](http://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf).

The key concepts in the Beam programming model are:

* `PCollection`: represents a collection of data, which could be bounded or unbounded in size.
* `PTransform`: represents a computation that transforms input PCollections into output PCollections.
* `Pipeline`: manages a directed acyclic graph of PTransforms and PCollections that is ready for execution.
* `PipelineRunner`: specifies where and how the pipeline should execute.

### SDKs

Beam supports multiple language-specific SDKs for writing pipelines against the Beam Model.

Currently, this repository contains SDKs for Java, Python and Go.

Have ideas for new SDKs or DSLs? See the [sdk-ideas label](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Asdk-ideas).

### Runners

Beam supports executing programs on multiple distributed processing backends through PipelineRunners. Currently, the following PipelineRunners are available:

- The `DirectRunner` runs the pipeline on your local machine.
- The `PrismRunner` runs the pipeline on your local machine using Beam Portability.
- The `DataflowRunner` submits the pipeline to the [Google Cloud Dataflow](http://cloud.google.com/dataflow/).
- The `FlinkRunner` runs the pipeline on an Apache Flink cluster. The code has been donated from [dataArtisans/flink-dataflow](https://github.com/dataArtisans/flink-dataflow) and is now part of Beam.
- The `SparkRunner` runs the pipeline on an Apache Spark cluster.
- The `JetRunner` runs the pipeline on a Hazelcast Jet cluster. The code has been donated from [hazelcast/hazelcast-jet](https://github.com/hazelcast/hazelcast-jet) and is now part of Beam.
- The `Twister2Runner` runs the pipeline on a Twister2 cluster. The code has been donated from [DSC-SPIDAL/twister2](https://github.com/DSC-SPIDAL/twister2) and is now part of Beam.

Have ideas for new Runners? See the [runner-ideas label](https://github.com/apache/beam/issues?q=is%3Aopen+is%3Aissue+label%3Arunner-ideas).


Instructions for building and testing Beam itself
are in the [contribution guide](./CONTRIBUTING.md).

## 📚 Learn More

Here are some resources actively maintained by the Beam community to help you get started:
<table>
<thead>
  <tr>
      <th><b>Resource</b></th>
      <th><b>Details</b></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td><a href="https://beam.apache.org" target="_blank" rel="noopener noreferrer">Apache Beam Website</a></td>
    <td>Our website discussing the project, and it's specifics.</td>
  </tr>
  <tr>
    <td><a href="https://beam.apache.org/get-started/quickstart-java" target="_blank" rel="noopener noreferrer">Java Quickstart</a></td>
    <td>A guide to getting started with the Java SDK.</td>
  </tr>
  <tr>
    <td><a href="https://beam.apache.org/get-started/quickstart-py" target="_blank" rel="noopener noreferrer">Python Quickstart</a></td>
    <td>A guide to getting started with the Python SDK.</td>
  </tr>
  <tr>
    <td><a href="https://beam.apache.org/get-started/quickstart-go" target="_blank" rel="noopener noreferrer">Go Quickstart </a></td>
    <td>A guide to getting started with the Go SDK.</td>
  </tr>
  <tr>
    <td><a href="https://tour.beam.apache.org/" target="_blank" rel="noopener noreferrer">Tour of Beam </a></td>
    <td>A comprehensive, interactive learning experience covering Beam concepts in depth.</td>
  </tr>
  <tr>
    <td><a href="https://www.cloudskillsboost.google/course_templates/724" target="_blank" rel="noopener noreferrer">Beam Quest </a></td>
    <td>A certification granted by Google Cloud, certifying proficiency in Beam.</td>
  </tr>
  <tr>
    <td><a href="https://s.apache.org/beam-community-metrics" target="_blank" rel="noopener noreferrer">Community Metrics </a></td>
    <td>Beam's Git Community Metrics.</td>
  </tr>
</tbody>
</table>

## Contact Us

To get involved with Apache Beam:

* [Subscribe to](https://beam.apache.org/community/contact-us/#:~:text=Subscribe%20and%20Unsubscribe) or e-mail the [user@beam.apache.org](http://mail-archives.apache.org/mod_mbox/beam-user/) list.
* [Subscribe to](https://beam.apache.org/community/contact-us/#:~:text=Subscribe%20and%20Unsubscribe) or e-mail the [dev@beam.apache.org](http://mail-archives.apache.org/mod_mbox/beam-dev/) list.
* [Join ASF Slack](https://s.apache.org/slack-invite) on [#beam channel](https://s.apache.org/beam-slack-channel)
* [Report an issue](https://github.com/apache/beam/issues/new/choose).
