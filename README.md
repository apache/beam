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

[Apache Beam](http://beam.apache.org/) is a unified model for defining both batch and streaming data-parallel processing pipelines, as well as a set of language-specific SDKs for constructing pipelines and Runners for executing them on distributed processing backends, including [Apache Flink](http://flink.apache.org/), [Apache Spark](http://spark.apache.org/), [Google Cloud Dataflow](http://cloud.google.com/dataflow/) and [Hazelcast Jet](https://jet.hazelcast.org/).

## Status

[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.beam/beam-sdks-java-core/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.beam")
[![PyPI version](https://badge.fury.io/py/apache-beam.svg)](https://badge.fury.io/py/apache-beam)
[![Python coverage](https://codecov.io/gh/apache/beam/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/beam)
[![Compat Check PyPI](https://python-compatibility-tools.appspot.com/one_badge_image?package=apache-beam%5Bgcp%5D)](https://python-compatibility-tools.appspot.com/one_badge_target?package=apache-beam%5Bgcp%5D)
[![Compat Check at master](https://python-compatibility-tools.appspot.com/one_badge_image?package=git%2Bgit%3A//github.com/apache/beam.git%23subdirectory%3Dsdks/python)](https://python-compatibility-tools.appspot.com/one_badge_target?package=git%2Bgit%3A//github.com/apache/beam.git%23subdirectory%3Dsdks/python)
[![Build python source distribution and wheels](https://github.com/apache/beam/workflows/Build%20python%20source%20distribution%20and%20wheels/badge.svg?branch=master&event=schedule)](https://github.com/apache/beam/actions?query=workflow%3A%22Build+python+source+distribution+and+wheels%22+branch%3Amaster+event%3Aschedule)
[![Python tests](https://github.com/apache/beam/workflows/Python%20tests/badge.svg?branch=master&event=schedule)](https://github.com/apache/beam/actions?query=workflow%3A%22Python+Tests%22+branch%3Amaster+event%3Aschedule)
[![Java tests](https://github.com/apache/beam/workflows/Java%20Tests/badge.svg?branch=master&event=schedule)](https://github.com/apache/beam/actions?query=workflow%3A%22Java+Tests%22+branch%3Amaster+event%3Aschedule)

### Post-commit tests status (on master branch)

Lang | SDK | Dataflow | Flink | Samza | Spark | Twister2
--- | --- | --- | --- | --- | --- | ---
Go | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Go/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Go/lastCompletedBuild/) | --- | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Go_VR_Flink/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Go_VR_Flink/lastCompletedBuild/) | --- | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Go_VR_Spark/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Go_VR_Spark/lastCompletedBuild/) | ---
Java | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java/lastCompletedBuild/) | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Dataflow/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Dataflow/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Dataflow_Java11/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Dataflow_Java11/lastCompletedBuild/) | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Flink/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Flink/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Flink_Java11/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Flink_Java11/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_PVR_Flink_Batch/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_PVR_Flink_Batch/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_PVR_Flink_Streaming/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_PVR_Flink_Streaming/lastCompletedBuild/) | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Samza/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Samza/lastCompletedBuild/) | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Spark/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Spark/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_PVR_Spark_Batch/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_PVR_Spark_Batch/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_SparkStructuredStreaming/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_SparkStructuredStreaming/lastCompletedBuild/) | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Twister2/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Twister2/lastCompletedBuild/)
Python | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Python2/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Python2/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Python35/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Python35/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Python36/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Python36/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Python37/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Python37/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Python38/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Python38/lastCompletedBuild/) | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Py_VR_Dataflow/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Py_VR_Dataflow/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Py_VR_Dataflow_V2/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Py_VR_Dataflow_V2/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Py_ValCont/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Py_ValCont/lastCompletedBuild/) | [![Build Status](https://ci-beam.apache.org/job/beam_PreCommit_Python2_PVR_Flink_Cron/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PreCommit_Python2_PVR_Flink_Cron/lastCompletedBuild/)<br>[![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Python35_VR_Flink/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Python35_VR_Flink/lastCompletedBuild/) | --- | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_Python_VR_Spark/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_Python_VR_Spark/lastCompletedBuild/) | ---
XLang | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_XVR_Direct/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_XVR_Direct/lastCompletedBuild/) | --- | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_XVR_Flink/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_XVR_Flink/lastCompletedBuild/) | --- | [![Build Status](https://ci-beam.apache.org/job/beam_PostCommit_XVR_Spark/lastCompletedBuild/badge/icon)](https://ci-beam.apache.org/job/beam_PostCommit_XVR_Spark/lastCompletedBuild/) | ---

## Overview

Beam provides a general approach to expressing [embarrassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) data processing pipelines and supports three categories of users, each of which have relatively disparate backgrounds and needs.

1. _End Users_: Writing pipelines with an existing SDK, running it on an existing runner. These users want to focus on writing their application logic and have everything else just work.
2. _SDK Writers_: Developing a Beam SDK targeted at a specific user community (Java, Python, Scala, Go, R, graphical, etc). These users are language geeks and would prefer to be shielded from all the details of various runners and their implementations.
3. _Runner Writers_: Have an execution environment for distributed processing and would like to support programs written against the Beam Model. Would prefer to be shielded from details of multiple SDKs.

### The Beam Model

The model behind Beam evolved from a number of internal Google data processing projects, including [MapReduce](http://research.google.com/archive/mapreduce.html), [FlumeJava](http://research.google.com/pubs/pub35650.html), and [Millwheel](http://research.google.com/pubs/pub41378.html). This model was originally known as the “[Dataflow Model](http://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf)”.

To learn more about the Beam Model (though still under the original name of Dataflow), see the World Beyond Batch: [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) and [Streaming 102](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-102) posts on O’Reilly’s Radar site, and the [VLDB 2015 paper](http://www.vldb.org/pvldb/vol8/p1792-Akidau.pdf).

The key concepts in the Beam programming model are:

* `PCollection`: represents a collection of data, which could be bounded or unbounded in size.
* `PTransform`: represents a computation that transforms input PCollections into output PCollections.
* `Pipeline`: manages a directed acyclic graph of PTransforms and PCollections that is ready for execution.
* `PipelineRunner`: specifies where and how the pipeline should execute.

### SDKs

Beam supports multiple language specific SDKs for writing pipelines against the Beam Model.

Currently, this repository contains SDKs for Java, Python and Go.

Have ideas for new SDKs or DSLs? See the [JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20sdk-ideas).

### Runners

Beam supports executing programs on multiple distributed processing backends through PipelineRunners. Currently, the following PipelineRunners are available:

- The `DirectRunner` runs the pipeline on your local machine.
- The `DataflowRunner` submits the pipeline to the [Google Cloud Dataflow](http://cloud.google.com/dataflow/).
- The `FlinkRunner` runs the pipeline on an Apache Flink cluster. The code has been donated from [dataArtisans/flink-dataflow](https://github.com/dataArtisans/flink-dataflow) and is now part of Beam.
- The `SparkRunner` runs the pipeline on an Apache Spark cluster. The code has been donated from [cloudera/spark-dataflow](https://github.com/cloudera/spark-dataflow) and is now part of Beam.
- The `JetRunner` runs the pipeline on a Hazelcast Jet cluster. The code has been donated from [hazelcast/hazelcast-jet](https://github.com/hazelcast/hazelcast-jet) and is now part of Beam.
- The `Twister2Runner` runs the pipeline on a Twister2 cluster. The code has been donated from [DSC-SPIDAL/twister2](https://github.com/DSC-SPIDAL/twister2) and is now part of Beam.

Have ideas for new Runners? See the [JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-ideas).

## Getting Started

To learn how to write Beam pipelines, read the Quickstart for [[Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), or
[Go](https://beam.apache.org/get-started/quickstart-go)] available on our website.

## Contact Us

To get involved in Apache Beam:

* [Subscribe](mailto:user-subscribe@beam.apache.org) or [mail](mailto:user@beam.apache.org) the [user@beam.apache.org](http://mail-archives.apache.org/mod_mbox/beam-user/) list.
* [Subscribe](mailto:dev-subscribe@beam.apache.org) or [mail](mailto:dev@beam.apache.org) the [dev@beam.apache.org](http://mail-archives.apache.org/mod_mbox/beam-dev/) list.
* [Join ASF Slack](https://s.apache.org/slack-invite) on [#beam channel](https://s.apache.org/beam-slack-channel)
* Report issues on [JIRA](https://issues.apache.org/jira/browse/BEAM).

Instructions for building and testing Beam itself
are in the [contribution guide](https://beam.apache.org/contribute/).

## More Information

* [Apache Beam](https://beam.apache.org)
* [Overview](https://beam.apache.org/use/beam-overview/)
* Quickstart: [Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), [Go](https://beam.apache.org/get-started/quickstart-go)
* [Community metrics](https://s.apache.org/beam-community-metrics)
