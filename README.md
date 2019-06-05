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

[Apache Beam](http://beam.apache.org/) is a unified model for defining both batch and streaming data-parallel processing pipelines, as well as a set of language-specific SDKs for constructing pipelines and Runners for executing them on distributed processing backends, including [Apache Apex](http://apex.apache.org/), [Apache Flink](http://flink.apache.org/), [Apache Spark](http://spark.apache.org/), and [Google Cloud Dataflow](http://cloud.google.com/dataflow/).

## Status

[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.beam/beam-sdks-java-core/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.beam")
[![PyPI version](https://badge.fury.io/py/apache-beam.svg)](https://badge.fury.io/py/apache-beam)
[![Build Status](https://builds.apache.org/buildStatus/icon?job=beam_PostCommit_Java)](https://builds.apache.org/job/beam_PostCommit_Java)
[![Coverage Status](https://coveralls.io/repos/github/apache/beam/badge.svg?branch=master)](https://coveralls.io/github/apache/beam?branch=master)

### Post-commit tests status (on master branch)

Lang | SDK | Apex | Dataflow | Flink | Gearpump | Samza | Spark
--- | --- | --- | --- | --- | --- | --- | ---
Go | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Go/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Go/lastCompletedBuild/) | --- | --- | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Go_VR_Flink/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Go_VR_Flink/lastCompletedBuild/) | --- | --- | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Go_VR_Spark/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Go_VR_Spark/lastCompletedBuild/)
Java | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Apex/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Apex/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Dataflow/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Dataflow/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Flink/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Flink/lastCompletedBuild/)<br>[![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Flink_Batch/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Flink_Batch/lastCompletedBuild/)<br>[![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Flink_Streaming/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Flink_Streaming/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Gearpump/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Gearpump/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Samza/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Samza/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Spark/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_ValidatesRunner_Spark/lastCompletedBuild/)<br>[![Build Status](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Spark_Batch/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Java_PVR_Spark_Batch/lastCompletedBuild/)
Python | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Python_Verify/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Python_Verify/lastCompletedBuild/)<br>[![Build Status](https://builds.apache.org/job/beam_PostCommit_Python3_Verify/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Python3_Verify/lastCompletedBuild/) | --- | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Py_VR_Dataflow/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Py_VR_Dataflow/lastCompletedBuild/) <br> [![Build Status](https://builds.apache.org/job/beam_PostCommit_Py_ValCont/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Py_ValCont/lastCompletedBuild/) | [![Build Status](https://builds.apache.org/job/beam_PreCommit_Python_PVR_Flink_Cron/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PreCommit_Python_PVR_Flink_Cron/lastCompletedBuild/) | --- | --- | [![Build Status](https://builds.apache.org/job/beam_PostCommit_Python_VR_Spark/lastCompletedBuild/badge/icon)](https://builds.apache.org/job/beam_PostCommit_Python_VR_Spark/lastCompletedBuild/)

## Overview

Beam provides a general approach to expressing [embarrassingly parallel](https://en.wikipedia.org/wiki/Embarrassingly_parallel) data processing pipelines and supports three categories of users, each of which have relatively disparate backgrounds and needs.

1. _End Users_: Writing pipelines with an existing SDK, running it on an existing runner. These users want to focus on writing their application logic and have everything else just work.
2. _SDK Writers_: Developing a Beam SDK targeted at a specific user community (Java, Python, Scala, Go, R, graphical, etc). These users are language geeks, and  would prefer to be shielded from all the details of various runners and their implementations.
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
- The `ApexRunner` runs the pipeline on an Apache Hadoop YARN cluster (or in embedded mode).
- The `DataflowRunner` submits the pipeline to the [Google Cloud Dataflow](http://cloud.google.com/dataflow/).
- The `FlinkRunner` runs the pipeline on an Apache Flink cluster. The code has been donated from [dataArtisans/flink-dataflow](https://github.com/dataArtisans/flink-dataflow) and is now part of Beam.
- The `SparkRunner` runs the pipeline on an Apache Spark cluster. The code has been donated from [cloudera/spark-dataflow](https://github.com/cloudera/spark-dataflow) and is now part of Beam.

Have ideas for new Runners? See the [JIRA](https://issues.apache.org/jira/issues/?jql=project%20%3D%20BEAM%20AND%20component%20%3D%20runner-ideas).

## Getting Started

Please refer to the Quickstart[[Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), [Go](https://beam.apache.org/get-started/quickstart-go)] available on our website.

If you'd like to build and install the whole project from the source distribution, you may need some additional tools installed
in your system. In a Debian-based distribution:

```
sudo apt-get install \
    openjdk-8-jdk \
    python-setuptools \
    python-pip \
    virtualenv
```

Then please use the standard `./gradlew build` command.

## Contact Us

To get involved in Apache Beam:

* [Subscribe](mailto:user-subscribe@beam.apache.org) or [mail](mailto:user@beam.apache.org) the [user@beam.apache.org](http://mail-archives.apache.org/mod_mbox/beam-user/) list.
* [Subscribe](mailto:dev-subscribe@beam.apache.org) or [mail](mailto:dev@beam.apache.org) the [dev@beam.apache.org](http://mail-archives.apache.org/mod_mbox/beam-dev/) list.
* Report issues on [JIRA](https://issues.apache.org/jira/browse/BEAM).

We also have a [contributor's guide](https://beam.apache.org/contribute/contribution-guide/).

## More Information

* [Apache Beam](http://beam.apache.org)
* [Overview](http://beam.apache.org/use/beam-overview/)
* Quickstart: [Java](https://beam.apache.org/get-started/quickstart-java), [Python](https://beam.apache.org/get-started/quickstart-py), [Go](https://beam.apache.org/get-started/quickstart-go)
* [Community metrics](https://s.apache.org/beam-community-metrics)
