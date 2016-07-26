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

# Apache Beam (incubating)

[Apache Beam](http://beam.incubator.apache.org) is a unified model for defining both batch and streaming data-parallel processing pipelines, as well as a set of language-specific SDKs for constructing pipelines and Runners for executing them on distributed processing backends like [Apache Spark](http://spark.apache.org/), [Apache Flink](http://flink.apache.org), and [Google Cloud Dataflow](http://cloud.google.com/dataflow).

```
Apache Beam is an effort undergoing incubation at the Apache Software
Foundation (ASF), sponsored by the Apache Incubator PMC.

Incubation is required of all newly accepted projects until a further review
indicates that the infrastructure, communications, and decision making process
have stabilized in a manner consistent with other successful ASF projects.

While incubation status is not necessarily a reflection of the completeness
or stability of the code, it does indicate that the project has yet to be
fully endorsed by the ASF.
```

## Status

_**The Apache Beam project is in the process of bootstrapping. This includes the creation of project resources, the refactoring of the initial code submissions, and the formulation of project documentation, planning, and design documents. Please expect a significant amount of churn and breaking changes in the near future.**_

[![Build Status](https://api.travis-ci.org/apache/incubator-beam.svg?branch=master)](https://travis-ci.org/apache/incubator-beam?branch=master)
[![Build Status](https://builds.apache.org/buildStatus/icon?job=beam_PostCommit_MavenVerify)](https://builds.apache.org/job/beam_PostCommit_MavenVerify/)
[![Coverage Status](https://coveralls.io/repos/github/apache/incubator-beam/badge.svg?branch=master)](https://coveralls.io/github/apache/incubator-beam?branch=master)

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

Currently, this repository contains the Beam Java SDK, which is in the process of evolving from the [Dataflow Java SDK](https://github.com/GoogleCloudPlatform/DataflowJavaSDK). The [Dataflow Python SDK](https://github.com/GoogleCloudPlatform/DataflowPythonSDK) will also become part of Beam in the near future.

Have ideas for new SDKs or DSLs? See the [JIRA](https://issues.apache.org/jira/browse/BEAM/component/12328909/).


### Runners

Beam supports executing programs on multiple distributed processing backends through PipelineRunners. Currently, the following PipelineRunners are available:

- The `DirectRunner` runs the pipeline on your local machine.
- The `DataflowRunner` submits the pipeline to the [Google Cloud Dataflow](http://cloud.google.com/dataflow/).
- The `FlinkRunner` runs the pipeline on an Apache Flink cluster. The code has been donated from [dataArtisans/flink-dataflow](https://github.com/dataArtisans/flink-dataflow) and is now part of Beam.
- The `SparkRunner` runs the pipeline on an Apache Spark cluster. The code has been donated from [cloudera/spark-dataflow](https://github.com/cloudera/spark-dataflow) and is now part of Beam.

Have ideas for new Runners? See the [JIRA](https://issues.apache.org/jira/browse/BEAM/component/12328916/).


## Getting Started

_Coming soon!_

### Flink Runner

See the Flink Runner [README](https://github.com/apache/incubator-beam/tree/master/runners/flink).

### Spark Runner

See the Spark Runner [README](https://github.com/apache/incubator-beam/tree/master/runners/spark).

## Contact Us

To get involved in Apache Beam:

* [Subscribe](mailto:user-subscribe@beam.incubator.apache.org) or [mail](mailto:user@beam.incubator.apache.org) the [user@beam.incubator.apache.org](http://mail-archives.apache.org/mod_mbox/incubator-beam-user/) list.
* [Subscribe](mailto:dev-subscribe@beam.incubator.apache.org) or [mail](mailto:dev@beam.incubator.apache.org) the [dev@beam.incubator.apache.org](http://mail-archives.apache.org/mod_mbox/incubator-beam-dev/) list.
* Report issues on [JIRA](https://issues.apache.org/jira/browse/BEAM).


## More Information

* [Apache Beam](http://beam.incubator.apache.org)
* [Getting Started with Apache Beam](http://beam.incubator.apache.org/getting_started/)
