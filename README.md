# Google Cloud Dataflow SDK for Java (Beta)

[Google Cloud Dataflow](https://cloud.google.com/dataflow/) provides a simple,
powerful programming model for building both batch and streaming parallel data
processing pipelines. This repository hosts the open-sourced Cloud Dataflow SDK
for Java, which can be used to run pipelines against the Google Cloud Dataflow
Service.

The contents of this repository are also available as released artifacts in the
[Maven Central Repository](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.google.cloud.dataflow%22).
You can bypass this GitHub repository and depend directly on the released
artifacts from Maven Central by adding the following dependency to development
environments like Eclipse or Apache Maven:

    <dependency>
      <groupId>com.google.cloud.dataflow</groupId>
      <artifactId>google-cloud-dataflow-java-sdk-all</artifactId>
      <version>version_number</version>
    </dependency>

Please replace `version_number` with one of the supported versions from our
[Release Notes](https://cloud.google.com/dataflow/release-notes/java).

## Status [![Build Status](https://travis-ci.org/GoogleCloudPlatform/DataflowJavaSDK.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/DataflowJavaSDK)

The SDK is publicly available as a Beta release, and might be changed in
backward-incompatible ways.

The Google Cloud Dataflow Service is also publicly available in Beta under the
following conditions:

* Your use of Google Cloud Dataflow is governed by the Google Cloud Platform
  Terms of Service. The foregoing   notwithstanding, Google Cloud Dataflow is
  currently in Beta release and might be changed in backward-incompatible ways.
  It is not subject to any SLA or deprecation policy and is not recommended for
  production use.

## Overview

The key concepts in this programming model are:

* [`PCollection`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/values/PCollection.java):
represents a collection of data, which could be bounded or unbounded in size.
* [`PTransform`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/transforms/PTransform.java):
represents a computation that transforms input PCollections into output
PCollections.
* [`Pipeline`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/Pipeline.java):
manages a directed acyclic graph of PTransforms and PCollections that is ready
for execution.
* [`PipelineRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/PipelineRunner.java):
specifies where and how the pipeline should execute.

We provide three PipelineRunners:

  1. The [`DirectPipelineRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/DirectPipelineRunner.java)
runs the pipeline on your local machine.
  2. The [`DataflowPipelineRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/DataflowPipelineRunner.java)
submits the pipeline to the Dataflow Service, where it runs using managed
resources in the [Google Cloud Platform](https://cloud.google.com) (GCP).
  3. The [`BlockingDataflowPipelineRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/BlockingDataflowPipelineRunner.java)
submits the pipeline to the Dataflow Service via the `DataflowPipelineRunner`
and then prints messages about the job status until the execution is complete.

The SDK is built to be extensible and support additional execution environments
beyond local execution and the Google Cloud Dataflow Service. In partnership
with [Cloudera](https://www.cloudera.com/), you can run Dataflow pipelines on
an [Apache Spark](https://spark.apache.org/) backend using the
[`SparkPipelineRunner`](https://github.com/cloudera/spark-dataflow).
Additionally, you can run Dataflow pipelines on an
[Apache Flink](https://flink.apache.org/) backend using the
[`FlinkPipelineRunner`](https://github.com/dataArtisans/flink-dataflow).

## Getting Started

This repository consists of three parts:

* The [`SDK`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk)
module provides a set of basic Java APIs to program against.
* The [`Examples`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples)
module provides a few samples to get started. We recommend starting with the
`WordCount` example.
* The [`Contrib`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/contrib)
directory hosts community-contributed Dataflow modules.

The following command will build both modules and install them in your local
Maven repository:

    mvn clean install

You can speed up the build and install process by using the following options:

  1. To skip execution of the unit tests, run:

        mvn install -DskipTests

  2. While iterating on a specific module, use the following command to compile
  and reinstall it. For example, to reinstall the `examples` module, run:

        mvn install -pl examples

  Be careful, however, as this command will use the most recently installed SDK
  from the local repository (or Maven Central) even if you have changed it
  locally.

If you are using [Eclipse](https://eclipse.org/) integrated development
environment (IDE), please additionally review our
[Eclipse integration instructions](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/eclipse/README.md).

After building and installing, you can execute the `WordCount` and other
example pipelines by following the instructions in this
[README](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples/README.md).

## Contact Us

We welcome all usage-related questions on [Stack Overflow](http://stackoverflow.com/questions/tagged/google-cloud-dataflow)
tagged with `google-cloud-dataflow`.

Please use [issue tracker](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/issues)
on GitHub to report any bugs, comments or questions regarding SDK development.

## More Information

* [Google Cloud Dataflow](https://cloud.google.com/dataflow/)
* [Dataflow Concepts and Programming Model](https://cloud.google.com/dataflow/model/programming-model)
* [Javadoc](https://cloud.google.com/dataflow/java-sdk/JavaDoc/index)
