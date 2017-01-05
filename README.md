<!--
  Copyright (C) 2017 Google Inc.

  Licensed under the Apache License, Version 2.0 (the "License"); you may not
  use this file except in compliance with the License. You may obtain a copy of
  the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
  WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
  License for the specific language governing permissions and limitations under
  the License.
-->

# Google Cloud Dataflow SDK for Java

[Google Cloud Dataflow](https://cloud.google.com/dataflow/) provides a simple,
powerful programming model for building both batch and streaming parallel data
processing pipelines.

Dataflow SDK for Java is a distribution of a portion of the
[Apache Beam](https://beam.apache.org) project. This repository hosts the
code to build this distribution and any Dataflow-specific code/modules. The
underlying source code is hosted in the
[Apache Beam repository](https://github.com/apache/beam).

[General usage](https://cloud.google.com/dataflow/getting-started) of Google
Cloud Dataflow does **not** require use of this repository. Instead:

1. depend directly on a specific
[version](https://cloud.google.com/dataflow/release-notes/java) of the SDK in
the [Maven Central Repository](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.google.cloud.dataflow%22)
by adding the following dependency to development
environments like Eclipse or Apache Maven:

        <dependency>
          <groupId>com.google.cloud.dataflow</groupId>
          <artifactId>google-cloud-dataflow-java-sdk-all</artifactId>
          <version>version_number</version>
        </dependency>

1. download the example pipelines from the separate
[DataflowJavaSDK-examples](https://github.com/GoogleCloudPlatform/DataflowJavaSDK-examples)
repository.

<!-- 1. If you are using [Eclipse](https://eclipse.org/) integrated development
environment (IDE), the
[Cloud Dataflow Plugin for Eclipse](https://cloud.google.com/dataflow/getting-started-eclipse)
provides tools to create and execute Dataflow pipelines locally and on the
Dataflow Service. -->

## Status [![Build Status](https://travis-ci.org/GoogleCloudPlatform/DataflowJavaSDK.svg?branch=v2)](https://travis-ci.org/GoogleCloudPlatform/DataflowJavaSDK)

This branch is a work-in-progress for the Dataflow SDK for Java, version 2.0.0.
It is currently supported on the Cloud Dataflow service in Beta.

<!--Both the SDK and the Dataflow Service are generally available, open to all
developers, and considered stable and fully qualified for production use.-->

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

We provide two runners:

  1. The [`DirectRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/DirectPipelineRunner.java)
runs the pipeline on your local machine.
  1. The [`DataflowRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/DataflowPipelineRunner.java)
submits the pipeline to the Dataflow Service, where it runs using managed
resources in the [Google Cloud Platform](https://cloud.google.com) (GCP).

The SDK is built to be extensible and support additional execution environments
beyond local execution and the Google Cloud Dataflow Service. Apache Beam
contains additional SDKs, runners, IO connectors, etc.

## Getting Started

This repository consists of the following parts:

* The [`sdk`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk)
module provides a set of basic Java APIs to program against.
* The [`examples`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples)
module provides a few samples to get started. We recommend starting with the
`WordCount` example.

The following command will build both the `sdk` and `example` modules and
install them in your local Maven repository:

    mvn clean install

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
* [Apache Beam](https://beam.apache.org/)
* [Dataflow Concepts and Programming Model](https://cloud.google.com/dataflow/model/programming-model)
* [Java API Reference](https://cloud.google.com/dataflow/java-sdk/JavaDoc/index)
