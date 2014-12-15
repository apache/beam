# Cloud Dataflow Java SDK (Alpha)

[Google Cloud Dataflow](http://cloud.google.com/dataflow)
provides a simple, powerful programming model for building both batch
and streaming parallel data processing pipelines.

## Overview

The key concepts in this programming model are:

* [PCollection](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/values/PCollection.java):
represents a collection of data, which could be bounded or unbounded in size.
* [PTransform](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/transforms/PTransform.java):
represents a computation that transform input PCollections into output PCollections.
* [Pipeline](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/Pipeline.java):
manages a directed acyclic graph of PTransforms and PCollections, which is ready for excution.
* [PipelineRunner](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/PipelineRunner.java):
specifies where and how the pipeline should execute.

Currently there are three runners:

  1. The [DirectPipelineRunner](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/DirectPipelineRunner.java)
runs the pipeline on your local machine.
  2. The
[DataflowPipelineRunner](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/DataflowPipelineRunner.java)
submits the pipeline to the Dataflow Service**\***, where it runs using managed
resources in the [Google Cloud Platform](http://cloud.google.com).
  3. The
[BlockingDataflowPipelineRunner](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/BlockingDataflowPipelineRunner.java)
submits the pipeline to the Dataflow Service**\*** via the DataflowPipelineRunner and then prints messages
about the job status until execution is complete.

**\***_The Dataflow Service is currently in the Alpha phase of development and access
is limited to whitelisted users._

## Getting Started

## More Information

* [Google Cloud Dataflow](http://cloud.google.com/dataflow)
* [Dataflow Concepts and Programming Model](https://cloud.google.com/dataflow/java-sdk/building-a-pipeline)
* [Javadoc](https://cloud.google.com/dataflow/java-sdk/JavaDoc/index)
