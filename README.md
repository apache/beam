# Cloud Dataflow Java SDK (Alpha)

[Google Cloud Dataflow](https://cloud.google.com/dataflow/)
provides a simple, powerful programming model for building both batch
and streaming parallel data processing pipelines.

## Status [![Build Status](https://travis-ci.org/GoogleCloudPlatform/DataflowJavaSDK.svg?branch=master)](https://travis-ci.org/GoogleCloudPlatform/DataflowJavaSDK)

The Cloud Dataflow SDK is used to access the Google Cloud Dataflow service,
which is currently in Alpha and restricted to whitelisted users.

The SDK is publicly available and can be used for local execution by anyone.
Note, however, that the SDK is also an Alpha release and may change
significantly over time. The SDK is built to be extensible and support
additional execution environments ("runners") beyond local execution and the
Google Cloud Dataflow service. As the product matures, we look forward to
working with you to improve Cloud Dataflow.

## Overview

The key concepts in this programming model are:

* [`PCollection`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/values/PCollection.java):
represents a collection of data, which could be bounded or unbounded in size.
* [`PTransform`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/transforms/PTransform.java):
represents a computation that transforms input PCollections into output
PCollections.
* [`Pipeline`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/Pipeline.java):
manages a directed acyclic graph of PTransforms and PCollections, which is ready
for execution.
* [`PipelineRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/PipelineRunner.java):
specifies where and how the pipeline should execute.

We provide three PipelineRunners:

  1. The [`DirectPipelineRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/DirectPipelineRunner.java)
runs the pipeline on your local machine.
  2. The [`DataflowPipelineRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/DataflowPipelineRunner.java)
submits the pipeline to the Dataflow Service, where it runs using managed
resources in the [Google Cloud Platform](https://cloud.google.com) (GCP).
  3. The
[`BlockingDataflowPipelineRunner`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk/src/main/java/com/google/cloud/dataflow/sdk/runners/BlockingDataflowPipelineRunner.java)
submits the pipeline to the Dataflow Service via the `DataflowPipelineRunner`
and then prints messages about the job status until the execution is complete.

_The Dataflow Service is currently in the Alpha phase of development and
access is limited to whitelisted users._

Additionally, in partnership with [Cloudera](https://www.cloudera.com/), you can
run Dataflow pipelines on an [Apache Spark](https://spark.apache.org/) backend.
The relevant runner code is hosted in
[this](https://github.com/cloudera/spark-dataflow) repository.

## Getting Started

This repository consists of two modules:

* [`SDK`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/sdk)
module provides a set of basic Java APIs to program against.
* [`Examples`](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/blob/master/examples)
module provides a few samples to get started. We recommend starting with the
WordCount example.

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

  3. To run Maven using multiple threads, run:

        mvn -T 4 install

After building and installing, you can execute the `WordCount` and other example
pipelines using the `DirectPipelineRunner` on your local machine:

    mvn exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.WordCount \
    -Dexec.args="--input=<INPUT FILE PATTERN> --output=<OUTPUT FILE>"

If you have been whitelisted for Alpha access to the Dataflow Service and
followed the [developer setup](https://cloud.google.com/dataflow/java-sdk/getting-started#DeveloperSetup)
steps, you can use the `BlockingDataflowPipelineRunner` to execute the
`WordCount` example in the GCP. In this case, you specify your project name,
pipeline runner, and the staging location in
[Google Cloud Storage](https://cloud.google.com/storage/) (GCS), as follows:

    mvn exec:java -pl examples \
    -Dexec.mainClass=com.google.cloud.dataflow.examples.WordCount \
    -Dexec.args="--project=<YOUR GCP PROJECT NAME> --stagingLocation=<YOUR GCS LOCATION> --runner=BlockingDataflowPipelineRunner"

GCS location should be entered in the form of
`gs://bucket/path/to/staging/directory`. GCP project refers to its name (not
number), which has been whitelisted for Cloud Dataflow. Refer to
[Google Cloud Platform](https://cloud.google.com/) for general instructions on
getting started with GCP.

Alternatively, you may choose to bundle all dependencies into a single JAR and
execute it outside of the Maven environment. For example, after building and
installing as usual, you can execute the following commands to create the
bundled JAR of the `Examples` module and execute it both locally and in GCP:

    mvn bundle:bundle -pl examples

    java -cp examples/target/google-cloud-dataflow-java-examples-all-bundled-manual_build.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --input=<INPUT FILE PATTERN> --output=<OUTPUT FILE>

    java -cp examples/target/google-cloud-dataflow-java-examples-all-bundled-manual_build.jar \
    com.google.cloud.dataflow.examples.WordCount \
    --project=<YOUR GCP PROJECT NAME> --stagingLocation=<YOUR GCS LOCATION> --runner=BlockingDataflowPipelineRunner

Other examples can be run similarly by replacing the `WordCount` class name with
`BigQueryTornadoes`, `DatastoreWordCount`, `TfIdf`, `TopWikipediaSessions`, etc.
and adjusting runtime options under the `Dexec.args` parameter, as specified in
the example itself.

Note that when running Maven on Microsoft Windows platform, backslashes (`\`)
under the `Dexec.args` parameter should be escaped with another backslash. For
example, input file pattern of `c:\*.txt` should be entered as `c:\\*.txt`.

## Contact Us

We welcome all usage-related questions on [Stack Overflow](http://stackoverflow.com/questions/tagged/google-cloud-dataflow)
tagged with `google-cloud-dataflow`.

Please use [issue tracker](https://github.com/GoogleCloudPlatform/DataflowJavaSDK/issues)
on GitHub to report any bugs, comments or questions regarding SDK development.

## More Information

* [Google Cloud Dataflow](https://cloud.google.com/dataflow/)
* [Dataflow Concepts and Programming Model](https://cloud.google.com/dataflow/java-sdk/building-a-pipeline)
* [Javadoc](https://cloud.google.com/dataflow/java-sdk/JavaDoc/index)
