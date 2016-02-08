# Apache Beam

[Apache Beam](http://beam.incubator.apache.org) provides a simple, powerful
programming model for building both batch and streaming parallel data processing
pipelines. It also covers the data integration processus.

[General usage](http://beam.incubator.apache.org/documentation/getting-started) is
a good starting point for Apache Beam.

You can take a look on the [Beam Examples](http://git-wip-us.apache.org/repos/asf/incubator-beam/examples).

## Status [Build Status](http://builds.apache.org/job/beam-master)

## Overview

The key concepts in this programming model are:

* `PCollection`: represents a collection of data, which could be bounded or unbounded in size.
* `PTransform`: represents a computation that transforms input PCollections into output PCollections.
* `Pipeline`: manages a directed acyclic graph of PTransforms and PCollections that is ready for execution.
* `PipelineRunner`: specifies where and how the pipeline should execute.

We provide the following PipelineRunners:

  1. The `DirectPipelineRunner` runs the pipeline on your local machine.
  2. The `BlockingDataflowPipelineRunner` submits the pipeline to the Dataflow Service via the `DataflowPipelineRunner`
and then prints messages about the job status until the execution is complete.
  3. The `SparkPipelineRunner` runs the pipeline on an Apache Spark cluster.
  4. The `FlinkPipelineRunner` runs the pipeline on an Apache Flink cluster.

## Getting Started

The following command will build both the `sdk` and `example` modules and
install them in your local Maven repository:

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

After building and installing, you can execute the `WordCount` and other
example pipelines by following the instructions in this [README](https://git-wip-us.apache.org/repos/asf/incubator-beam/examples/README.md).

## Contact Us

You can subscribe on the mailing lists to discuss and get involved in Apache Beam:

* [Subscribe](mailto:user-subscribe@beam.incubator.apache.org) on the [user@beam.incubator.apache.org](mailto:user@beam.incubator.apache.org)
* [Subscribe](mailto:dev-subscribe@beam.incubator.apache.org) on the [dev@beam.incubator.apache.org](mailto:dev@beam.incubator.apache.org)

You can report issue on [Jira](https://issues.apache.org/jira/browse/BEAM).

## More Information

* [Apache Beam](http://beam.incubator.apache.org)
* [Apache Beam Documentation](http://beam.incubator.apache.org/documentation)
