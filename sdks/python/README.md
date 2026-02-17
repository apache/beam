# Apache Beam

[Apache Beam](http://beam.apache.org/) is a unified model for defining both batch and streaming data-parallel processing pipelines, as well as a set of language-specific SDKs for constructing pipelines and Runners for executing them on distributed processing backends, including [Apache Flink](http://flink.apache.org/), [Apache Spark](http://spark.apache.org/), [Google Cloud Dataflow](http://cloud.google.com/dataflow/), and [Hazelcast Jet](https://jet.hazelcast.org/).


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


## Get started with the Python SDK

Get started with the [Beam Python SDK quickstart](/get-started/quickstart-py) to set up your Python development environment, get the Beam SDK for Python, and run an example pipeline. Then, read through the [Beam programming guide](/documentation/programming-guide) to learn the basic concepts that apply to all SDKs in Beam. The [Python Tips document](../../contributor-docs/python-tips.md) is also a useful resource for setting up a development environment and performing common processes.

See the [Python API reference](https://beam.apache.org/releases/pydoc/) for more information on individual APIs.

## Python streaming pipelines

Python [streaming pipeline execution](https://beam.apache.org/documentation/sdks/python-streaming)
is available (with some [limitations](https://beam.apache.org//documentation/sdks/python-streaming/#unsupported-features))
starting with Beam SDK version 2.5.0.

## Python type safety

Python is a dynamically-typed language with no static type checking. The Beam SDK for Python uses type hints during pipeline construction and runtime to try to emulate the correctness guarantees achieved by true static typing. [Ensuring Python Type Safety](https://beam.apache.org/documentation/sdks/python-type-safety) walks through how to use type hints, which help you to catch potential bugs up front with the [Direct Runner](https://beam.apache.org//documentation/runners/direct/).

## Managing Python pipeline dependencies

When you run your pipeline locally, the packages that your pipeline depends on are available because they are installed on your local machine. However, when you want to run your pipeline remotely, you must make sure these dependencies are available on the remote machines. [Managing Python Pipeline Dependencies](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies) shows you how to make your dependencies available to the remote workers.

## Developing new I/O connectors for Python

The Beam SDK for Python provides an extensible API that you can use to create
new I/O connectors. See the [Developing I/O connectors overview](https://beam.apache.org/documentation/io/developing-io-overview)
for information about developing new I/O connectors and links to
language-specific implementation guidance.

## Making machine learning inferences with Python

To integrate machine learning models into your pipelines for making inferences, use the RunInference API for PyTorch and Scikit-learn models. If you are using TensorFlow models, you can make use of the
[library from `tfx_bsl`](https://github.com/tensorflow/tfx-bsl/tree/master/tfx_bsl/beam).

You can create multiple types of transforms using the RunInference API: the API takes multiple types of setup parameters from model handlers, and the parameter type determines the model implementation. For more information,
see [About Beam ML](https://beam.apache.org/documentation/ml/about-ml).

[TensorFlow Extended (TFX)](https://www.tensorflow.org/tfx) is an end-to-end platform for deploying production ML pipelines. TFX is integrated with Beam. For more information, see [TFX user guide](https://www.tensorflow.org/tfx/guide).

## Python multi-language pipelines quickstart

Apache Beam lets you combine transforms written in any supported SDK language and use them in one multi-language pipeline. To learn how to create a multi-language pipeline using the Python SDK, see the [Python multi-language pipelines quickstart](https://beam.apache.org/documentation/sdks/python-multi-language-pipelines).

## Unrecoverable Errors in Beam Python

Some common errors can occur during worker start-up and prevent jobs from starting. To learn about these errors and how to troubleshoot them in the Python SDK, see [Unrecoverable Errors in Beam Python](https://beam.apache.org/documentation/sdks/python-unrecoverable-errors).

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
    <td><a href="https://beam.apache.org/get-started/quickstart-py" target="_blank" rel="noopener noreferrer">Python Quickstart</a></td>
    <td>A guide to getting started with the Python SDK.</td>
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
    <td><a href="https://metrics.beam.apache.org/" target="_blank" rel="noopener noreferrer">Community Metrics </a></td>
    <td>Beam's Git Community Metrics.</td>
  </tr>
</tbody>
</table>


## Contribution

Instructions for building and testing Beam itself
are in the [contribution guide](https://beam.apache.org/contribute).

## Contact Us

To get involved with Apache Beam:

* [Subscribe to](https://beam.apache.org/community/contact-us/#:~:text=Subscribe%20and%20Unsubscribe) or e-mail the [user@beam.apache.org](http://mail-archives.apache.org/mod_mbox/beam-user/) list.
* [Subscribe to](https://beam.apache.org/community/contact-us/#:~:text=Subscribe%20and%20Unsubscribe) or e-mail the [dev@beam.apache.org](http://mail-archives.apache.org/mod_mbox/beam-dev/) list.
* [Join ASF Slack](https://s.apache.org/slack-invite) on [#beam channel](https://s.apache.org/beam-slack-channel)
* [Report an issue](https://github.com/apache/beam/issues/new/choose).
