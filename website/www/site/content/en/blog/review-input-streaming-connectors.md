---
title:  "A review of input streaming connectors"
date:   2018-08-20 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2018/08/20/review-input-streaming-connectors.html
authors:
  - lkuligin
  - jphalip
---
<!--
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

In this post, you'll learn about the current state of support for input streaming connectors in [Apache Beam](/). For more context, you'll also learn about the corresponding state of support in [Apache Spark](https://spark.apache.org/).<!--more-->

With batch processing, you might load data from any source, including a database system. Even if there are no specific SDKs available for those database systems, you can often resort to using a [JDBC](https://en.wikipedia.org/wiki/Java_Database_Connectivity) driver. With streaming, implementing a proper data pipeline is arguably more challenging as generally fewer source types are available. For that reason, this article particularly focuses on the streaming use case.

## Connectors for Java

Beam has an official [Java SDK](/documentation/sdks/java/) and has several execution engines, called [runners](/documentation/runners/capability-matrix/). In most cases it is fairly easy to transfer existing Beam pipelines written in Java or Scala to a Spark environment by using the [Spark Runner](/documentation/runners/spark/).

Spark is written in Scala and has a [Java API](https://spark.apache.org/docs/latest/api/java/). Spark's source code compiles to [Java bytecode](https://en.wikipedia.org/wiki/Java_(programming_language)#Java_JVM_and_Bytecode) and the binaries are run by a [Java Virtual Machine](https://en.wikipedia.org/wiki/Java_virtual_machine). Scala code is interoperable with Java and therefore has native compatibility with Java libraries (and vice versa).

Spark offers two approaches to streaming: [Discretized Streaming](https://spark.apache.org/docs/latest/streaming-programming-guide.html) (or DStreams) and [Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). DStreams are a basic abstraction that represents a continuous series of [Resilient Distributed Datasets](https://spark.apache.org/docs/latest/rdd-programming-guide.html) (or RDDs). Structured Streaming was introduced more recently (the alpha release came with Spark 2.1.0) and is based on a [model](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#programming-model) where live data is continuously appended to a table structure.

Spark Structured Streaming supports [file sources](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/streaming/DataStreamReader.html) (local filesystems and HDFS-compatible systems like Cloud Storage or S3) and [Kafka](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) as streaming [inputs](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources). Spark maintains built-in connectors for DStreams aimed at third-party services, such as Kafka or Flume, while other connectors are available through linking external dependencies, as shown in the table below.

Below are the main streaming input connectors for available for Beam and Spark DStreams in Java:

<table class="table table-bordered">
  <tr>
   <td>
   </td>
   <td>
   </td>
   <td><strong>Apache Beam</strong>
   </td>
   <td><strong>Apache Spark DStreams</strong>
   </td>
  </tr>
  <tr>
   <td rowspan="2" >File Systems
   </td>
   <td>Local<br>(Using the <code>file://</code> URI)
   </td>
   <td><a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/TextIO.html">TextIO</a>
   </td>
   <td><a href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/streaming/StreamingContext.html#textFileStream-java.lang.String-">textFileStream</a><br>(Spark treats most Unix systems as HDFS-compatible, but the location should be accessible from all nodes)
   </td>
  </tr>
  <tr>
   <td>HDFS<br>(Using the <code>hdfs://</code> URI)
   </td>
    <td><a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/FileIO.html">FileIO</a> + <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/hdfs/HadoopFileSystemOptions.html">HadoopFileSystemOptions</a>
   </td>
   <td><a href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/streaming/util/HdfsUtils.html">HdfsUtils</a>
   </td>
  </tr>
  <tr>
   <td rowspan="2" >Object Stores
   </td>
   <td>Cloud Storage<br>(Using the <code>gs://</code> URI)
   </td>
   <td><a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/FileIO.html">FileIO</a> + <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/extensions/gcp/options/GcsOptions.html">GcsOptions</a>
   </td>
   <td rowspan="2" ><a href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkContext.html#hadoopConfiguration--">hadoopConfiguration</a>
and <a href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/streaming/StreamingContext.html#textFileStream-java.lang.String-">textFileStream</a>
   </td>
  </tr>
  <tr>
   <td>S3<br>(Using the <code>s3://</code> URI)
   </td>
    <td><a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/FileIO.html">FileIO</a> + <a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/aws/options/S3Options.html">S3Options</a>
   </td>
  </tr>
  <tr>
   <td rowspan="3" >Messaging Queues
   </td>
   <td>Kafka
   </td>
   <td><a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/kafka/KafkaIO.html">KafkaIO</a>
   </td>
   <td><a href="https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html">spark-streaming-kafka</a>
   </td>
  </tr>
  <tr>
   <td>Kinesis
   </td>
   <td><a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/kinesis/KinesisIO.html">KinesisIO</a>
   </td>
   <td><a href="https://spark.apache.org/docs/latest/streaming-kinesis-integration.html">spark-streaming-kinesis</a>
   </td>
  </tr>
  <tr>
   <td>Cloud Pub/Sub
   </td>
   <td><a href="https://beam.apache.org/releases/javadoc/{{< param release_latest >}}/org/apache/beam/sdk/io/gcp/pubsub/PubsubIO.html">PubsubIO</a>
   </td>
   <td><a href="https://github.com/apache/bahir/tree/master/streaming-pubsub">spark-streaming-pubsub</a> from <a href="https://bahir.apache.org">Apache Bahir</a>
   </td>
  </tr>
  <tr>
   <td>Other
   </td>
   <td>Custom receivers
   </td>
   <td><a href="/documentation/io/developing-io-overview/">Read Transforms</a>
   </td>
   <td><a href="https://spark.apache.org/docs/latest/streaming-custom-receivers.html">receiverStream</a>
   </td>
  </tr>
</table>

## Connectors for Python

Beam has an official [Python SDK](/documentation/sdks/python/) that currently supports a subset of the streaming features available in the Java SDK. Active development is underway to bridge the gap between the featuresets in the two SDKs. Currently for Python, the [Direct Runner](/documentation/runners/direct/) and [Dataflow Runner](/documentation/runners/dataflow/) are supported, and [several streaming options](/documentation/sdks/python-streaming/) were introduced in beta in [version 2.5.0](/blog/2018/06/26/beam-2.5.0.html).

Spark also has a Python SDK called [PySpark](https://spark.apache.org/docs/latest/api/python/pyspark.html). As mentioned earlier, Scala code compiles to a bytecode that is executed by the JVM. PySpark uses [Py4J](https://www.py4j.org/), a library that enables Python programs to interact with the JVM and therefore access Java libraries, interact with Java objects, and register callbacks from Java. This allows PySpark to access native Spark objects like RDDs. Spark Structured Streaming supports [file sources](https://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.streaming.DataStreamReader) (local filesystems and HDFS-compatible systems like Cloud Storage or S3) and [Kafka](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html) as streaming inputs.

Below are the main streaming input connectors for available for Beam and Spark DStreams in Python:

<table class="table table-bordered">
  <tr>
   <td>
   </td>
   <td>
   </td>
   <td><strong>Apache Beam</strong>
   </td>
   <td><strong>Apache Spark DStreams</strong>
   </td>
  </tr>
  <tr>
   <td rowspan="2" >File Systems
   </td>
   <td>Local
   </td>
   <td><a href="https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.textio.html">io.textio</a>
   </td>
   <td><a href="https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#pyspark.streaming.StreamingContext.textFileStream">textFileStream</a>
   </td>
  </tr>
  <tr>
   <td>HDFS
   </td>
   <td><a href="https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.hadoopfilesystem.html">io.hadoopfilesystem</a>
   </td>
   <td><a href="https://spark.apache.org/docs/latest/api/java/org/apache/spark/SparkContext.html#hadoopConfiguration--">hadoopConfiguration</a> (Access through <code>sc._jsc</code> with Py4J)
and <a href="https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#pyspark.streaming.StreamingContext.textFileStream">textFileStream</a>
   </td>
  </tr>
  <tr>
   <td rowspan="2" >Object stores
   </td>
   <td>Google Cloud Storage
   </td>
   <td><a href="https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.gcp.gcsio.html">io.gcp.gcsio</a>
   </td>
   <td rowspan="2" ><a href="https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#pyspark.streaming.StreamingContext.textFileStream">textFileStream</a>
   </td>
  </tr>
  <tr>
   <td>S3
   </td>
   <td>N/A
   </td>
  </tr>
  <tr>
   <td rowspan="3" >Messaging Queues
   </td>
   <td>Kafka
   </td>
   <td>N/A
   </td>
   <td><a href="https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#pyspark.streaming.kafka.KafkaUtils">KafkaUtils</a>
   </td>
  </tr>
  <tr>
   <td>Kinesis
   </td>
   <td>N/A
   </td>
   <td><a href="https://spark.apache.org/docs/latest/api/python/pyspark.streaming.html#module-pyspark.streaming.kinesis">KinesisUtils</a>
   </td>
  </tr>
  <tr>
   <td>Cloud Pub/Sub
   </td>
   <td><a href="https://beam.apache.org/releases/pydoc/{{< param release_latest >}}/apache_beam.io.gcp.pubsub.html">io.gcp.pubsub</a>
   </td>
   <td>N/A
   </td>
  </tr>
  <tr>
   <td>Other
   </td>
   <td>Custom receivers
   </td>
   <td><a href="/documentation/sdks/python-custom-io/">BoundedSource and RangeTracker</a>
   </td>
   <td>N/A
   </td>
  </tr>
</table>

## Connectors for other languages

### Scala

Since Scala code is interoperable with Java and therefore has native compatibility with Java libraries (and vice versa), you can use the same Java connectors described above in your Scala programs. Apache Beam also has a [Scala API](https://github.com/spotify/scio) open-sourced [by Spotify](https://labs.spotify.com/2017/10/16/big-data-processing-at-spotify-the-road-to-scio-part-1/).

### Go

A [Go SDK](/documentation/sdks/go/) for Apache Beam is under active development. It is currently experimental and is not recommended for production. Spark does not have an official Go SDK.

### R

Apache Beam does not have an official R SDK. Spark Structured Streaming is supported by an [R SDK](https://spark.apache.org/docs/latest/sparkr.html#structured-streaming), but only for [file sources](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#input-sources) as a streaming input.

## Next steps

We hope this article inspired you to try new and interesting ways of connecting streaming sources to your Beam pipelines!

Check out the following links for further information:

*   See a full list of all built-in and in-progress [I/O Transforms](/documentation/io/built-in/) for Apache Beam.
*   Learn about some Apache Beam mobile gaming pipeline [examples](/get-started/mobile-gaming-example/).
