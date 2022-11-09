---
title: "Overview: Developing a new I/O connector"
aliases:
  - /documentation/io/authoring-overview/
  - /documentation/io/io-toc/
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

# Overview: Developing a new I/O connector

_A guide for users who need to connect to a data store that isn't supported by
the [Built-in I/O connectors](/documentation/io/built-in/)_

To connect to a data store that isn’t supported by Beam’s existing I/O
connectors, you must create a custom I/O connector. A connector usually consists
of a source and a sink. All Beam sources and sinks are composite transforms;
however, the implementation of your custom I/O depends on your use case. Here
are the recommended steps to get started:

1. Read this overview and choose your implementation. You can email the
   [Beam dev mailing list](/get-started/support) with any
   questions you might have. In addition, you can check if anyone else is
   working on the same I/O connector.

1. If you plan to contribute your I/O connector to the Beam community, see the
   [Apache Beam contribution guide](/contribute/contribution-guide/).

1. Read the [PTransform style guide](/contribute/ptransform-style-guide/)
   for additional style guide recommendations.


## Sources

For **bounded (batch) sources**, there are currently two options for creating a
Beam source:

1. Use `Splittable DoFn`.

1. Use `ParDo` and `GroupByKey`.


`Splittable DoFn` is the recommended option, as it's the most recent source framework for both
bounded and unbounded sources. This is meant to replace the `Source` APIs(
[BoundedSource](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/BoundedSource.html) and
[UnboundedSource](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/UnboundedSource.html))
in the new system. Read
[Splittable DoFn Programming Guide](/learn/programming-guide/#splittable-dofns) for how to write one
Splittable DoFn. For more information, see the
[roadmap for multi-SDK connector efforts](/roadmap/connectors-multi-sdk/).

For Java and Python **unbounded (streaming) sources**, you must use the `Splittable DoFn`, which
supports features that are useful for streaming pipelines, including checkpointing, controlling
watermark, and tracking backlog.


### When to use the Splittable DoFn interface {#when-to-use-splittable-dofn}

If you are not sure whether to use `Splittable DoFn`, feel free to email the
[Beam dev mailing list](/get-started/support) and we can discuss the specific pros and cons of your
case.

In some cases, implementing a `Splittable DoFn` might be necessary or result in better performance:

* **Unbounded sources:** `ParDo` does not work for reading from unbounded
  sources.  `ParDo` does not support checkpointing or mechanisms like de-duping
  that are useful for streaming data sources.

* **Progress and size estimation:** `ParDo` can't provide hints to runners about
  progress or the size of data they are reading. Without size estimation of the
  data or progress on your read, the runner doesn't have any way to guess how
  large your read will be. Therefore, if the runner attempts to dynamically
  allocate workers, it does not have any clues as to how many workers you might
  need for your pipeline.

* **Dynamic work rebalancing:** `ParDo` does not support dynamic work
  rebalancing, which is used by some readers to improve the processing speed of
  jobs. Depending on your data source, dynamic work rebalancing might not be
  possible.

* **Splitting initially to increase parallelism:** `ParDo`
  does not have the ability to perform initial splitting.

For example, if you'd like to read from a new file format that contains many
records per file, or if you'd like to read from a key-value store that supports
read operations in sorted key order.

### I/O examples using SDFs
**Java Examples**

* [Kafka](https://github.com/apache/beam/blob/571338b0cc96e2e80f23620fe86de5c92dffaccc/sdks/java/io/kafka/src/main/java/org/apache/beam/sdk/io/kafka/ReadFromKafkaDoFn.java#L118):
An I/O connector for [Apache Kafka](https://kafka.apache.org/)
(an open-source distributed event streaming platform).
* [Watch](https://github.com/apache/beam/blob/571338b0cc96e2e80f23620fe86de5c92dffaccc/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/Watch.java#L787):
Uses a polling function producing a growing set of outputs for each input until a per-input
termination condition is met.
* [Parquet](https://github.com/apache/beam/blob/571338b0cc96e2e80f23620fe86de5c92dffaccc/sdks/java/io/parquet/src/main/java/org/apache/beam/sdk/io/parquet/ParquetIO.java#L365):
An I/O connector for [Apache Parquet](https://parquet.apache.org/)
(an open-source columnar storage format).
* [HL7v2](https://github.com/apache/beam/blob/6fdde4f4eab72b49b10a8bb1cb3be263c5c416b5/sdks/java/io/google-cloud-platform/src/main/java/org/apache/beam/sdk/io/gcp/healthcare/HL7v2IO.java#L493):
An I/O connector for HL7v2 messages (a clinical messaging format that provides data about events
that occur inside an organization) part of
[Google’s Cloud Healthcare API](https://cloud.google.com/healthcare).
* [BoundedSource wrapper](https://github.com/apache/beam/blob/571338b0cc96e2e80f23620fe86de5c92dffaccc/sdks/java/core/src/main/java/org/apache/beam/sdk/io/Read.java#L248):
A wrapper which converts an existing [BoundedSource](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/BoundedSource.html)
implementation to a splittable DoFn.
* [UnboundedSource wrapper](https://github.com/apache/beam/blob/571338b0cc96e2e80f23620fe86de5c92dffaccc/sdks/java/core/src/main/java/org/apache/beam/sdk/io/Read.java#L432):
A wrapper which converts an existing [UnboundedSource](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/UnboundedSource.html)
implementation to a splittable DoFn.

**Python Examples**
* [BoundedSourceWrapper](https://github.com/apache/beam/blob/571338b0cc96e2e80f23620fe86de5c92dffaccc/sdks/python/apache_beam/io/iobase.py#L1375):
A wrapper which converts an existing [BoundedSource](https://beam.apache.org/releases/pydoc/current/apache_beam.io.iobase.html#apache_beam.io.iobase.BoundedSource)
implementation to a splittable DoFn.

### Using ParDo and GroupByKey

For data stores or file types where the data can be read in parallel, you can
think of the process as a mini-pipeline. This often consists of two steps:

1. Splitting the data into parts to be read in parallel

2. Reading from each of those parts

Each of those steps will be a `ParDo`, with a `GroupByKey` in between. The
`GroupByKey` is an implementation detail, but for most runners `GroupByKey`
allows the runner to use different numbers of workers in some situations:

* Determining how to split up the data to be read into chunks

* Reading data, which often benefits from more workers

In addition, `GroupByKey` also allows dynamic work rebalancing to happen on
runners that support the feature.

Here are some examples of read transform implementations that use the "reading
as a mini-pipeline" model when data can be read in parallel:

* **Reading from a file glob**: For example, reading all files in "~/data/**".
  * Get File Paths `ParDo`: As input, take in a file glob. Produce a
    `PCollection` of strings, each of which is a file path.
  * Reading `ParDo`: Given the `PCollection` of file paths, read each one,
    producing a `PCollection` of records.

* **Reading from a NoSQL database** (such as Apache HBase): These databases
  often allow reading from ranges in parallel.
  * Determine Key Ranges `ParDo`: As input, receive connection information for
    the database and the key range to read from. Produce a `PCollection` of key
    ranges that can be read in parallel efficiently.
  * Read Key Range `ParDo`: Given the `PCollection` of key ranges, read the key
    range, producing a `PCollection` of records.

For data stores or files where reading cannot occur in parallel, reading is a
simple task that can be accomplished with a single `ParDo`+`GroupByKey`. For
example:

  * **Reading from a database query**: Traditional SQL database queries often
    can only be read in sequence. In this case, the `ParDo` would establish a
    connection to the database and read batches of records, producing a
    `PCollection` of those records.

  * **Reading from a gzip file**: A gzip file must be read in order, so the read
    cannot be parallelized. In this case, the `ParDo` would open the file and
    read in sequence, producing a `PCollection` of records from the file.

## Sinks

To create a Beam sink, we recommend that you use a `ParDo` that writes the
received records to the data store. To develop more complex sinks (for example,
to support data de-duplication when failures are retried by a runner), use
`ParDo`, `GroupByKey`, and other available Beam transforms.
Many data services are optimized to write batches of elements at a time,
so it may make sense to group the elements into batches before writing.
Persistent connections can be initialized in a DoFn's `setUp` or `startBundle`
method rather than upon the receipt of every element as well.
It should also be noted that in a large-scale, distributed system work can
[fail and/or be retried](/documentation/runtime/model/), so it is preferable to
make the external interactions idempotent when possible.

For **file-based sinks**, you can use the `FileBasedSink` abstraction that is
provided by both the Java and Python SDKs. Beam's `FileSystems` utility classes
can also be useful for reading and writing files. See our language specific
implementation guides for more details:




