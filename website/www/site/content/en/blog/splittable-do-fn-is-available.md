---
title:  "Splittable DoFn in Apache Beam is Ready to Use"
date:   2020-12-14 00:00:01 -0800
categories:
  - blog
aliases:
  - /blog/2020/12/14/splittable-do-fn-is-available.html
authors:
  - boyuanzz
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

We are pleased to announce that Splittable DoFn (SDF) is ready for use in the Beam Python, Java,
and Go SDKs for versions 2.25.0 and later.

In 2017, [Splittable DoFn Blog Post](/blog/splittable-do-fn/) proposed
to build [Splittable DoFn](https://s.apache.org/splittable-do-fn) APIs as the new recommended way of
building I/O connectors. Splittable DoFn is a generalization of `DoFn` that gives it the core
capabilities of `Source` while retaining `DoFn`'s syntax, flexibility, modularity, and ease of
coding. Thus, it becomes much easier to develop complex I/O connectors with simpler and reusable
code.

SDF has three advantages over the existing `UnboundedSource` and `BoundedSource`:
* SDF provides a unified set of APIs to handle both unbounded and bounded cases.
* SDF enables reading from source descriptors dynamically.
  - Taking KafkaIO as an example, within `UnboundedSource`/`BoundedSource` API, you must specify
  the topic and partition you want to read from during pipeline construction time. There is no way
  for `UnboundedSource`/`BoundedSource` to accept topics and partitions as inputs during execution
  time. But it's built-in to SDF.
* SDF fits in as any node on a pipeline freely with the ability of splitting.
  - `UnboundedSource`/`BoundedSource` has to be the root node of the pipeline to gain performance
  benefits from splitting strategies, which limits many real-world usages. This is no longer a limit
  for an SDF.

As SDF is now ready to use with all the mentioned improvements, it is the recommended
way to build the new I/O connectors. Try out building your own Splittable DoFn by following the
[programming guide](/documentation/programming-guide/#splittable-dofns). We
have provided tonnes of common utility classes such as common types of `RestrictionTracker` and
`WatermarkEstimator` in Beam SDK, which will help you onboard easily. As for the existing I/O
connectors, we have wrapped `UnboundedSource` and `BoundedSource` implementations into Splittable
DoFns, yet we still encourage developers to convert `UnboundedSource`/`BoundedSource` into actual
Splittable DoFn implementation to gain more performance benefits.

Many thanks to every contributor who brought this highly anticipated design into the data processing
world. We are really excited to see that users benefit from SDF.

Below are some real-world SDF examples for you to explore.

## Real world Splittable DoFn examples

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

**Go Examples**
 *  [textio.ReadSdf](https://github.com/apache/beam/blob/ce190e11332469ea59b6c9acf16ee7c673ccefdd/sdks/go/pkg/beam/io/textio/sdf.go#L40) implements reading from text files using a splittable DoFn.
