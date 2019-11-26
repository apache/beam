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

# Example Pipelines

The examples included in this module serve to demonstrate the basic
functionality of Apache Beam, and act as starting points for
the development of more complex pipelines.

## Word Count

A good starting point for new users is our set of
[word count](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples) examples, which computes word frequencies.  This series of four successively more detailed pipelines is described in detail in the accompanying [walkthrough](https://beam.apache.org/get-started/wordcount-example/).

1. [`MinimalWordCount`](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/MinimalWordCount.java) is the simplest word count pipeline and introduces basic concepts like [Pipelines](https://beam.apache.org/documentation/programming-guide/#pipeline),
[PCollections](https://beam.apache.org/documentation/programming-guide/#pcollection),
[ParDo](https://beam.apache.org/documentation/programming-guide/#pardo),
and [reading and writing data](https://beam.apache.org/documentation/programming-guide/#io) from external storage.

1. [`WordCount`](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java) introduces best practices like [PipelineOptions](https://beam.apache.org/documentation/programming-guide/#pipeline) and custom [PTransforms](https://beam.apache.org/documentation/programming-guide/#transforms-composite).

1. [`DebuggingWordCount`](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/DebuggingWordCount.java)
demonstrates some best practices for instrumenting your pipeline code.

1. [`WindowedWordCount`](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WindowedWordCount.java) shows how to run the same pipeline over either unbounded PCollections in streaming mode or bounded PCollections in batch mode.

## Running Examples

See [Apache Beam WordCount Example](https://beam.apache.org/get-started/wordcount-example/) for information on running these examples.

## Beyond Word Count

After you've finished running your first few word count pipelines, take a look at the [cookbook](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook)
directory for some common and useful patterns like joining, filtering, and combining.

The [complete](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete)
directory contains a few realistic end-to-end pipelines.

See the other examples as well. This directory includes a Java 8 version of the
MinimalWordCount example, as well as a series of examples in a simple 'mobile
gaming' domain. This series introduces some advanced concepts. Finally, the
following are user contributed examples:

1. [`CryptoRealTime`](https://github.com/GoogleCloudPlatform/professional-services/tree/master/examples/cryptorealtime) Create an unbounded streaming source/reader and manage basic watermarking, checkpointing and record id for data ingestion, stream trading data from exchanges with BEAM + [Medium post](https://medium.com/@igalic/bigtable-beam-dataflow-cryptocurrencies-gcp-terraform-java-maven-4e7873811e86)
