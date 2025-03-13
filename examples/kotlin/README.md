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
[word count](https://github.com/apache/beam/blob/master/examples/kotlin/src/main/java/org/apache/beam/examples/kotlin) examples, which computes word frequencies.  This series of four successively more detailed pipelines is described in detail in the accompanying [walkthrough](https://beam.apache.org/get-started/wordcount-example/).

1. [`MinimalWordCount`](https://github.com/apache/beam/blob/master/examples/kotlin/src/main/java/org/apache/beam/examples/kotlin/MinimalWordCount.kt) is the simplest word count pipeline and introduces basic concepts like [Pipelines](https://beam.apache.org/documentation/programming-guide/#pipeline),
[PCollections](https://beam.apache.org/documentation/programming-guide/#pcollection),
[ParDo](https://beam.apache.org/documentation/programming-guide/#pardo),
and [reading and writing data](https://beam.apache.org/documentation/programming-guide/#io) from external storage.

1. [`WordCount`](https://github.com/apache/beam/blob/master/examples/kotlin/src/main/java/org/apache/beam/examples/kotlin/WordCount.kt) introduces best practices like [PipelineOptions](https://beam.apache.org/documentation/programming-guide/#pipeline) and custom [PTransforms](https://beam.apache.org/documentation/programming-guide/#transforms-composite).

1. [`DebuggingWordCount`](https://github.com/apache/beam/blob/master/examples/kotlin/src/main/java/org/apache/beam/examples/kotlin/DebuggingWordCount.kt)
demonstrates some best practices for instrumenting your pipeline code.

1. [`WindowedWordCount`](https://github.com/apache/beam/blob/master/examples/kotlin/src/main/java/org/apache/beam/examples/kotlin/WindowedWordCount.kt) shows how to run the same pipeline over either unbounded PCollections in streaming mode or bounded PCollections in batch mode.

## Running Examples

See [Apache Beam WordCount Example](https://beam.apache.org/get-started/wordcount-example/) for information on running these examples.

## Beyond Word Count [WIP]

After you've finished running your first few word count pipelines, take a look at the [cookbook](https://github.com/apache/beam/blob/master/examples/kotlin/src/main/java/org/apache/beam/examples/kotlin/cookbook)
directory for some common and useful patterns like joining, filtering, and combining.

The [complete](https://github.com/apache/beam/blob/master/examples/kotlin/src/main/java/org/apache/beam/examples/kotlin/complete)
directory contains a few realistic end-to-end pipelines.

See the other examples as well. This directory includes a Java 8 version of the
MinimalWordCount example, as well as a series of examples in a simple 'mobile
gaming' domain. This series introduces some advanced concepts.
