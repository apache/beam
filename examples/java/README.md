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
[ParDo](https://beam.apache.org/documentation/programming-guide/#transforms-pardo),
and [reading and writing data](https://beam.apache.org/documentation/programming-guide/#io) from external storage.

1. [`WordCount`](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WordCount.java) introduces best practices like [PipelineOptions](https://beam.apache.org/documentation/programming-guide/#pipeline) and custom [PTransforms](https://beam.apache.org/documentation/programming-guide/#transforms-composite).

1. [`DebuggingWordCount`](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/DebuggingWordCount.java)
demonstrates some best practices for instrumenting your pipeline code.

1. [`WindowedWordCount`](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/WindowedWordCount.java) shows how to run the same pipeline over either unbounded PCollections in streaming mode or bounded PCollections in batch mode.

## Building and Running

Change directory into `examples/java` and run the examples:

    mvn compile exec:java \
    -Dexec.mainClass=<MAIN CLASS> \
    -Dexec.args="<EXAMPLE-SPECIFIC ARGUMENTS>"

Alternatively, you may choose to bundle all dependencies into a single JAR and
execute it outside of the Maven environment.

### Direct Runner

You can execute the `WordCount` pipeline on your local machine as follows:

    mvn compile exec:java \
    -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--inputFile=<LOCAL INPUT FILE> --output=<LOCAL OUTPUT FILE>"

To create the bundled JAR of the examples and execute it locally:

    mvn package

    java -cp examples/java/target/beam-examples-java-bundled-<VERSION>.jar \
    org.apache.beam.examples.WordCount \
    --inputFile=<INPUT FILE PATTERN> --output=<OUTPUT FILE>

### Google Cloud Dataflow Runner

After you have followed the general Cloud Dataflow
[prerequisites and setup](https://beam.apache.org/documentation/runners/dataflow/), you can execute
the pipeline on fully managed resources in Google Cloud Platform:

    mvn compile exec:java \
    -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--project=<YOUR CLOUD PLATFORM PROJECT ID> \
    --tempLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=DataflowRunner"

Make sure to use your project id, not the project number or the descriptive name.
The Google Cloud Storage location should be entered in the form of
`gs://bucket/path/to/staging/directory`.

To create the bundled JAR of the examples and execute it in Google Cloud Platform:

    mvn package

    java -cp examples/java/target/beam-examples-java-bundled-<VERSION>.jar \
    org.apache.beam.examples.WordCount \
    --project=<YOUR CLOUD PLATFORM PROJECT ID> \
    --tempLocation=<YOUR CLOUD STORAGE LOCATION> \
    --runner=DataflowRunner

## Other Examples

Other examples can be run similarly by replacing the `WordCount` class path with the example classpath, e.g.
`org.apache.beam.examples.cookbook.CombinePerKeyExamples`,
and adjusting runtime options under the `Dexec.args` parameter, as specified in
the example itself.

Note that when running Maven on Microsoft Windows platform, backslashes (`\`)
under the `Dexec.args` parameter should be escaped with another backslash. For
example, input file pattern of `c:\*.txt` should be entered as `c:\\*.txt`.

## Beyond Word Count

After you've finished running your first few word count pipelines, take a look at the [cookbook](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/cookbook)
directory for some common and useful patterns like joining, filtering, and combining.

The [complete](https://github.com/apache/beam/blob/master/examples/java/src/main/java/org/apache/beam/examples/complete)
directory contains a few realistic end-to-end pipelines.

See the
[Java 8](https://github.com/apache/beam/tree/master/examples/java8/src/main/java/org/apache/beam/examples)
examples as well. This directory includes a Java 8 version of the
MinimalWordCount example, as well as series of examples in a simple 'mobile
gaming' domain. This series introduces some advanced concepts and provides
additional examples of using Java 8 syntax. Other than usage of Java 8 lambda
expressions, the concepts that are used apply equally well in Java 7.
