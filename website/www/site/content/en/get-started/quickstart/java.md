---
title: "Beam Quickstart for Java"
aliases:
  - /get-started/quickstart/
  - /use/quickstart/
  - /getting-started/
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

# Apache Beam Java SDK quickstart

This quickstart shows you how to run an
[example pipeline](https://github.com/apache/beam-starter-java) written with
the [Apache Beam Java SDK](/documentation/sdks/java), using the
[Direct Runner](/documentation/runners/direct/). The Direct Runner executes
pipelines locally on your machine.

If you're interested in contributing to the Apache Beam Java codebase, see the
[Contribution Guide](/contribute).

On this page:

{{< toc >}}

## Set up your development environment

Use [`sdkman`](https://sdkman.io/) to install the Java Development Kit (JDK).

{{< highlight >}}
# Install sdkman
curl -s "https://get.sdkman.io" | bash

# Install Java 17
sdk install java 17.0.5-tem
{{< /highlight >}}

You can use either [Gradle](https://gradle.org/) or
[Apache Maven](https://maven.apache.org/) to run this quickstart:

{{< highlight >}}
# Install Gradle
sdk install gradle

# Install Maven
sdk install maven
{{< /highlight >}}

## Clone the GitHub repository

Clone or download the
[apache/beam-starter-java](https://github.com/apache/beam-starter-java) GitHub
repository and change into the `beam-starter-java` directory.

{{< highlight >}}
git clone https://github.com/apache/beam-starter-java.git
cd beam-starter-java
{{< /highlight >}}

## Run the quickstart

**Gradle**: To run the quickstart with Gradle, run the following command:

{{< highlight >}}
gradle run --args='--inputText=Greetings'
{{< /highlight >}}

**Maven**: To run the quickstart with Maven, run the following command:

{{< highlight >}}
mvn compile exec:java -Dexec.args=--inputText='Greetings'
{{< /highlight >}}

The output is similar to the following:

{{< highlight >}}
Hello
World!
Greetings
{{< /highlight >}}

The lines might appear in a different order.

## Explore the code

The main code file for this quickstart is **App.java**
([GitHub](https://github.com/apache/beam-starter-java/blob/main/src/main/java/com/example/App.java)).
The code performs the following steps:

1. Create a Beam pipeline.
3. Create an initial `PCollection`.
3. Apply a transform to the `PCollection`.
4. Run the pipeline, using the Direct Runner.

### Create a pipeline

The code first creates a `Pipeline` object. The `Pipeline` object builds up the
graph of transformations to be executed.

```java
var options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
var pipeline = Pipeline.create(options);
```

The `PipelineOptions` object lets you set various options for the pipeline. The
`fromArgs` method shown in this example parses command-line arguments, which
lets you set pipeline options through the command line.

### Create an initial PCollection

The `PCollection` abstraction represents a potentially distributed,
multi-element data set. A Beam pipeline needs a source of data to populate an
initial `PCollection`. The source can be bounded (with a known, fixed size) or
unbounded (with unlimited size).

This example uses the
[`Create.of`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/Create.html)
method to create a `PCollection` from an in-memory array of strings. The
resulting `PCollection` contains the strings "Hello", "World!", and a
user-provided input string.

```java
return pipeline
  .apply("Create elements", Create.of(Arrays.asList("Hello", "World!", inputText)))
```

### Apply a transform to the PCollection

Transforms can change, filter, group, analyze, or otherwise process the
elements in a `PCollection`. This example uses the
[`MapElements`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/transforms/MapElements.html)
transform, which maps the elements of a collection into a new collection:

```java
.apply("Print elements",
    MapElements.into(TypeDescriptors.strings()).via(x -> {
      System.out.println(x);
      return x;
    }));
```

where

* `into` specifies the data type for the elements in the output collection.
* `via` defines a mapping function that is called on each element of the input
  collection to create the output collection.

In this example, the mapping function is a lambda that just returns the
original value. It also prints the value to `System.out` as a side effect.

### Run the pipeline

The code shown in the previous sections defines a pipeline, but does not
process any data yet. To process data, you run the pipeline:

```java
pipeline.run().waitUntilFinish();
```

A Beam [runner](/documentation/basics/#runner) runs a
Beam pipeline on a specific platform. This example uses the
[Direct Runner](https://beam.apache.org/releases/javadoc/2.3.0/org/apache/beam/runners/direct/DirectRunner.html),
which is the default runner if you don't specify one. The Direct Runner runs
the pipeline locally on your machine. It is meant for testing and development,
rather than being optimized for efficiency. For more information, see
[Using the Direct Runner](/documentation/runners/direct/).

For production workloads, you typically use a distributed runner that runs the
pipeline on a big data processing system such as Apache Flink, Apache Spark, or
Google Cloud Dataflow. These systems support massively parallel processing.

## Next Steps

* Learn more about the [Beam SDK for Java](/documentation/sdks/java/)
  and look through the
  [Java SDK API reference](https://beam.apache.org/releases/javadoc).
* Take a self-paced tour through our
  [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite
  [Videos and Podcasts](/get-started/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any
issues!
