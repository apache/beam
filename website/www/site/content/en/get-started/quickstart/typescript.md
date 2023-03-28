---
title: "Beam Quickstart for Typescript"
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

# Apache Beam Typescript SDK quickstart

This quickstart shows you how to run an
[example pipeline](https://github.com/apache/beam-starter-typescript) written with
the [Apache Beam Typescript SDK](/documentation/sdks/typescript), using the
[Direct Runner](/documentation/runners/direct/). The Direct Runner executes
pipelines locally on your machine.

If you're interested in contributing to the Apache Beam Typescript codebase, see the
[Contribution Guide](/contribute).

On this page:

{{< toc >}}

## Set up your development environment

Make sure you have a [Node.js](https://nodejs.org/) development environment installed.
If you don't, you can download and install it from the
[downloads page](https://nodejs.org/en/download/).

Due to its extensive use of cross-language transforms, it is recommended that
Python 3 and Java be available on the system as well.

## Clone the GitHub repository

Clone or download the
[apache/beam-starter-typescript](https://github.com/apache/beam-starter-typescript)
GitHub repository and change into the `beam-starter-typescript` directory.

{{< highlight >}}
git clone https://github.com/apache/beam-starter-typescript.git
cd beam-starter-typescript
{{< /highlight >}}

## Install the project dependences

Run the following command to install the project's dependencies.

{{< highlight >}}
npm install
{{< /highlight >}}

## Compile the pipeline

The pipeline is then built with

{{< highlight >}}
npm run build
{{< /highlight >}}

## Run the quickstart

Run the following command:

{{< highlight >}}
node dist/src/main.js --input_text="Greetings"
{{< /highlight >}}

The output is similar to the following:

{{< highlight >}}
Hello
World!
Greetings
{{< /highlight >}}

The lines might appear in a different order.

## Explore the code

The main code file for this quickstart is **app.ts**
([GitHub](https://github.com/apache/beam-starter-typescript/blob/main/src/app.ts)).
The code performs the following steps:

1. Define a Beam pipeline that.
  + Creates an initial `PCollection`.
  + Applies a transform (map) to the `PCollection`.
2. Run the pipeline, using the Direct Runner.

### Create a pipeline

A `Pipeline` is simply a callable that takes a single `root` object.
The `Pipeline` function builds up the graph of transformations to be executed.

### Create an initial PCollection

The `PCollection` abstraction represents a potentially distributed,
multi-element data set. A Beam pipeline needs a source of data to populate an
initial `PCollection`. The source can be bounded (with a known, fixed size) or
unbounded (with unlimited size).

This example uses the
[`Create`](https://beam.apache.org/releases/typedoc/current/functions/transforms_create.create.html)
method to create a `PCollection` from an in-memory array of strings. The
resulting `PCollection` contains the strings "Hello", "World!", and a
user-provided input string.

```typescript
root.apply(beam.create(["Hello", "World!", input_text]))
```

### Apply a transform to the PCollection

Transforms can change, filter, group, analyze, or otherwise process the
elements in a `PCollection`. This example uses the
[`Map`](https://beam.apache.org/releases/typedoc/current/classes/pvalue.PCollection.html#map)
transform, which maps the elements of a collection into a new collection:

```typescript
.map(printAndReturn);
```

For convenience, `PColletion` has a `map` method, but more generally transforms
are applied with `.apply(someTransform())`.

### Run the pipeline

To run the pipeline, a runner is created (possibly with some options)

```typescript
createRunner(options)
```

and then its `run` method is invoked on the pipeline callable created above.

```typescript
.run(createPipeline(...));
```

A Beam [runner](/documentation/basics/#runner) runs a Beam pipeline on a
specific platform. If you don't specify a runner, the Direct Runner is the
default. The Direct Runner runs the pipeline locally on your machine. It is
meant for testing and development, rather than being optimized for efficiency.
For more information, see
[Using the Direct Runner](/documentation/runners/direct/).

For production workloads, you typically use a distributed runner that runs the
pipeline on a big data processing system such as Apache Flink, Apache Spark, or
Google Cloud Dataflow. These systems support massively parallel processing.
Different runners can be requested via the runner property on options, e.g.
`createRunner({runner: "dataflow"})` or `createRunner({runner: "flink"})`.
In this example this value can be passed in via the command line as
`--runner=...`, e.g. to run on Dataflow one would write

```sh
node dist/src/main.js \
    --runner=dataflow \
    --project=${PROJECT_ID} \
    --tempLocation=gs://${GCS_BUCKET}/wordcount-js/temp --region=${REGION}
```


## Next Steps

* Learn more about the [Beam SDK for Typescript](/documentation/sdks/typescript/)
  and look through the
  [Typescript SDK API reference](https://beam.apache.org/releases/typedoc/current).
* Take a self-paced tour through our
  [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite
  [Videos and Podcasts](/documentation/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any
issues!
