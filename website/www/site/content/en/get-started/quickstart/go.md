---
title: "Beam Quickstart for Go"
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

# Apache Beam Go SDK quickstart

This quickstart shows you how to run an
[example pipeline](https://github.com/apache/beam-starter-go) written with the
[Apache Beam Go SDK](/documentation/sdks/go), using the
[Direct Runner](/documentation/runners/direct/). The Direct Runner executes
pipelines locally on your machine.

If you're interested in contributing to the Apache Beam Go codebase, see the
[Contribution Guide](/contribute).

On this page:

{{< toc >}}

## Set up your development environment

Make sure you have a [Go](https://go.dev/) development environment ready. If
not, follow the instructions in the
[Download and install](https://go.dev/doc/install) page.

## Clone the GitHub repository

Clone or download the
[apache/beam-starter-go](https://github.com/apache/beam-starter-go) GitHub
repository and change into the `beam-starter-go` directory.

{{< highlight >}}
git clone https://github.com/apache/beam-starter-go.git
cd beam-starter-go
{{< /highlight >}}

## Run the quickstart

Run the following command:

{{< highlight >}}
go run main.go --input-text="Greetings"
{{< /highlight >}}

The output is similar to the following:

{{< highlight >}}
Hello
World!
Greetings
{{< /highlight >}}

The lines might appear in a different order.

## Explore the code

The main code file for this quickstart is **main.go**
([GitHub](https://github.com/apache/beam-starter-go/blob/main/main.go)).
The code performs the following steps:

1. Create a Beam pipeline.
3. Create an initial `PCollection`.
3. Apply transforms.
4. Run the pipeline, using the Direct Runner.

### Create a pipeline

Before creating a pipeline, call the [`Init`][Init] function:

```go
beam.Init()
```

Then create the pipeline:

```go
pipeline, scope := beam.NewPipelineWithRoot()
```

The [`NewPipelineWithRoot`][newPipelineWithRoot] function returns a new
`Pipeline` object, along with the pipeline's root scope. A *scope* is a
hierarchical grouping for composite transforms.


### Create an initial PCollection

The `PCollection` abstraction represents a potentially distributed,
multi-element data set. A Beam pipeline needs a source of data to populate an
initial `PCollection`. The source can be bounded (with a known, fixed size) or
unbounded (with unlimited size).

This example uses the [`Create`][Create] function to create a `PCollection`
from an in-memory array of strings. The resulting `PCollection` contains the
strings "hello", "world!", and a user-provided input string.

```go
elements := beam.Create(scope, "hello", "world!", input_text)
```

### Apply transforms to the PCollection

Transforms can change, filter, group, analyze, or otherwise process the
elements in a `PCollection`.

This example adds a [ParDo](/documentation/programming-guide/#pardo) transform
to convert the input strings to title case:

```go
elements = beam.ParDo(scope, strings.Title, elements)
```

The [`ParDo`][ParDo] function takes the parent scope, a transform function that
will be applied to the data, and the input PCollection. It returns the output
PCollection.

The previous example uses the built-in [`strings.Title`][Title] function for
the transform. You can also provide an application-defined function to a ParDo.
For example:

```go
func logAndEmit(ctx context.Context, element string, emit func(string)) {
    beamLog.Infoln(ctx, element)
    emit(element)
}
```

This function logs the input element and returns the same element unmodified.
Create a ParDo for this function as follows:

```
beam.ParDo(scope, logAndEmit, elements)
```

At runtime, the ParDo will call the `logAndEmit` function on each element in
the input collection.

### Run the pipeline

The code shown in the previous sections defines a pipeline, but does not
process any data yet. To process data, you run the pipeline:

```go
beamx.Run(ctx, pipeline)
```

A Beam [runner](https://beam.apache.org/documentation/basics/#runner) runs a
Beam pipeline on a specific platform. This example uses the Direct Runner,
which is the default runner if you don't specify one. The Direct Runner runs
the pipeline locally on your machine. It is meant for testing and development,
rather than being optimized for efficiency. For more information, see
[Using the Direct Runner](https://beam.apache.org/documentation/runners/direct/).

For production workloads, you typically use a distributed runner that runs the
pipeline on a big data processing system such as Apache Flink, Apache Spark, or
Google Cloud Dataflow. These systems support massively parallel processing.

## Next Steps

* Learn more about the [Beam SDK for Go](/documentation/sdks/go/)
  and look through the
  [Go SDK API reference](https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam).
* Take a self-paced tour through our
  [Learning Resources](/documentation/resources/learning-resources).
* Dive in to some of our favorite
  [Videos and Podcasts](/get-started/resources/videos-and-podcasts).
* Join the Beam [users@](/community/contact-us) mailing list.

Please don't hesitate to [reach out](/community/contact-us) if you encounter any
issues!

[Init]: https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam#Init
[Create]: https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam#Create
[NewPipelineWithRoot]: https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam#NewPipelineWithRoot
[ParDo]: https://pkg.go.dev/github.com/apache/beam/sdks/v2/go/pkg/beam#ParDo
[Title]: https://pkg.go.dev/strings#Title

