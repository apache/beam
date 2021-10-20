---
title: "Basics of the Beam model"
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

# Basics of the Beam model

Suppose you have a data processing engine that can pretty easily process graphs
of operations. You want to integrate it with the Beam ecosystem to get access
to other languages, great event time processing, and a library of connectors.
You need to know the core vocabulary:

 * [_Pipeline_](#pipeline) - A pipeline is a user-constructed graph of
   transformations that defines the desired data processing operations.
 * [_PCollection_](#pcollection) - A `PCollection` is a data set or data
   stream. The data that a pipeline processes is part of a PCollection.
 * [_PTransform_](#ptransform) - A `PTransform` (or _transform_) represents a
   data processing operation, or a step, in your pipeline. A transform is
   applied to zero or more `PCollection` objects, and produces zero or more
   `PCollection` objects.
 * _SDK_ - A language-specific library for pipeline authors (we often call them
   "users" even though we have many kinds of users) to build transforms,
   construct their pipelines and submit them to a runner
 * _Runner_ - You are going to write a piece of software called a runner that
   takes a Beam pipeline and executes it using the capabilities of your data
   processing engine.

These concepts may be very similar to your processing engine's concepts. Since
Beam's design is for cross-language operation and reusable libraries of
transforms, there are some special features worth highlighting.

### Pipeline

A Beam pipeline is a graph (specifically, a
[directed acyclic graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph))
of all the data and computations in your data processing task. This includes
reading input data, transforming that data, and writing output data. A pipeline
is constructed by a user in their SDK of choice. Then, the pipeline makes its
way to the runner either through the SDK directly or through the Runner API's
RPC interface. For example, this diagram shows a branching pipeline:

![The pipeline applies two transforms to a single input collection. Each
  transform produces an output collection.](/images/design-your-pipeline-multiple-pcollections.svg)

In this diagram, the boxes represent the parallel computations called
[_PTransforms_](#ptransform)  and the arrows with the circles represent the data
(in the form of [_PCollections_](#pcollection)) that flows between the
transforms. The data might be bounded, stored, data sets, or the data might also
be unbounded streams of data. In Beam, most transforms apply equally to bounded
and unbounded data.

You can express almost any computation that you can think of as a graph as a
Beam pipeline. A Beam driver program typically starts by creating a `Pipeline`
object, and then uses that object as the basis for creating the pipeline’s data
sets and its transforms.

For more information about pipelines, see the following pages:

 * [Beam Programming Guide: Overview](/documentation/programming-guide/#overview)
 * [Beam Programming Guide: Creating a pipeline](/documentation/programming-guide/#creating-a-pipeline)
 * [Design your pipeline](/documentation/pipelines/design-your-pipeline)
 * [Create your pipeline](/documentation/pipeline/create-your-pipeline)

### PTransform

A `PTransform` (or transform) represents a data processing operation, or a step,
in your pipeline. A transform is usually applied to one or more input
`PCollection` objects. Transforms that read input are an exception; these
transforms might not have an input `PCollection`.

You provide transform processing logic in the form of a function object
(colloquially referred to as “user code”), and your user code is applied to each
element of the input PCollection (or more than one PCollection). Depending on
the pipeline runner and backend that you choose, many different workers across a
cluster might execute instances of your user code in parallel. The user code
that runs on each worker generates the output elements that are added to zero or
more output `PCollection` objects.

The Beam SDKs contain a number of different transforms that you can apply to
your pipeline’s PCollections. These include general-purpose core transforms,
such as `ParDo` or `Combine`. There are also pre-written composite transforms
included in the SDKs, which combine one or more of the core transforms in a
useful processing pattern, such as counting or combining elements in a
collection. You can also define your own more complex composite transforms to
fit your pipeline’s exact use case.

The following list has some common transform types:

 * Source transforms such as `TextIO.Read` and `Create`. A source transform
   conceptually has no input.
 * Processing and conversion operations such as `ParDo`, `GroupByKey`,
   `CoGroupByKey`, `Combine`, and `Count`.
 * Outputting transforms such as `TextIO.Write`.
 * User-defined, application-specific composite transforms.

For more information about transforms, see the following pages:

 * [Beam Programming Guide: Overview](/documentation/programming-guide/#overview)
 * [Beam Programming Guide: Transforms](/documentation/programming-guide/#transforms)
 * Beam transform catalog ([Java](/documentation/transforms/java/overview/),
   [Python](/documentation/transforms/python/overview/))

### PCollection

A `PCollection` is an unordered bag of elements. Each `PCollection` is a
potentially distributed, homogeneous data set or data stream, and is owned by
the specific `Pipeline` object for which it is created. Multiple pipelines
cannot share a `PCollection`. Beam pipelines process PCollections, and the
runner is responsible for storing these elements.

A `PCollection` generally contains "big data" (too much data to fit in memory on
a single machine). Sometimes a small sample of data or an intermediate result
might fit into memory on a single machine, but Beam's computational patterns and
transforms are focused on situations where distributed data-parallel computation
is required. Therefore, the elements of a `PCollection` cannot be processed
individually, and are instead processed uniformly in parallel.

The following characteristics of a `PCollection` are important to know.

####  Bounded vs unbounded

A `PCollection` can be either bounded or unbounded.

 - A _bounded_ `PCollection` is a dataset of a known, fixed size (alternatively,
   a dataset that is not growing over time). Bounded data can be processed by
   batch pipelines.
 - An _unbounded_ `PCollection` is a dataset that grows over time, and the
   elements are processed as they arrive. Unbounded data must be processed by
   streaming pipelines.

These two categories derive from the intuitions of batch and stream processing,
but the two are unified in Beam and bounded and unbounded PCollections can
coexist in the same pipeline. If your runner can only support bounded
PCollections, you must reject pipelines that contain unbounded PCollections. If
your runner is only targeting streams, there are adapters in Beam's support code
to convert everything to APIs that target unbounded data.

#### Timestamps

Every element in a `PCollection` has a timestamp associated with it.

When you execute a primitive connector to a storage system, that connector is
responsible for providing initial timestamps. The runner must propagate and
aggregate timestamps. If the timestamp is not important, such as with certain
batch processing jobs where elements do not denote events, the timestamp will be
the minimum representable timestamp, often referred to colloquially as "negative
infinity".

#### Watermarks

Every `PCollection` must have a watermark that estimates how complete the
`PCollection` is.

The watermark is a guess that "we'll never see an element with an earlier
timestamp". Data sources are responsible for producing a watermark. The runner
must implement watermark propagation as PCollections are processed, merged, and
partitioned.

The contents of a `PCollection` are complete when a watermark advances to
"infinity". In this manner, you can discover that an unbounded PCollection is
finite.

#### Windowed elements

Every element in a `PCollection` resides in a window. No element resides in
multiple windows; two elements can be equal except for their window, but they
are not the same.

When elements are read from the outside world, they arrive in the global window.
When they are written to the outside world, they are effectively placed back
into the global window. Transforms that write data and don't take this
perspective probably risks data loss.

A window has a maximum timestamp. When the watermark exceeds the maximum
timestamp plus the user-specified allowed lateness, the window is expired. All
data related to an expired window might be discarded at any time.

#### Coder

Every `PCollection` has a coder, which is a specification of the binary format
of the elements.

In Beam, the user's pipeline can be written in a language other than the
language of the runner. There is no expectation that the runner can actually
deserialize user data. The Beam model operates principally on encoded data,
"just bytes". Each `PCollection` has a declared encoding for its elements,
called a coder. A coder has a URN that identifies the encoding, and might have
additional sub-coders. For example, a coder for lists might contain a coder for
the elements of the list. Language-specific serialization techniques are
frequently used, but there are a few common key formats (such as key-value pairs
and timestamps) so the runner can understand them.

#### Windowing strategy

Every `PCollection` has a windowing strategy, which is a specification of
essential information for grouping and triggering operations. The `Window`
transform sets up the windowing strategy, and the `GroupByKey` transform has
behavior that is governed by the windowing strategy.

<br/>

For more information about PCollections, see the following page:

 * [Beam Programming Guide: PCollections](/documentation/programming-guide/#pcollections)

### User-Defined Functions (UDFs)

Beam has seven varieties of user-defined function (UDF). A Beam pipeline
may contain UDFs written in a language other than your runner, or even multiple
languages in the same pipeline (see the [Runner API](#the-runner-api)) so the
definitions are language-independent (see the [Fn API](#the-fn-api)).

The UDFs of Beam are:

 * _DoFn_ - per-element processing function (used in ParDo)
 * _WindowFn_ - places elements in windows and merges windows (used in Window
   and GroupByKey)
 * _Source_ - emits data read from external sources, including initial and
   dynamic splitting for parallelism (used in Read)
 * _ViewFn_ - adapts a materialized PCollection to a particular interface (used
   in side inputs)
 * _WindowMappingFn_ - maps one element's window to another, and specifies
   bounds on how far in the past the result window will be (used in side
   inputs)
 * _CombineFn_ - associative and commutative aggregation (used in Combine and
   state)
 * _Coder_ - encodes user data; some coders have standard formats and are not really UDFs

The various types of user-defined functions will be described further alongside
the [_PTransforms_](#ptransforms) that use them.

### Runner

The term "runner" is used for a couple of things. It generally refers to the
software that takes a Beam pipeline and executes it somehow. Often, this is the
translation code that you write. It usually also includes some customized
operators for your data processing engine, and is sometimes used to refer to
the full stack.

A runner has just a single method `run(Pipeline)`. From here on, I will often
use code font for proper nouns in our APIs, whether or not the identifiers
match across all SDKs.

The `run(Pipeline)` method should be asynchronous and results in a
PipelineResult which generally will be a job descriptor for your data
processing engine, providing methods for checking its status, canceling it, and
waiting for it to terminate.
