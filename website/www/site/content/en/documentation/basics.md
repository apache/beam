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

Apache Beam is a unified model for defining both batch and streaming
data-parallel processing pipelines. To get started with Beam, you'll need to
understand an important set of core concepts:

 * [_Pipeline_](#pipeline) - A pipeline is a user-constructed graph of
   transformations that defines the desired data processing operations.
 * [_PCollection_](#pcollection) - A `PCollection` is a data set or data
   stream. The data that a pipeline processes is part of a PCollection.
 * [_PTransform_](#ptransform) - A `PTransform` (or _transform_) represents a
   data processing operation, or a step, in your pipeline. A transform is
   applied to zero or more `PCollection` objects, and produces zero or more
   `PCollection` objects.
 * [_Aggregation_](#aggregation) - Aggregation is computing a value from
   multiple (1 or more) input elements.
 * [_User-defined function (UDF)_](#user-defined-function-udf) - Some Beam
   operations allow you to run user-defined code as a way to configure the
   transform.
 * [_Schema_](#schema) - A schema is a language-independent type definition for
   a `PCollection`. The schema for a `PCollection` defines elements of that
   `PCollection` as an ordered list of named fields.
 * [_SDK_](/documentation/sdks/java/) - A language-specific library that lets
   pipeline authors build transforms, construct their pipelines, and submit
   them to a runner.
 * [_Runner_](#runner) - A runner runs a Beam pipeline using the capabilities of
   your chosen data processing engine.
 * [_Splittable DoFn_](#splittable-dofn) - Splittable DoFns let you process
   elements in a non-monolithic way. You can checkpoint the processing of an
   element, and the runner can split the remaining work to yield additional
   parallelism.

The following sections cover these concepts in more detail and provide links to
additional documentation.

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

### Aggregation

Aggregation is computing a value from multiple (1 or more) input elements. In
Beam, the primary computational pattern for aggregation is to group all elements
with a common key and window then combine each group of elements using an
associative and commutative operation. This is similar to the "Reduce" operation
in the [MapReduce](https://en.wikipedia.org/wiki/MapReduce) model, though it is
enhanced to work with unbounded input streams as well as bounded data sets.

<img src="/images/aggregation.png" alt="Aggregation of elements." width="120px">

*Figure 1: Aggregation of elements. Elements with the same color represent those
with a common key and window.*

Some simple aggregation transforms include `Count` (computes the count of all
elements in the aggregation), `Max` (computes the maximum element in the
aggregation), and `Sum` (computes the sum of all elements in the aggregation).

When elements are grouped and emitted as a bag, the aggregation is known as
`GroupByKey` (the associative/commutative operation is bag union). In this case,
the output is no smaller than the input. Often, you will apply an operation such
as summation, called a `CombineFn`, in which the output is significantly smaller
than the input. In this case the aggregation is called `CombinePerKey`.

In a real application, you might have millions of keys and/or windows; that is
why this is still an "embarassingly parallel" computational pattern. In those
cases where you have fewer keys, you can add parallelism by adding a
supplementary key, splitting each of your problem's natural keys into many
sub-keys. After these sub-keys are aggregated, the results can be further
combined into a result for the original natural key for your problem. The
associativity of your aggregation function ensures that this yields the same
answer, but with more parallelism.

When your input is unbounded, the computational pattern of grouping elements by
key and window is roughly the same, but governing when and how to emit the
results of aggregation involves three concepts:

 * Windowing, which partitions your input into bounded subsets that can be
   complete.
 * Watermarks, which estimate the completeness of your input.
 * Triggers, which govern when and how to emit aggregated results.

For more information about available aggregation transforms, see the following
pages:

 * [Beam Programming Guide: Core Beam transforms](/documentation/programming-guide/#core-beam-transforms)
 * Beam Transform catalog
   ([Java](/documentation/transforms/java/overview/#aggregation),
   [Python](/documentation/transforms/python/overview/#aggregation))

### User-defined function (UDF)

Some Beam operations allow you to run user-defined code as a way to configure
the transform. For example, when using `ParDo`, user-defined code specifies what
operation to apply to every element. For `Combine`, it specifies how values
should be combined. By using [cross-language transforms](/documentation/patterns/cross-language/),
a Beam pipeline can contain UDFs written in a different language, or even
multiple languages in the same pipeline.

Beam has several varieties of UDFs:

 * [_DoFn_](/programming-guide/#pardo) - per-element processing function (used
   in `ParDo`)
 * [_WindowFn_](/programming-guide/#setting-your-pcollections-windowing-function) -
   places elements in windows and merges windows (used in `Window` and
   `GroupByKey`)
 * [_ViewFn_](/documentation/programming-guide/#side-inputs) - adapts a
   materialized `PCollection` to a particular interface (used in side inputs)
 * [_WindowMappingFn_](/documentation/programming-guide/#side-inputs-windowing) -
   maps one element's window to another, and specifies bounds on how far in the
   past the result window will be (used in side inputs)
 * [_CombineFn_](/documentation/programming-guide/#combine) - associative and
   commutative aggregation (used in `Combine` and state)
 * [_Coder_](/documentation/programming-guide/#data-encoding-and-type-safety) -
   encodes user data; some coders have standard formats and are not really UDFs

Each language SDK has its own idiomatic way of expressing the user-defined
functions in Beam, but there are common requirements. When you build user code
for a Beam transform, you should keep in mind the distributed nature of
execution. For example, there might be many copies of your function running on a
lot of different machines in parallel, and those copies function independently,
without communicating or sharing state with any of the other copies. Each copy
of your user code function might be retried or run multiple times, depending on
the pipeline runner and the processing backend that you choose for your
pipeline. Beam also supports stateful processing through the
[stateful processing API](/blog/stateful-processing/).

For more information about user-defined functions, see the following pages:

 * [Requirements for writing user code for Beam transforms](/documentation/programming-guide/#requirements-for-writing-user-code-for-beam-transforms)
 * [Beam Programming Guide: ParDo](/documentation/programming-guide/#pardo)
 * [Beam Programming Guide: WindowFn](/programming-guide/#setting-your-pcollections-windowing-function)
 * [Beam Programming Guide: CombineFn](/documentation/programming-guide/#combine)
 * [Beam Programming Guide: Coder](/documentation/programming-guide/#data-encoding-and-type-safety)
 * [Beam Programming Guide: Side inputs](/documentation/programming-guide/#side-inputs)

### Schema

A schema is a language-independent type definition for a `PCollection`. The
schema for a `PCollection` defines elements of that `PCollection` as an ordered
list of named fields. Each field has a name, a type, and possibly a set of user
options.

In many cases, the element type in a `PCollection` has a structure that can be
introspected. Some examples are JSON, Protocol Buffer, Avro, and database row
objects. All of these formats can be converted to Beam Schemas. Even within a
SDK pipeline, Simple Java POJOs (or equivalent structures in other languages)
are often used as intermediate types, and these also have a clear structure that
can be inferred by inspecting the class. By understanding the structure of a
pipeline’s records, we can provide much more concise APIs for data processing.

Beam provides a collection of transforms that operate natively on schemas.  For
example, [Beam SQL](/documentation/dsls/sql/overview/) is a common transform
that operates on schemas. These transforms allow selections and aggregations in
terms of named schema fields. Another advantage of schemas is that they allow
referencing of element fields by name. Beam provides a selection syntax for
referencing fields, including nested and repeated fields.

For more information about schemas, see the following pages:

 * [Beam Programming Guide: Schemas](/documentation/programming-guide/#schemas)
 * [Schema Patterns](/documentation/patterns/schema/)

### Runner

A Beam runner runs a Beam pipeline on a specific platform. Most runners are
translators or adapters to massively parallel big data processing systems, such
as Apache Flink, Apache Spark, Google Cloud Dataflow, and more. For example, the
Flink runner translates a Beam pipeline into a Flink job. The Direct Runner runs
pipelines locally so you can test, debug, and validate that your pipeline
adheres to the Apache Beam model as closely as possible.

For an up-to-date list of Beam runners and which features of the Apache Beam
model they support, see the runner
[capability matrix](/documentation/runners/capability-matrix/).

For more information about runners, see the following pages:

 * [Choosing a Runner](/documentation/#choosing-a-runner)
 * [Beam Capability Matrix](/documentation/runners/capability-matrix/)

### Splittable DoFn

Splittable `DoFn` (SDF) is a generalization of `DoFn` that lets you process
elements in a non-monolithic way. Splittable `DoFn` makes it easier to create
complex, modular I/O connectors in Beam.

A regular `ParDo` processes an entire element at a time, applying your regular
`DoFn` and waiting for the call to terminate. When you instead apply a
splittable `DoFn` to each element, the runner has the option of splitting the
element's processing into smaller tasks. You can checkpoint the processing of an
element, and you can split the remaining work to yield additional parallelism.

For example, imagine you want to read every line from very large text files.
When you write your splittable `DoFn`, you can have separate pieces of logic to
read a segment of a file, split a segment of a file into sub-segments, and
report progress through the current segment. The runner can then invoke your
splittable `DoFn` intelligently to split up each input and read portions
separately, in parallel.

A common computation pattern has the following steps:

 1. The runner splits an incoming element before starting any processing.
 2. The runner starts running your processing logic on each sub-element.
 3. If the runner notices that some sub-elements are taking longer than others,
    the runner splits those sub-elements further and repeats step 2.
 4. The sub-element either finishes processing, or the user chooses to
    checkpoint the sub-element and the runner repeats step 2.

You can also write your splittable `DoFn` so the runner can split the unbounded
processing. For example, if you write a splittable `DoFn` to watch a set of
directories and output filenames as they arrive, you can split to subdivide the
work of different directories. This allows the runner to split off a hot
directory and give it additional resources.

For more information about Splittable `DoFn`, see the following pages:

 * [Splittable DoFns](/documentation/programming-guide/#splittable-dofns)
 * [Splittable DoFn in Apache Beam is Ready to Use](/blog/splittable-do-fn-is-available/)

