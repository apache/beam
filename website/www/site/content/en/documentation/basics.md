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

 * [_Pipeline_](#pipeline) - A pipeline is a graph of transformations that a user constructs
   that defines the data processing they want to do.
 * [_PCollection_](#pcollections) - Data being processed in a pipeline is part of a PCollection.
 * [_PTransforms_](#ptransforms) - The operations executed within a pipeline. These are best
   thought of as operations on PCollections.
 * [_Aggregation_](#aggregation) - Aggregation is computing a value from
   multiple (1 or more) input elements.
 * [_User-defined function (UDF)_](#user-defined-function-udf) - Some Beam
   operations allow you to run user-defined code as a way to configure the
   transform.
 * [_Schema_](#schema) - A schema is a language-independent type definition for
   a `PCollection`. The schema for a `PCollection` defines elements of that
   `PCollection` as an ordered list of named fields.
 * [_SDK_](/documentation/sdks/java/) - A language-specific library for pipeline
   authors.
   to build transforms, construct their pipelines, and submit them to a runner.
 * [_Runner_](#runner) - A runner runs a Beam pipeline using the capabilities of
   your chosen data processing engine.

The following sections cover these concepts in more detail and provide links to
additional documentation.

### Pipeline

A pipeline in Beam is a graph of PTransforms operating on PCollections. A
pipeline is constructed by a user in their SDK of choice, and makes its way to
your runner either via the SDK directly or via the Runner API's
RPC interfaces.

### PTransforms

A `PTransform` represents a data processing operation, or a step,
in your pipeline. A `PTransform` can be applied to one or more
`PCollection` objects as input which performs some processing on the elements of that
`PCollection` and produces zero or more output `PCollection` objects.

### PCollections

A PCollection is an unordered bag of elements. Your runner will be responsible
for storing these elements.  There are some major aspects of a PCollection to
note:

#### Bounded vs Unbounded

A PCollection may be bounded or unbounded.

 - _Bounded_ - it is finite and you know it, as in batch use cases
 - _Unbounded_ - it may be never end, you don't know, as in streaming use cases

These derive from the intuitions of batch and stream processing, but the two
are unified in Beam and bounded and unbounded PCollections can coexist in the
same pipeline. If your runner can only support bounded PCollections, you'll
need to reject pipelines that contain unbounded PCollections. If your
runner is only really targeting streams, there are adapters in our support code
to convert everything to APIs targeting unbounded data.

#### Timestamps

Every element in a PCollection has a timestamp associated with it.

When you execute a primitive connector to some storage system, that connector
is responsible for providing initial timestamps.  Your runner will need to
propagate and aggregate timestamps. If the timestamp is not important, as with
certain batch processing jobs where elements do not denote events, they will be
the minimum representable timestamp, often referred to colloquially as
"negative infinity".

#### Watermarks

Every PCollection has to have a watermark that estimates how complete the
PCollection is.

The watermark is a guess that "we'll never see an element with an earlier
timestamp". Sources of data are responsible for producing a watermark. Your
runner needs to implement watermark propagation as PCollections are processed,
merged, and partitioned.

The contents of a PCollection are complete when a watermark advances to
"infinity". In this manner, you may discover that an unbounded PCollection is
finite.

#### Windowed elements

Every element in a PCollection resides in a window. No element resides in
multiple windows (two elements can be equal except for their window, but they
are not the same).

When elements are read from the outside world they arrive in the global window.
When they are written to the outside world, they are effectively placed back
into the global window (any writing transform that doesn't take this
perspective probably risks data loss).

A window has a maximum timestamp, and when the watermark exceeds this plus
user-specified allowed lateness the window is expired. All data related
to an expired window may be discarded at any time.

#### Coder

Every PCollection has a coder, a specification of the binary format of the elements.

In Beam, the user's pipeline may be written in a language other than the
language of the runner. There is no expectation that the runner can actually
deserialize user data. So the Beam model operates principally on encoded data -
"just bytes". Each PCollection has a declared encoding for its elements, called
a coder. A coder has a URN that identifies the encoding, and may have
additional sub-coders (for example, a coder for lists may contain a coder for
the elements of the list). Language-specific serialization techniques can, and
frequently are used, but there are a few key formats - such as key-value pairs
and timestamps - that are common so your runner can understand them.

#### Windowing Strategy

Every PCollection has a windowing strategy, a specification of essential
information for grouping and triggering operations.

The details will be discussed below when we discuss the
[Window](#implementing-the-window-primitive) primitive, which sets up the
windowing strategy, and
[GroupByKey](#implementing-the-groupbykey-and-window-primitive) primitive,
which has behavior governed by the windowing strategy.

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

The associativity and commutativity of a `CombineFn` allows runners to
automatically apply some optimizations:

 * _Combiner lifting_: This is the most significant optimization. Input elements
   are combined per key and window before they are shuffled, so the volume of
   data shuffled might be reduced by many orders of magnitude. Another term for
   this optimization is "mapper-side combine."
 * _Incremental combining_: When you have a `CombineFn` that reduces the data
   size by a lot, it is useful to combine elements as they emerge from a
   streaming shuffle. This spreads out the cost of doing combines over the time
   that your streaming computation might be idle. Incremental combining also
   reduces the storage of intermediate accumulators.

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

Beam has seven varieties of UDFs:

 * [_DoFn_](/programming-guide/#pardo) - per-element processing function (used
   in `ParDo`)
 * [_WindowFn_](/programming-guide/#setting-your-pcollections-windowing-function) -
   places elements in windows and merges windows (used in `Window` and
   `GroupByKey`)
 * [_Source_](/documentation/programming-guide/#pipeline-io) - emits data read
   from external sources, including initial and dynamic splitting for
   parallelism
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
 * [Beam Programming Guide: Pipeline I/O](/documentation/programming-guide/#pipeline-io)

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
