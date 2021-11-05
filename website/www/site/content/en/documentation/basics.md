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

 * [_Pipeline_](#pipeline) - A pipeline is a graph of transformations that a user constructs
   that defines the data processing they want to do.
 * [_PCollection_](#pcollections) - Data being processed in a pipeline is part of a PCollection.
 * [_PTransforms_](#ptransforms) - The operations executed within a pipeline. These are best
   thought of as operations on PCollections.
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
