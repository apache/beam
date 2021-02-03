---
title: "Runner Authoring Guide"
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

# Runner Authoring Guide

This guide walks through how to implement a new runner. It is aimed at someone
who has a data processing system and wants to use it to execute a Beam
pipeline. The guide starts from the basics, to help you evaluate the work
ahead. Then the sections become more and more detailed, to be a resource
throughout the development of your runner.

Topics covered:

{{< toc >}}

## Basics of the Beam model

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
your runner either via the SDK directly or via the Runner API's (forthcoming)
RPC interfaces.

### PTransforms

In Beam, a PTransform can be one of the five primitives or it can be a
composite transform encapsulating a subgraph. The primitives are:

 * [_Read_](#implementing-the-read-primitive) - parallel connectors to external
   systems
 * [_ParDo_](#implementing-the-pardo-primitive) - per element processing
 * [_GroupByKey_](#implementing-the-groupbykey-and-window-primitive) -
   aggregating elements per key and window
 * [_Flatten_](#implementing-the-flatten-primitive) - union of PCollections
 * [_Window_](#implementing-the-window-primitive) - set the windowing strategy
   for a PCollection

When implementing a runner, these are the operations you need to implement.
Composite transforms may or may not be important to your runner. If you expose
a UI, maintaining some of the composite structure will make the pipeline easier
for a user to understand. But the result of processing is not changed.

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
the primitives that use them.

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

## Implementing the Beam Primitives

Aside from encoding and persisting data - which presumably your engine already
does in some way or another - most of what you need to do is implement the Beam
primitives. This section provides a detailed look at each primitive, covering
what you need to know that might not be obvious and what support code is
provided.

The primitives are designed for the benefit of pipeline authors, not runner
authors. Each represents a different conceptual mode of operation (external IO,
element-wise, grouping, windowing, union) rather than a specific implementation
decision.  The same primitive may require a very different implementation based
on how the user instantiates it. For example, a `ParDo` that uses state or
timers may require key partitioning, a `GroupByKey` with speculative triggering
may require a more costly or complex implementation, and `Read` is completely
different for bounded and unbounded data.

### What if you haven't implemented some of these features?

That's OK! You don't have to do it all at once, and there may even be features
that don't make sense for your runner to ever support.  We maintain a
[capability matrix](/documentation/runners/capability-matrix/) on the Beam site so you can tell
users what you support. When you receive a `Pipeline`, you should traverse it
and determine whether or not you can execute each `DoFn` that you find. If
you cannot execute some `DoFn` in the pipeline (or if there is any other
requirement that your runner lacks) you should reject the pipeline. In your
native environment, this may look like throwing an
`UnsupportedOperationException`.  The Runner API RPCs will make this explicit,
for cross-language portability.

### Implementing the ParDo primitive

The `ParDo` primitive describes element-wise transformation for a
`PCollection`.  `ParDo` is the most complex primitive, because it is where any
per-element processing is described. In addition to very simple operations like
standard `map` or `flatMap` from functional programming, `ParDo` also supports
multiple outputs, side inputs, initialization, flushing, teardown, and stateful
processing.

The UDF that is applied to each element is called a `DoFn`. The exact APIs for
a `DoFn` can vary per language/SDK but generally follow the same pattern, so we
can discuss it with pseudocode. I will also often refer to the Java support
code, since I know it and most of our current and future runners are
Java-based.

#### Bundles

For correctness, a `DoFn` _should_ represent an element-wise function, but in
fact is a long-lived object that processes elements in small groups called
bundles.

Your runner decides how many elements, and which elements, to include in a
bundle, and can even decide dynamically in the middle of processing that the
current bundle has "ended". How a bundle is processed ties in with the rest of
a DoFn's lifecycle.

It will generally improve throughput to make the largest bundles possible, so
that initialization and finalization costs are amortized over many elements.
But if your data is arriving as a stream, then you will want to terminate a
bundle in order to achieve appropriate latency, so bundles may be just a few
elements.

#### The DoFn Lifecycle

While each language's SDK is free to make different decisions, the Python and
Java SDKs share an API with the following stages of a DoFn's lifecycle.

However, if you choose to execute a DoFn directly to improve performance or
single-language simplicity, then your runner is responsible for implementing
the following sequence:

 * _Setup_ - called once per DoFn instance before anything else; this has not been
   implemented in the Python SDK so the user can work around just with lazy
   initialization
 * _StartBundle_ - called once per bundle as initialization (actually, lazy
   initialization is almost always equivalent and more efficient, but this hook
   remains for simplicity for users)
 * _ProcessElement_ / _OnTimer_ - called for each element and timer activation
 * _FinishBundle_ - essentially "flush"; required to be called before
   considering elements as actually processed
 * _Teardown_ - release resources that were used across bundles; calling this
   can be best effort due to failures

#### DoFnRunner(s)

This is a support class that has manifestations in both the Java codebase and
the Python codebase.

**Java**

In Java, the `beam-runners-core-java` library provides an interface
`DoFnRunner` for bundle processing, with implementations for many situations.

{{< highlight class="language-java no-toggle" >}}
interface DoFnRunner<InputT, OutputT> {
  void startBundle();
  void processElement(WindowedValue<InputT> elem);
  void onTimer(String timerId, BoundedWindow window, Instant timestamp, TimeDomain timeDomain);
  void finishBundle();
}
{{< /highlight >}}

There are some implementations and variations of this for different scenarios:

 * [`SimpleDoFnRunner`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/SimpleDoFnRunner.java) -
   not actually simple at all; implements lots of the core functionality of
   `ParDo`. This is how most runners execute most `DoFns`.
 * [`LateDataDroppingDoFnRunner`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/LateDataDroppingDoFnRunner.java) -
   wraps a `DoFnRunner` and drops data from expired windows so the wrapped
   `DoFnRunner` doesn't get any unpleasant surprises
 * [`StatefulDoFnRunner`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/StatefulDoFnRunner.java) -
   handles collecting expired state
 * [`PushBackSideInputDoFnRunner`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/PushbackSideInputDoFnRunner.java) -
   buffers input while waiting for side inputs to be ready

These are all used heavily in implementations of Java runners. Invocations
via the [Fn API](#the-fn-api) may manifest as another implementation of
`DoFnRunner` even though it will be doing far more than running a `DoFn`.

**Python**

See the [DoFnRunner pydoc](https://beam.apache.org/releases/pydoc/2.0.0/apache_beam.runners.html#apache_beam.runners.common.DoFnRunner).

#### Side Inputs

_Main design document:
[https://s.apache.org/beam-side-inputs-1-pager](https://s.apache.org/beam-side-inputs-1-pager)_

A side input is a global view of a window of a `PCollection`. This distinguishes
it from the main input, which is processed one element at a time. The SDK/user
prepares a `PCollection` adequately, the runner materializes it, and then the
runner feeds it to the `DoFn`.

What you will need to implement is to inspect the materialization requested for
the side input, and prepare it appropriately, and corresponding interactions
when a `DoFn` reads the side inputs.

The details and available support code vary by language.

**Java**

If you are using one of the above `DoFnRunner` classes, then the interface for
letting them request side inputs is
[`SideInputReader`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/SideInputReader.java).
It is a simple mapping from side input and window to a value. The `DoFnRunner`
will perform a mapping with the
[`WindowMappingFn`](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/windowing/WindowMappingFn.java)
to request the appropriate window so you do not worry about invoking this UDF.
When using the Fn API, it will be the SDK harness that maps windows as well.

A simple, but not necessarily optimal approach to building a
[`SideInputReader`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/SideInputReader.java)
is to use a state backend. In our Java support code, this is called
[`StateInternals`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/StateInternals.java)
and you can build a
[`SideInputHandler`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/SideInputHandler.java)
that will use your `StateInternals` to materialize a `PCollection` into the
appropriate side input view and then yield the value when requested for a
particular side input and window.

When a side input is needed but the side input has no data associated with it
for a given window, elements in that window must be deferred until the side
input has some data. The aforementioned
[`PushBackSideInputDoFnRunner`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/PushbackSideInputDoFnRunner.java)
is used to implement this.

**Python**

In Python, [`SideInputMap`](https://beam.apache.org/releases/pydoc/2.0.0/apache_beam.transforms.html#apache_beam.transforms.sideinputs.SideInputMap) maps
windows to side input values. The `WindowMappingFn` manifests as a simple
function. See
[sideinputs.py](https://github.com/apache/beam/blob/master/sdks/python/apache_beam/transforms/sideinputs.py).

#### State and Timers

_Main design document: [https://s.apache.org/beam-state](https://s.apache.org/beam-state)_

When a `ParDo` includes state and timers, its execution on your runner is usually
very different. See the full details beyond those covered here.

State and timers are partitioned per key and window. You may need or want to
explicitly shuffle data to support this.

**Java**

We provide
[`StatefulDoFnRunner`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/StatefulDoFnRunner.java)
to help with state cleanup. The non-user-facing interface
[`StateInternals`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/StateInternals.java)
is what a runner generally implements, and then the Beam support code can use
this to implement user-facing state.

#### Splittable DoFn

_Main design document: [https://s.apache.org/splittable-do-fn](https://s.apache.org/splittable-do-fn)_

Splittable `DoFn` is a generalization and combination of `ParDo` and `Read`. It
is per-element processing where each element has the capability of being "split"
in the same ways as a `BoundedSource` or `UnboundedSource`. This enables better
performance for use cases such as a `PCollection` of names of large files where
you want to read each of them. Previously they would have to be static data in
the pipeline or be read in a non-splittable manner.

This feature is still under development, but likely to become the new primitive
for reading. It is best to be aware of it and follow developments.

### Implementing the GroupByKey (and window) primitive

The `GroupByKey` operation (sometimes called GBK for short) groups a
`PCollection` of key-value pairs by key and window, emitting results according
to the `PCollection`'s triggering configuration.

It is quite a bit more elaborate than simply colocating elements with the same
key, and uses many fields from the `PCollection`'s windowing strategy.

#### Group By Encoded Bytes

For both the key and window, your runner sees them as "just bytes". So you need
to group in a way that is consistent with grouping by those bytes, even if you
have some special knowledge of the types involved.

The elements you are processing will be key-value pairs, and you'll need to extract
the keys. For this reason, the format of key-value pairs is standardized and
shared across all SDKS. See either
[`KvCoder`](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/coders/KvCoder.html)
in Java or
[`TupleCoder`](https://beam.apache.org/releases/pydoc/2.0.0/apache_beam.coders.html#apache_beam.coders.coders.TupleCoder.key_coder)
in Python for documentation on the binary format.

#### Window Merging

As well as grouping by key, your runner must group elements by their window. A
`WindowFn` has the option of declaring that it merges windows on a per-key
basis.  For example, session windows for the same key will be merged if they
overlap. So your runner must invoke the merge method of the `WindowFn` during
grouping.

#### Implementing via GroupByKeyOnly + GroupAlsoByWindow

The Java codebase includes support code for a particularly common way of
implementing the full `GroupByKey` operation: first group the keys, and then group
by window. For merging windows, this is essentially required, since merging is
per key.

#### Dropping late data

_Main design document:
[https://s.apache.org/beam-lateness](https://s.apache.org/beam-lateness)_

A window is expired in a `PCollection`  if the watermark of the input PCollection
has exceeded the end of the window by at least the input `PCollection`'s
allowed lateness.

Data for an expired window can be dropped any time and should be dropped at a
`GroupByKey`. If you are using `GroupAlsoByWindow`, then just before executing
this transform. You may shuffle less data if you drop data prior to
`GroupByKeyOnly`, but should only safely be done for non-merging windows, as a
window that appears expired may merge to become not expired.

#### Triggering

_Main design document:
[https://s.apache.org/beam-triggers](https://s.apache.org/beam-triggers)_

The input `PCollection`'s trigger and accumulation mode specify when and how
outputs should be emitted from the `GroupByKey` operation.

In Java, there is a lot of support code for executing triggers in the
`GroupAlsoByWindow` implementations, `ReduceFnRunner` (legacy name), and
`TriggerStateMachine`, which is an obvious way of implementing all triggers as
an event-driven machine over elements and timers.

#### TimestampCombiner

When an aggregated output is produced from multiple inputs, the `GroupByKey`
operation has to choose a timestamp for the combination. To do so, first the
WindowFn has a chance to shift timestamps - this is needed to ensure watermarks
do not prevent progress of windows like sliding windows (the details are beyond
this doc). Then, the shifted timestamps need to be combined - this is specified
by a `TimestampCombiner`, which can either select the minimum or maximum of its
inputs, or just ignore inputs and choose the end of the window.

### Implementing the Window primitive

The window primitive applies a `WindowFn` UDF to place each input element into
one or more windows of its output PCollection. Note that the primitive also
generally configures other aspects of the windowing strategy for a `PCollection`,
but the fully constructed graph that your runner receives will already have a
complete windowing strategy for each `PCollection`.

To implement this primitive, you need to invoke the provided WindowFn on each
element, which will return some set of windows for that element to be a part of
in the output `PCollection`.

**Implementation considerations**

A "window" is just a second grouping key that has a "maximum timestamp". It can
be any arbitrary user-defined type. The `WindowFn` provides the coder for the
window type.

Beam's support code provides `WindowedValue` which is a compressed
representation of an element in multiple windows. You may want to do use this,
or your own compressed representation. Remember that it simply represents
multiple elements at the same time; there is no such thing as an element "in
multiple windows".

For values in the global window, you may want to use an even further compressed
representation that doesn't bother including the window at all.

In the future, this primitive may be retired as it can be implemented as a
ParDo if the capabilities of ParDo are enhanced to allow output to new windows.

### Implementing the Read primitive

You implement this primitive to read data from an external system. The APIs are
carefully crafted to enable efficient parallel execution. Reading from an
`UnboundedSource` is a bit different than reading from a `BoundedSource`.

#### Reading from an UnboundedSource

An `UnboundedSource` is a source of potentially infinite data; you can think of
it like a stream. The capabilities are:

 * `split(int)` - your runner should call this to get the desired parallelism
 * `createReader(...)` - call this to start reading elements; it is an enhanced iterator that also provides:
 * watermark (for this source) which you should propagate downstream
 * timestamps, which you should associate with elements read
 * record identifiers, so you can dedup downstream if needed
 * progress indication of its backlog
 * checkpointing
 * `requiresDeduping` - this indicates that there is some chance that the source
   may emit duplicates; your runner should do its best to dedupe based on the
   identifier attached to emitted records

An unbounded source has a custom type of checkpoints and an associated coder for serializing them.

#### Reading from a BoundedSource

A `BoundedSource` is a source of data that you know is finite, such as a static
collection of log files, or a database table. The capabilities are:

 * `split(int)` - your runner should call this to get desired initial parallelism (but you can often steal work later)
 * `getEstimatedSizeBytes(...)` - self explanatory
 * `createReader(...)` - call this to start reading elements; it is an enhanced iterator that also provides:
 * timestamps to associate with each element read
 * `splitAtFraction` for dynamic splitting to enable work stealing, and other
   methods to support it - see the [Beam blog post on dynamic work
   rebalancing](/blog/2016/05/18/splitAtFraction-method.html)

The `BoundedSource` does not report a watermark currently. Most of the time, reading
from a bounded source can be parallelized in ways that result in utterly out-of-order
data, so a watermark is not terribly useful.
Thus the watermark for the output `PCollection` from a bounded read should
remain at the minimum timestamp throughout reading (otherwise data might get
dropped) and advance to the maximum timestamp when all data is exhausted.

### Implementing the Flatten primitive

This one is easy - take as input a finite set of `PCollections` and outputs their
bag union, keeping windows intact.

For this operation to make sense, it is the SDK's responsibility to make sure
the windowing strategies are compatible.

Also note that there is no requirement that the coders for all the `PCollections`
be the same. If your runner wants to require that (to avoid tedious
re-encoding) you have to enforce it yourself. Or you could just implement the
fast path as an optimization.

### Special mention: the Combine composite

A composite transform that is almost always treated specially by a runner is
`Combine` (per key), which applies an associative and commutative operator to
the elements of a `PCollection`. This composite is not a primitive. It is
implemented in terms of `ParDo` and `GroupByKey`, so your runner will work
without treating it - but it does carry additional information that you
probably want to use for optimizations: the associative-commutative operator,
known as a `CombineFn`.

## Working with pipelines

When you receive a pipeline from a user, you will need to translate it. This is
a tour of the APIs that you'll use to do it.

### Traversing a pipeline

Something you will likely do is to traverse a pipeline, probably to translate
it into primitives for your engine. The general pattern is to write a visitor
that builds a job specification as it walks the graph of `PTransforms`.

The entry point for this in Java is
[`Pipeline.traverseTopologically`](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/Pipeline.html#traverseTopologically-org.apache.beam.sdk.Pipeline.PipelineVisitor-)
and
[`Pipeline.visit`](https://beam.apache.org/releases/pydoc/2.0.0/apache_beam.html#apache_beam.pipeline.Pipeline.visit)
in Python. See the generated documentation for details.

### Altering a pipeline

Often, the best way to keep your
translator simple will be to alter the pipeline prior to translation. Some
alterations you might perform:

 * Elaboration of a Beam primitive into a composite transform that uses
   multiple runner-specific primitives
 * Optimization of a Beam composite into a specialized primitive for your
   runner
 * Replacement of a Beam composite with a different expansion more suitable for
   your runner

The Java SDK and the "runners core construction" library (the artifact is
`beam-runners-core-construction-java` and the namespaces is
`org.apache.beam.runners.core.construction`) contain helper code for this sort
of work. In Python, support code is still under development.

All pipeline alteration is done via
[`Pipeline.replaceAll(PTransformOverride)`](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/Pipeline.html#replaceAll-java.util.List-)
method. A
[`PTransformOverride`](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/runners/PTransformOverride.java)
is a pair of a
[`PTransformMatcher`](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/runners/PTransformMatcher.java)
to select transforms for replacement and a
[`PTransformOverrideFactory`](https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/runners/PTransformOverrideFactory.java)
to produce the replacement. All `PTransformMatchers` that have been needed by
runners to date are provided. Examples include: matching a specific class,
matching a `ParDo` where the `DoFn` uses state or timers, etc.

## Testing your runner

The Beam Java SDK and Python SDK have suites of runner validation tests. The
configuration may evolve faster than this document, so check the configuration
of other Beam runners. But be aware that we have tests and you can use them
very easily!  To enable these tests in a Java-based runner using Gradle, you
scan the dependencies of the SDK for tests with the JUnit category
`ValidatesRunner`.

{{< highlight class="no-toggle" >}}
task validatesRunner(type: Test) {
  group = "Verification"
  description = "Validates the runner"
  def pipelineOptions = JsonOutput.toJson(["--runner=MyRunner", ... misc test options ...])
  systemProperty "beamTestPipelineOptions", pipelineOptions
  classpath = configurations.validatesRunner
  testClassesDirs = files(project(":sdks:java:core").sourceSets.test.output.classesDirs)
  useJUnit {
    includeCategories 'org.apache.beam.sdk.testing.ValidatesRunner'
  }
}
{{< /highlight >}}

Enabling these tests in other languages is unexplored.

## Integrating your runner nicely with SDKs

Whether or not your runner is based in the same language as an SDK (such as
Java), you will want to provide a shim to invoke it from another SDK if you
want the users of that SDK (such as Python) to use it.

### Integrating with the Java SDK

#### Allowing users to pass options to your runner

The mechanism for configuration is
[`PipelineOptions`](https://beam.apache.org/releases/javadoc/2.0.0/org/apache/beam/sdk/options/PipelineOptions.html),
an interface that works completely differently than normal Java objects. Forget
what you know, and follow the rules, and `PipelineOptions` will treat you well.

You must implement a sub-interface for your runner with getters and setters
with matching names, like so:

{{< highlight class="language-java no-toggle" >}}
public interface MyRunnerOptions extends PipelineOptions {
  @Description("The Foo to use with MyRunner")
  @Required
  public Foo getMyRequiredFoo();
  public void setMyRequiredFoo(Foo newValue);

  @Description("Enable Baz; on by default")
  @Default.Boolean(true)
  public Boolean isBazEnabled();
  public void setBazEnabled(Boolean newValue);
}
{{< /highlight >}}

You can set up defaults, etc. See the javadoc for details.  When your runner is
instantiated with a `PipelineOptions` object, you access your interface by
`options.as(MyRunnerOptions.class)`.

To make these options available on the command line, you register your options
with a `PipelineOptionsRegistrar`. It is easy if you use `@AutoService`:

{{< highlight class="language-java no-toggle" >}}
@AutoService(PipelineOptionsRegistrar.class)
public static class MyOptionsRegistrar implements PipelineOptionsRegistrar {
  @Override
  public Iterable<Class<? extends PipelineOptions>> getPipelineOptions() {
    return ImmutableList.<Class<? extends PipelineOptions>>of(MyRunnerOptions.class);
  }
}
{{< /highlight >}}

#### Registering your runner with SDKs for command line use

To make your runner available on the command line, you register your options
with a `PipelineRunnerRegistrar`. It is easy if you use `@AutoService`:

{{< highlight class="language-java no-toggle" >}}
@AutoService(PipelineRunnerRegistrar.class)
public static class MyRunnerRegistrar implements PipelineRunnerRegistrar {
  @Override
  public Iterable<Class<? extends PipelineRunner>> getPipelineRunners() {
    return ImmutableList.<Class<? extends PipelineRunner>>of(MyRunner.class);
  }
}
{{< /highlight >}}

### Integrating with the Python SDK

In the Python SDK the registration of the code is not automatic. So there are
few things to keep in mind when creating a new runner.

Any dependencies on packages for the new runner should be options so create a
new target in `extra_requires` in `setup.py` that is needed for the new runner.

All runner code should go in it's own package in `apache_beam/runners` directory.

Register the new runner in the `create_runner` function of `runner.py` so that the
partial name is matched with the correct class to be used.

## Writing an SDK-independent runner

There are two aspects to making your runner SDK-independent, able to run
pipelines written in other languages: The Fn API and the Runner API.

### The Fn API

_Design documents:_

 - _[https://s.apache.org/beam-fn-api](https://s.apache.org/beam-fn-api)_
 - _[https://s.apache.org/beam-fn-api-processing-a-bundle](https://s.apache.org/beam-fn-api-processing-a-bundle)_
 - _[https://s.apache.org/beam-fn-api-send-and-receive-data](https://s.apache.org/beam-fn-api-send-and-receive-data)_

To run a user's pipeline, you need to be able to invoke their UDFs.  The Fn API
is an RPC interface for the standard UDFs of Beam, implemented using protocol
buffers over gRPC.

The Fn API includes:

 - APIs for registering a subgraph of UDFs
 - APIs for streaming elements of a bundle
 - Shared data formats (key-value pairs, timestamps, iterables, etc)

You are fully welcome to _also_ use the SDK for your language for utility code,
or provide optimized implementations of bundle processing for same-language
UDFs.

### The Runner API

The Runner API is an SDK-independent schema for a pipeline along with RPC
interfaces for launching a pipeline and checking the status of a job. The RPC
interfaces are still in development so for now we focus on the SDK-agnostic
representation of a pipeline. By examining a pipeline only through Runner API
interfaces, you remove your runner's dependence on the SDK for its language for
pipeline analysis and job translation.

To execute such an SDK-independent pipeline, you will need to support the Fn
API. UDFs are embedded in the pipeline as a specification of the function
(often just opaque serialized bytes for a particular language) plus a
specification of an environment that can execute it (essentially a particular
SDK). So far, this specification is expected to be a URI for a Docker container
hosting the SDK's Fn API harness.

You are fully welcome to _also_ use the SDK for your language, which may offer
useful utility code.

The language-independent definition of a pipeline is described via a protocol
buffers schema, covered below for reference. But your runner _should not_
directly manipulate protobuf messages.  Instead, the Beam codebase provides
utilities for working with pipelines so that you don't need to be aware of
whether or not the pipeline has ever been serialized or transmitted, or what
language it may have been written in to begin with.

**Java**

If your runner is Java-based, the tools to interact with pipelines in an
SDK-agnostic manner are in the `beam-runners-core-construction-java`
artifact, in the `org.apache.beam.runners.core.construction` namespace.
The utilities are named consistently, like so:

 * `PTransformTranslation` - registry of known transforms and standard URNs
 * `ParDoTranslation` - utilities for working with `ParDo` in a
   language-independent manner
 * `WindowIntoTranslation` - same for `Window`
 * `FlattenTranslation` - same for `Flatten`
 * `WindowingStrategyTranslation` - same for windowing strategies
 * `CoderTranslation` - same for coders
 * ... etc, etc ...

By inspecting transforms only through these classes, your runner will not
depend on the particulars of the Java SDK.

## The Runner API protos

The [Runner
API](https://github.com/apache/beam/blob/master/model/pipeline/src/main/proto/beam_runner_api.proto)
refers to a specific manifestation of the concepts in the Beam model, as a
protocol buffers schema.  Even though you should not manipulate these messages
directly, it can be helpful to know the canonical data that makes up a
pipeline.

Most of the API is exactly the same as the high-level description; you can get
started implementing a runner without understanding all the low-level details.

The most important takeaway of the Runner API for you is that it is a
language-independent definition of a Beam pipeline. You will probably always
interact via a particular SDK's support code wrapping these definitions with
sensible idiomatic APIs, but always be aware that this is the specification and
any other data is not necessarily inherent to the pipeline, but may be
SDK-specific enrichments (or bugs!).

The UDFs in the pipeline may be written for any Beam SDK, or even multiple in
the same pipeline. So this is where we will start, taking a bottom-up approach
to understanding the protocol buffers definitions for UDFs before going back to
the higher-level, mostly obvious, record definitions.

### `FunctionSpec` proto

The heart of cross-language portability is the `FunctionSpec`. This is a
language-independent specification of a function, in the usual programming
sense that includes side effects, etc.

{{< highlight class="no-toggle" >}}
message FunctionSpec {
  string urn;
  google.protobuf.Any parameter;
}
{{< /highlight >}}

A `FunctionSpec` includes a URN identifying the function as well as an arbitrary
fixed parameter. For example the (hypothetical) "max" CombineFn might have the
URN `beam:combinefn:max:0.1` and a parameter that indicates by what
comparison to take the max.

For most UDFs in a pipeline constructed using a particular language's SDK, the
URN will indicate that the SDK must interpret it, for example
`beam:dofn:javasdk:0.1` or `beam:dofn:pythonsdk:0.1`. The parameter
will contain serialized code, such as a Java-serialized `DoFn` or a Python
pickled `DoFn`.

A `FunctionSpec` is not only for UDFs. It is just a generic way to name/specify
any function. It is also used as the specification for a `PTransform`. But when
used in a `PTransform` it describes a function from `PCollection` to `PCollection`
and cannot be specific to an SDK because the runner is in charge of evaluating
transforms and producing `PCollections`.

### `SdkFunctionSpec` proto

When a `FunctionSpec` represents a UDF, in general only the SDK that serialized
it will be guaranteed to understand it. So in that case, it will always come
with an environment that can understand and execute the function. This is
represented by the `SdkFunctionSpec`.

{{< highlight class="no-toggle" >}}
message SdkFunctionSpec {
  FunctionSpec spec;
  bytes environment_id;
}
{{< /highlight >}}

In the Runner API, many objects are stored by reference. Here in the
`environment_id` is a pointer, local to the pipeline and just made up by the
SDK that serialized it, that can be dereferenced to yield the actual
environment proto.

Thus far, an environment is expected to be a Docker container specification for
an SDK harness that can execute the specified UDF.

### Primitive transform payload protos

The payload for the primitive transforms are just proto serializations of their
specifications. Rather than reproduce their full code here, I will just
highlight the important pieces to show how they fit together.

It is worth emphasizing again that while you probably will not interact
directly with these payloads, they are the only data that is inherently part of
the transform.

#### `ParDoPayload` proto

A `ParDo` transform carries its `DoFn` in an `SdkFunctionSpec` and then
provides language-independent specifications for its other features - side
inputs, state declarations, timer declarations, etc.

{{< highlight class="no-toggle" >}}
message ParDoPayload {
  SdkFunctionSpec do_fn;
  map<string, SideInput> side_inputs;
  map<string, StateSpec> state_specs;
  map<string, TimerSpec> timer_specs;
  ...
}
{{< /highlight >}}

#### `ReadPayload` proto

A `Read` transform carries an `SdkFunctionSpec` for its `Source` UDF.

{{< highlight class="no-toggle" >}}
message ReadPayload {
  SdkFunctionSpec source;
  ...
}
{{< /highlight >}}

#### `WindowIntoPayload` proto

A `Window` transform carries an `SdkFunctionSpec` for its `WindowFn` UDF. It is
part of the Fn API that the runner passes this UDF along and tells the SDK
harness to use it to assign windows (as opposed to merging).

{{< highlight class="no-toggle" >}}
message WindowIntoPayload {
  SdkFunctionSpec window_fn;
  ...
}
{{< /highlight >}}

#### `CombinePayload` proto

`Combine` is not a primitive. But non-primitives are perfectly able to carry
additional information for better optimization. The most important thing that a
`Combine` transform carries is the `CombineFn` in an `SdkFunctionSpec` record.
In order to effectively carry out the optimizations desired, it is also
necessary to know the coder for intermediate accumulations, so it also carries
a reference to this coder.

{{< highlight class="no-toggle" >}}
message CombinePayload {
  SdkFunctionSpec combine_fn;
  string accumulator_coder_id;
  ...
}
{{< /highlight >}}

### `PTransform` proto

A `PTransform` is a function from `PCollection` to `PCollection`. This is
represented in the proto using a FunctionSpec. Note that this is not an
`SdkFunctionSpec`, since it is the runner that observes these. They will never
be passed back to an SDK harness; they do not represent a UDF.

{{< highlight class="no-toggle" >}}
message PTransform {
  FunctionSpec spec;
  repeated string subtransforms;

  // Maps from local string names to PCollection ids
  map<string, bytes> inputs;
  map<string, bytes> outputs;
  ...
}
{{< /highlight >}}

A `PTransform` may have subtransforms if it is a composite, in which case the
`FunctionSpec` may be omitted since the subtransforms define its behavior.

The input and output `PCollections` are unordered and referred to by a local
name. The SDK decides what this name is, since it will likely be embedded in
serialized UDFs.

### `PCollection` proto

A `PCollection` just stores a coder, windowing strategy, and whether or not it
is bounded.

{{< highlight class="no-toggle" >}}
message PCollection {
  string coder_id;
  IsBounded is_bounded;
  string windowing_strategy_id;
  ...
}
{{< /highlight >}}

### `Coder` proto

This is a very interesting proto. A coder is a parameterized function that may
only be understood by a particular SDK, hence an `SdkFunctionSpec`, but also
may have component coders that fully define it. For example, a `ListCoder` is
only a meta-format, while `ListCoder(VarIntCoder)` is a fully specified format.

{{< highlight class="no-toggle" >}}
message Coder {
  SdkFunctionSpec spec;
  repeated string component_coder_ids;
}
{{< /highlight >}}

## The Runner API RPCs

While your language's SDK will probably insulate you from touching the Runner
API protos directly, you may need to implement adapters for your runner, to
expose it to another language. So this section covers proto that you will
possibly interact with quite directly.

The specific manner in which the existing runner method calls will be expressed
as RPCs is not implemented as proto yet. This RPC layer is to enable, for
example, building a pipeline using the Python SDK and launching it on a runner
that is written in Java. It is expected that a small Python shim will
communicate with a Java process or service hosting the Runner API.

The RPCs themselves will necessarily follow the existing APIs of PipelineRunner
and PipelineResult, but altered to be the minimal backend channel, versus a
rich and convenient API.

### `PipelineRunner.run(Pipeline)` RPC

This will take the same form, but `PipelineOptions` will have to be serialized
to JSON (or a proto `Struct`) and passed along.

{{< highlight class="no-toggle" >}}
message RunPipelineRequest {
  Pipeline pipeline;
  Struct pipeline_options;
}
{{< /highlight >}}

{{< highlight class="no-toggle" >}}
message RunPipelineResponse {
  bytes pipeline_id;

  // TODO: protocol for rejecting pipelines that cannot be executed
  // by this runner. May just be REJECTED job state with error message.

  // totally opaque to the SDK; for the shim to interpret
  Any contents;
}
{{< /highlight >}}

### `PipelineResult` aka "Job API"

The two core pieces of functionality in this API today are getting the state of
a job and canceling the job. It is very much likely to evolve, for example to
be generalized to support draining a job (stop reading input and let watermarks
go to infinity). Today, verifying our test framework benefits (but does not
depend upon wholly) querying metrics over this channel.

{{< highlight class="no-toggle" >}}
message CancelPipelineRequest {
  bytes pipeline_id;
  ...
}

message GetStateRequest {
  bytes pipeline_id;
  ...
}

message GetStateResponse {
  JobState state;
  ...
}

enum JobState {
  ...
}
{{< /highlight >}}
