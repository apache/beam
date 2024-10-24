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
may require a more costly or complex implementation.

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

### Implementing the Impulse primitive

`Impulse` is a PTransform that takes no inputs and produces exactly one output
during the lifetime of the pipeline which should be the empty bytes in the
global window with the minimum timestamp.  This has the encoded value of
`7f df 3b 64 5a 1c ac 09 00 00 00 01 0f 00` when encoded with the standard
windowed value coder.

Though `Impulse` is generally not invoked by a user, it is the only root
primitive operation, and other root operations (like `Read`s and `Create`)
are composite operations constructed from an `Impulse` followed by a series
of (possibly Splittable) `ParDo`s.

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

Generally, rather than applying a series of `ParDo`s one at a time over the
entire input data set, it is more efficient to fuse several `ParDo`s together
in a single executable stage that consists of a whole series (in general,
a DAG) of mapping operations. In addition to `ParDo`s, windowing operations,
local (pre- or post-GBK) combining operations, and other mapping operations
may be fused into these stages as well.

As DoFns may execute code in a different language, or requiring a different
environment, than the runner itself, Beam provides the ability to call these
in a cross-process way. This is the crux of the
[Beam Fn API](https://beam.apache.org/contribute/runner-guide/#writing-an-sdk-independent-runner),
for which more detail can be found below.
It is, however, perfectly acceptable for a runner to invoke this user code
in process (for simplicity or efficiency) when the environments are
compatible.

#### Bundles

For correctness, a `DoFn` _should_ represent an element-wise function, but in
most SDKS this is a long-lived object that processes elements in small groups
called bundles.

Your runner decides how many elements, and which elements, to include in a
bundle, and can even decide dynamically in the middle of processing that the
current bundle has "ended". How a bundle is processed ties in with the rest of
a DoFn's lifecycle.

It will generally improve throughput to make the largest bundles possible, so
that initialization and finalization costs are amortized over many elements.
But if your data is arriving as a stream, then you will want to terminate a
bundle in order to achieve appropriate latency, so bundles may be just a few
elements.

A bundle is the unit of commitment in Beam. If an error is encountered while
processing a bundle, all the prior outputs of that bundle (including any
modifications to state or timers) must be discarded by the runner and the
entire bundle retried.  Upon successful completion of a bundle, its outputs,
together with any state/timer modifications and watermark updates, must be
committed atomically.

#### The DoFn Lifecycle

`DoFns` in many SDKS have several methods such as `setup`, `start_bundle`,
`finish_bundle`, `teardown`, etc. in addition to the standard,
element-wise `process` calls. Generally proper invocation of
[this lifecycle](https://beam.apache.org/documentation/programming-guide/#dofn)
should be handled for you when invoking one or more
`DoFn`s from the standard bundle processors (either via the FnAPI or directly
using a BundleProcessor
([java](https://github.com/apache/beam/blob/master/sdks/java/harness/src/main/java/org/apache/beam/fn/harness/control/ProcessBundleHandler.java)
([python](https://github.com/apache/beam/blob/release-2.49.0/sdks/python/apache_beam/runners/worker/bundle_processor.py#L852))).
SDK-independent runners should never have to worry about these details directly.

#### Side Inputs

_Main design document:
[https://s.apache.org/beam-side-inputs-1-pager](https://s.apache.org/beam-side-inputs-1-pager)_

A side input is a global view of a window of a `PCollection`. This distinguishes
it from the main input, which is processed one element at a time. The SDK/user
prepares a `PCollection` adequately, the runner materializes it, and then the
runner feeds it to the `DoFn`.

Unlike main input data, which is *pushed* by the runner to the `ParDo` (generally
via the FnApi Data channel), side input data is *pulled* by the `ParDo`
from the runner (generally over the FnAPI State channel).

A side input is accessed via a specific `access_pattern`.
There are currently two access patterns enumerated in the
`StandardSideInputTypes` proto: `beam:side_input:iterable:v1` which indicates
the runner must return all values in a PCollection corresponding to a specific
window and `beam:side_input:multimap:v1` which indicates the runner must return
all values corresponding to a specific key and window.
Being able to serve these access patterns efficiently may influence how a
runner materializes this PCollection.

SideInputs can be detected by looking at the `side_inputs` map in the
`ParDoPayload` of `ParDo` transforms.
The `ParDo` operation itself is responsible for invoking the
`window_mapping_fn` (before invoking the runner) and `view_fn` (on the
runner-returned values), so the runner need not concern itself with these
fields.

When a side input is needed but the side input has no data associated with it
for a given window, elements in that window must be deferred until the side
input has some data or the watermark has advances sufficiently such that
we can be sure there will be no data for that window. The
[`PushBackSideInputDoFnRunner`](https://github.com/apache/beam/blob/master/runners/core-java/src/main/java/org/apache/beam/runners/core/PushbackSideInputDoFnRunner.java)
is an example of implementing this.

#### State and Timers

_Main design document: [https://s.apache.org/beam-state](https://s.apache.org/beam-state)_

When a `ParDo` includes state and timers, its execution on your runner is usually
very different. In particular, the state must be persisted when the bundle
completes and retrieved for future bundles. Timers that are set must also be
injected into future bundles as the watermark advances sufficiently.

State and timers are partitioned per key and window, that is, a `DoFn`
processing a given key must have a consistent view of the state and timers
across all elements that share this key. You may need or want to
explicitly shuffle data to support this.
Once the watermark has passed the end of the window (plus an allowance for
allowed lateness, if any), state associated with this window can be dropped.

State setting and retrieval is performed on the FnAPI State channel, whereas
timer setting and firing happens on the FnAPI Data channel.

#### Splittable DoFn

_Main design document: [https://s.apache.org/splittable-do-fn](https://s.apache.org/splittable-do-fn)_

Splittable `DoFn` is a generalization of `ParDo` that is useful for high-fanout
mappings that can be done in parallel.  The prototypical example of such an
operation is reading from a file, where a single file name (as an input element)
can be mapped to all the elements contained in that file.
The `DoFn` is considered splittable in the sense that an element representing,
say, a single file can be split (e.g. into ranges of that file) to be processed
(e.g. read) by different workers.
The full power of this primitive is in the fact that these splits can happen
dynamically rather than just statically (i.e. ahead of time) avoiding the
problem of over- or undersplitting.

A full explanation of Splittable `DoFn` is out of scope for this doc, but
here is a brief overview as it pertains to its execution.

A Splittable `DoFn` can participate in the dynamic splitting protocol by
splitting within an element as well as between elements.  Dynamic splitting
is triggered by the runner issuing `ProcessBundleSplitRequest` messages on
the control channel.  The SDK will commit to process just a portion of the
indicated element and return a description of the remainder (i.e. the
unprocessed portion) to the runner in the `ProcessBundleSplitResponse`
to be scheduled by the runner (e.g. on a different worker or as part of a
different bundle).

A Splittable `DoFn` can also initiate its own spitting, indicating it has
processed an element as far as it can for the moment (e.g. when tailing a file)
but more remains.  These most often occur when reading unbounded sources.
In this case a set of elements representing the deferred work are passed back
in the `residual_roots` field of the `ProcessBundleResponse`.
At a future time, the runner must re-invoke these same operations with
the elements given in `residual_roots`.

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
the keys. For this reason, the format of key-value pairs is
[standardized and shared](https://github.com/apache/beam/blob/release-2.49.0/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto#L838)
across all SDKS. See either
[`KvCoder`](https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/coders/KvCoder.html)
in Java or
[`TupleCoder`](https://beam.apache.org/releases/pydoc/current/apache_beam.coders.coders.html#apache_beam.coders.coders.TupleCoder)
in Python for documentation on the binary format.

#### Window Merging

As well as grouping by key, your runner must group elements by their window. A
`WindowFn` has the option of declaring that it merges windows on a per-key
basis.  For example, session windows for the same key will be merged if they
overlap. So your runner must invoke the merge method of the `WindowFn` during
grouping.

#### Implementing via GroupByKeyOnly + GroupAlsoByWindow

The Java and Python codebases includes support code for a particularly common way of
implementing the full `GroupByKey` operation: first group the keys, and then group
by window. For merging windows, this is essentially required, since merging is
per key.

Often presenting the set of values in timestamp order can allow more
efficient grouping of these values into their final windows.

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
In Python this is supported by the
[TriggerDriver](https://github.com/apache/beam/blob/release-2.49.0/sdks/python/apache_beam/transforms/trigger.py#L1199) classes.

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

Most runners implement this by fusing these window-altering mappings in with
the `DoFns`.

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

We provide coders with these optimizations such as
[`PARAM_WINDOWED_VALUE`](https://github.com/apache/beam/blob/release-2.49.0/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto#L968)
that can be used to reduce the size of serialized data.

In the future, this primitive may be retired as it can be implemented as a
ParDo if the capabilities of ParDo are enhanced to allow output to new windows.

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
`CombinePerKey`, which applies an associative and commutative operator to
the elements of a `PCollection`. This composite is not a primitive. It is
implemented in terms of `ParDo` and `GroupByKey`, so your runner will work
without treating it - but it does carry additional information that you
probably want to use for optimizations: the associative-commutative operator,
known as a `CombineFn`.

Generally runners will want to implement this via what is called
combiner lifting, where a new operation is placed before the `GroupByKey`
that does partial (within-bundle) combining, which often requires a slight
modification of what comes after the `GroupByKey` as well.
An example of this transformation can be found in the
[Python](https://github.com/apache/beam/blob/release-2.49.0/sdks/python/apache_beam/runners/portability/fn_api_runner/translations.py#L1193)
or [go](https://github.com/apache/beam/blob/release-2.49.0/sdks/go/pkg/beam/runners/prism/internal/handlecombine.go#L67)
implementations of this optimization.
The resulting pre- and post-`GroupByKey` operations are generally fused in with
the `ParDo`s and executed as above.

## Working with pipelines

When you receive a pipeline from a user, you will need to translate it.
An explanation of how Beam pipelines are represented can be found
[here](https://docs.google.com/presentation/d/1atu-QC_mnK2SaeLhc0D78wZYgVOX1fN0H544QmBi3VA)
which compliment the [official proto declarations](https://github.com/apache/beam/blob/master/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto).

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

Python Runners can also be identified (e.g. when passing the runner parameter)
by their fully qualified name whether or not they live in the Beam repository.

## Writing an SDK-independent runner

There are two aspects to making your runner SDK-independent, able to run
pipelines written in other languages: The Fn API and the Runner API.

### The Fn API

_Design documents:_

 - _[https://s.apache.org/beam-fn-api](https://s.apache.org/beam-fn-api)_
 - _[https://s.apache.org/beam-fn-api-processing-a-bundle](https://s.apache.org/beam-fn-api-processing-a-bundle)_
 - _[https://s.apache.org/beam-fn-api-send-and-receive-data](https://s.apache.org/beam-fn-api-send-and-receive-data)_

 - _[Overview](https://docs.google.com/presentation/d/1Cso0XP9dmj77OD9Bd53C1M3W1sPJF0ZnA20gzb2BPhE/edit#slide=id.g42e4c9aad6_0_317)_
 - _[Spec](https://github.com/apache/beam/blob/master/model/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1/beam_fn_api.proto)_

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

The [Runner API](https://docs.google.com/presentation/d/1Cso0XP9dmj77OD9Bd53C1M3W1sPJF0ZnA20gzb2BPhE/edit#slide=id.g42e4c9aad6_1_3736)
is an SDK-independent schema for a pipeline along with RPC
interfaces for launching a pipeline and checking the status of a job.
By examining a pipeline only through Runner API
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
buffers schema, covered below for reference. But your runner _need not_
directly manipulate protobuf messages.  Instead, the Beam codebase provides
utilities for working with pipelines so that you don't need to be aware of
whether or not the pipeline has ever been serialized or transmitted, or what
language it may have been written in to begin with.

**Java**

If your runner is Java-based, the tools to interact with pipelines in an
SDK-agnostic manner are in the `beam-runners-core-construction-java`
artifact, in the `org.apache.beam.sdk.util.construction` namespace.
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
API](https://github.com/apache/beam/blob/master/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto)
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
  bytes payload;
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

It goes without saying that not every environment will be able to deserialize
every function spec. For this reason `PTransform`s have an `environment_id`
parameter that indicates at least one environment that is capable of interpreting
the contained URNs. This is a reference to an environment in the environments
map of the Pipeline proto and is typically defined by a docker image (possibly
with some extra dependencies).
There may be other environments that are also capable of
doing so, and a runner is free to use them if it has this knowledge.

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
  FunctionSpec do_fn;
  map<string, SideInput> side_inputs;
  map<string, StateSpec> state_specs;
  map<string, TimerSpec> timer_specs;
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
  FunctionSpec combine_fn;
  string accumulator_coder_id;
  ...
}
{{< /highlight >}}

### `PTransform` proto

A `PTransform` is a function from `PCollection` to `PCollection`. This is
represented in the proto using a FunctionSpec.

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

A runner that understands the specification of a given `PTransform` (whether
primitive or composite), as defined by its `FunctionSpec`, is free to
substitute it with another `PTransform` (or set thereof) that has identical
semantics.
This is typically how `CombinePerKey` is handled, but many other substitutions
can be done as well.

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
only be understood by a particular SDK, hence an `FunctionSpec`, but also
may have component coders that fully define it. For example, a `ListCoder` is
only a meta-format, while `ListCoder(VarIntCoder)` is a fully specified format.

{{< highlight class="no-toggle" >}}
message Coder {
  FunctionSpec spec;
  repeated string component_coder_ids;
}
{{< /highlight >}}

There are a large number of
[standard coders](https://github.com/apache/beam/blob/release-2.49.0/model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1/beam_runner_api.proto#L829)
understood by most, if not all,
SDKs.  Using these allows for cross-language transforms.

## The Jobs API RPCs

[Overview](https://docs.google.com/presentation/d/1Cso0XP9dmj77OD9Bd53C1M3W1sPJF0ZnA20gzb2BPhE/edit#slide=id.g42e4c9aad6_1_3722)
[Spec](https://github.com/apache/beam/blob/master/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_job_api.proto)

While your language's SDK will may insulate you from touching the Runner
API protos directly, you may need to implement adapters for your runner, to
expose it to another language.
This allows a Python SDK to invoke a Java runner or vice versa.
A typical implementation of this can be found in
[local_job_service.py](https://github.com/apache/beam/blob/release-2.48.0/sdks/python/apache_beam/runners/portability/local_job_service.py)
which is used directly to front several Python-implemented runners.

The RPCs themselves will necessarily follow the existing APIs of PipelineRunner
and PipelineResult, but altered to be the minimal backend channel, versus a
rich and convenient API.

A key piece of this is the
[Artifacts API](https://github.com/apache/beam/blob/master/model/job-management/src/main/proto/org/apache/beam/model/job_management/v1/beam_artifact_api.proto),
which allows a Runner to fetch and deploy binary artifacts (such as jars,
pypi packages, etc.) that are listed as dependencies in the various environments,
and may have various representations.  This is invoked after a pipeline
is submitted, but before it is executed. The SDK submitting a pipeline acts
as an artifact server to the runner receiving the request, and in turn the
runner then acts as an artifact server to the workers (environments) hosting
the users UDFs.

