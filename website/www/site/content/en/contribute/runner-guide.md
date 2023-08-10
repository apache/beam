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

