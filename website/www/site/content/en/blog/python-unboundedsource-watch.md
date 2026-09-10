---
title: "UnboundedSource and the Watch Transform in the Apache Beam Python SDK"
date: 2026-09-10T00:00:00+10:00
categories:
  - blog
  - gsoc
authors:
  - eliaaazzz
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

The Apache Beam Python SDK now includes an `UnboundedSource` API for custom
unbounded sources and a `Watch` transform for repeatedly polling growing inputs.
This project brought both APIs to Python, improved `Watch` deduplication, and
addressed runner issues found while validating the new transforms.

<!--more-->

## The UnboundedSource API

The first public Python
[`UnboundedSource` API](https://github.com/apache/beam/pull/38724) addresses a
[long-standing gap](https://github.com/apache/beam/issues/19137) between the Java
and Python SDKs. Source authors implement `UnboundedSource`, `UnboundedReader`,
and `CheckpointMark`, then read the source with `beam.io.Read(MySource())`.

The SDK runs the reader through a splittable DoFn (SDF), which allows a read to
pause and resume while preserving its progress. The wrapper handles
checkpointing and event-time watermarks, and uses bundle finalization to invoke
`CheckpointMark.finalize_checkpoint` after the runner has durably committed the
output. A source can use this hook to acknowledge consumed messages.

Each invocation is limited by record count and elapsed time so a busy source
periodically yields to the runner. This was an important design refinement from
mentor review.

## The Watch transform

The Python [`Watch` transform](https://github.com/apache/beam/pull/39023) ports
Java's polling transform. For each input element, it calls a user-supplied poll
function, emits newly discovered outputs, and saves progress between rounds.
Polling stops when the poll reports completion or a termination condition fires.
The API includes `PollFn`, `PollResult`, and the `never()` and `after_total_of()`
termination conditions.

Deduplication was a central design challenge. The default mode retains a hash
for every distinct output key, so its history grows throughout a long-running
watch. The opt-in
[`timestamp_cursor` mode](https://github.com/apache/beam/pull/39090) lets history
expire as event time advances. Outputs more than `allowed_lateness` behind the
greatest emitted event time are also skipped, including previously unseen ones.
This suits inputs arriving in roughly non-decreasing event time; retained state
depends on the keys within that time range.

[Refactoring `MatchContinuously` onto `Watch`](https://github.com/apache/beam/pull/39461)
made cursor mode available for continuous file matching. The same design was
also [ported back to Java](https://github.com/apache/beam/pull/39746).

## Validation across runners

Both transforms were exercised on DirectRunner, Prism, Flink, and Dataflow.
Long-running pipelines, checkpoint recovery, and polling exposed issues beyond
the SDK implementations:

- [Flink](https://github.com/apache/beam/pull/39191) accumulated state entries
  when an SDF saved unfinished work. Reusing a state entry addressed the growth.
- [Prism](https://github.com/apache/beam/pull/39572) could leave downstream
  records unprocessed when a source paused and resumed without advancing its
  watermark. Consumers with new data are now scheduled in that case.
- [Portable Spark batch](https://github.com/apache/beam/pull/39331) gained
  support for retaining and resuming unfinished SDF work.

## Benchmarks and remaining work

The [local benchmarks](https://github.com/Eliaaazzz/gsoc-2026-beam#6-validation-and-benchmarks)
measured `UnboundedSource` throughput and checkpoint cadence, and `Watch`
deduplication overhead as the polled set grew. On Prism, `UnboundedSource`
processed about 34,000 to 44,000 records per second across checkpoint settings,
excluding startup. In the 200,000-output `Watch` benchmark, cursor mode reduced
total time from 111 to 24 seconds on DirectRunner and from 59 to 15 seconds on
Prism. These were single-machine experiments; distributed benchmarks remain
future work.

Both Python APIs remain experimental, and
[Spark streaming SDF support](https://github.com/apache/beam/issues/19468) is
still open. The [full project report](https://github.com/Eliaaazzz/gsoc-2026-beam)
includes the contribution list, documentation, validation details, and benchmark
methodology.

Thank you to my mentor, Yi Hu, and the Apache Beam community for their guidance
and reviews throughout Google Summer of Code 2026.
