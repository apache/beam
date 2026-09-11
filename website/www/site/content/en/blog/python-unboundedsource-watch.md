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

The Apache Beam Python SDK now has an `UnboundedSource` API for writing custom
unbounded sources and a `Watch` transform for repeatedly polling an input that
keeps growing. I built both during my Google Summer of Code 2026 project with
Apache Beam, mentored by Yi Hu.

<!--more-->

## Motivation

The Java SDK has had both of these for years. A Python user who wanted to read
from a streaming system with no existing connector had to reach for a
cross-language Java connector or write an unbounded splittable DoFn directly,
wiring up a restriction tracker, a watermark estimator, and their own decision
about when to pause and resume. Polling an input that keeps growing had no
Python answer at all: there was no `Watch` transform, and
`fileio.MatchContinuously` kept one state entry per matched file path for the
life of the pipeline, so its state grew with every file the pattern had ever
seen.

This project brought both APIs to Python, gave duplicate suppression a way to
forget outputs it has moved past, and fixed the runner bugs that validating
them turned up.

## The UnboundedSource API

The first public Python
[`UnboundedSource` API](https://github.com/apache/beam/pull/38724) addresses a
[long-standing gap](https://github.com/apache/beam/issues/19137) between the Java
and Python SDKs. Source authors implement `UnboundedSource`, `UnboundedReader`,
and `CheckpointMark`, then read the source with `beam.io.Read(MySource())`.

The reader exposes methods such as `start()`, `advance()`, `get_current()`, and
`get_checkpoint_mark()`. Returning `False` from `advance()` means that no record
is available now; the reader can resume when more data arrives. The reader also
reports an event-time watermark through `get_watermark()`, which Beam uses to
track progress and determine when windows can close. A watermark of
`MAX_TIMESTAMP` signals that the source has permanently finished.

The SDK runs the reader through a splittable DoFn (SDF), Beam's mechanism for
managing work that can pause and resume. The wrapper saves the reader's
checkpoint with the unfinished work and reports its watermark to the runner.
This lets the same source implementation run on DirectRunner, Prism, Flink, and
Dataflow. Sources can split their work at pipeline startup; an active read is
not subdivided further.

The wrapper uses bundle finalization to invoke
`CheckpointMark.finalize_checkpoint` after the runner has durably committed
the output. A message-queue source can use this hook to acknowledge consumed
messages. Readers can also be reused across resumed bundles on the same worker,
with idle readers evicted from a bounded cache, reducing the need to reopen
connections.

The wrapper checks the record count and elapsed time between reads, yielding
when either limit is reached. This refinement came from mentor review:
correctly saving progress also requires giving the runner regular opportunities
to take over. The [Python I/O connector guide](https://beam.apache.org/documentation/io/developing-io-python/)
documents the API and its lifecycle.

## The Watch transform

The Python [`Watch` transform](https://github.com/apache/beam/pull/39023) ports
Java's polling transform. For each input element, it calls a user-supplied poll
function, emits newly discovered outputs, and saves progress between rounds.
Polling stops when the poll reports completion or a termination condition fires.
The API includes `PollFn`, `PollResult`, and the `never()` and `after_total_of()`
termination conditions.

A single SDF manages each input's polling, duplicate suppression, output,
waiting, and termination. For example, a poll can repeatedly list files under
a prefix while `Watch` remembers which results it has already emitted. Keeping
this lifecycle together also lets the transform save its deduplication state
with its progress.

An output's identity is the hash of its encoded key. The key defaults to the
output itself, and `output_key_fn` can select another identity. `Watch` requires
a deterministic key coder so equal keys produce the same fingerprint across
workers and after a restart. A coder with no deterministic form is rejected
when the pipeline is built.

The default deduplication mode retains a hash for every distinct output key,
so its history grows throughout a long-running watch. This also allows the
transform to recognize an item seen much earlier. The opt-in
[`timestamp_cursor` mode](https://github.com/apache/beam/pull/39090) addresses
this [state-growth problem](https://github.com/apache/beam/issues/18459) by
letting history expire as event time advances.

The cursor records the greatest emitted event time. Outputs more than
`allowed_lateness` behind it are skipped, including previously unseen ones,
and hashes older than that threshold can be discarded. This suits inputs
arriving in roughly non-decreasing event time. Increasing `allowed_lateness`
accommodates older arrivals while retaining more history. The cursor itself is
a single timestamp; the retained hashes depend on the keys within that time
range.

[Refactoring `MatchContinuously` onto `Watch`](https://github.com/apache/beam/pull/39461)
replaced its per-file state entries with the `Watch` restriction, so continuous
file matching can use cursor mode and stop accumulating an entry for every file
it has ever matched. The existing implementation remains for users who disable
duplicate suppression. The cursor design was also
[ported back to Java](https://github.com/apache/beam/pull/39746).

## Validation across runners

Both transforms were exercised on DirectRunner, Prism, Flink, and Dataflow.
Validation covered pause and resume behavior, acknowledgments, watermarks, and
polling. The `UnboundedSource` wrapper passed five end-to-end tests submitted
as Dataflow streaming jobs. For `MatchContinuously` on Flink, testing included
killing a worker during a run and restoring from a checkpoint. Prism tests
added files while a watch was running and checked that both deduplication modes
emitted them once and terminated on time.

These runs exposed issues beyond the SDK implementations:

- [Flink](https://github.com/apache/beam/pull/39191) accumulated state entries
  when an SDF saved unfinished work. Reusing a state entry addressed the growth.
- [Prism](https://github.com/apache/beam/pull/39572) could leave downstream
  records unprocessed when a source paused and resumed without advancing its
  watermark. Consumers with new data are now scheduled in that case.
- [Portable Spark batch](https://github.com/apache/beam/pull/39331) gained
  support for retaining and resuming unfinished SDF work.

The work also produced a [local Flink contributor guide](https://github.com/apache/beam/pull/39580),
documenting the cluster setup used to reproduce and investigate streaming
behavior.

## Benchmarks

The [local benchmarks](https://github.com/Eliaaazzz/gsoc-2026-beam#6-validation-and-benchmarks)
measured `UnboundedSource` throughput and checkpoint cadence, and `Watch`
deduplication overhead as the polled set grew.

For `UnboundedSource`, an in-memory source supplied one million records to
isolate the wrapper's overhead from external I/O. On Prism, a cap of 1,000
records per invocation produced 1,001 self-checkpoints and about 34,000 records
per second. Raising the cap to 100,000 reduced the self-checkpoint count to 11
and reached about 44,000 records per second. Throughput was measured from the
first record to the last, excluding runner startup.

The `Watch` benchmark repeatedly listed a set that gained 2,000 items per round
for 100 rounds. Each item retained its original event time. Both modes emitted
all 200,000 items once. Cursor mode reduced total time from 111 to 24 seconds
on DirectRunner and from 59 to 15 seconds on Prism. These single-machine
experiments show how checkpoint frequency and growing deduplication history
affect the transforms; distributed benchmarks remain future work.

## Remaining work

Both Python APIs remain experimental, and
[Spark streaming SDF support](https://github.com/apache/beam/issues/19468) is
still open. The [full project report](https://github.com/Eliaaazzz/gsoc-2026-beam)
includes the contribution list, documentation, validation details, and benchmark
methodology.

Thank you to my mentor, Yi Hu, and the Apache Beam community for their guidance
and reviews throughout Google Summer of Code 2026.
