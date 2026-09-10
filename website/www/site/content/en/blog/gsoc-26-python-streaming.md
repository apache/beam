---
title: "Google Summer of Code 2026: Native Streaming Transforms for the Python SDK"
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

During Google Summer of Code 2026, I added two streaming APIs to the Apache Beam
Python SDK: `UnboundedSource` for custom streaming sources and `Watch` for polling
growing datasets. The project also improved continuous file matching and addressed
runner issues found while testing the new transforms.

<!--more-->

## Custom streaming sources

The new [`UnboundedSource` API](https://github.com/apache/beam/pull/38724) provides
a reader interface for sources such as message queues and change feeds. A source
author implements `UnboundedSource`, `UnboundedReader`, and `CheckpointMark`, then
reads the source with `beam.io.Read(MySource())`.

The SDK wraps the reader in a splittable DoFn (SDF), which lets runners pause and
resume reads while preserving progress and reporting event-time watermarks.
Bundle finalization lets a source acknowledge records after the runner commits
their output. The wrapper also limits each read invocation so a busy source
periodically yields work to the runner.

## Watching growing datasets

The [`Watch` transform](https://github.com/apache/beam/pull/39023) repeatedly calls
a poll function, emits new outputs, and stops when polling completes or a
termination condition is met. This supports use cases such as discovering files
as they arrive.

A key design question was how much history to retain for duplicate suppression.
By default, `Watch` stores a hash for every distinct output key. That history can
grow throughout a long-running pipeline. The opt-in
[`timestamp_cursor` mode](https://github.com/apache/beam/pull/39090) limits the
retained history by event time. Outputs more than `allowed_lateness` behind the
greatest emitted event time are treated as already seen and dropped. This mode
suits sources whose outputs arrive in roughly non-decreasing event time.

[`MatchContinuously` now uses `Watch`](https://github.com/apache/beam/pull/39461),
making the same option available for continuous file matching. The cursor design
was also [ported to Java](https://github.com/apache/beam/pull/39746).

## Lessons from runner validation

I validated the transforms on DirectRunner, Prism, Flink, and Dataflow. Testing
empty polls, checkpoint recovery, and long-running pipelines exposed runner
behavior that short unit tests could miss:

- Flink accumulated state entries each time an SDF saved unfinished work.
  [Reusing a state entry](https://github.com/apache/beam/pull/39191) addressed the
  growth.
- Prism needed to [schedule downstream consumers](https://github.com/apache/beam/pull/39572)
  when a source paused without emitting data, and to
  [honor requested resume delays](https://github.com/apache/beam/pull/39849).
- Portable Spark batch needed to
  [retain and resume unfinished SDF work](https://github.com/apache/beam/pull/39331).
  Streaming SDF support remains [open](https://github.com/apache/beam/issues/19468).

These tests made runner validation part of the API design process. Correct
polling depends on how runners preserve progress, advance watermarks, and
schedule work after a pause.

## Next steps

Both Python APIs remain experimental. Follow-up work includes Spark streaming
SDF support and distributed benchmarks. The
[final report](https://github.com/Eliaaazzz/gsoc-2026-beam) includes the complete
contribution list, validation details, and local benchmark results.

Thank you to my mentor, Yi Hu, and the Apache Beam community for their guidance
and reviews throughout the project. I look forward to continuing this work.
