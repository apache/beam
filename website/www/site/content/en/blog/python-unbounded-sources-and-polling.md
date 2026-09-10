---
title: "Unbounded Sources and Continuous Polling in the Beam Python SDK"
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

A streaming pipeline may read messages from a queue or discover files that arrive
over time. These sources need to preserve progress, report event-time watermarks,
and wait for more data. Two new APIs in the Beam Python SDK support these patterns:
`UnboundedSource` for reader-based sources and `Watch` for periodic polling.

<!--more-->

## Reading from a resumable source

Use [`UnboundedSource`](https://github.com/apache/beam/pull/38724) when a source
exposes a reader and a position from which it can resume. A source author
implements `UnboundedSource`, `UnboundedReader`, and `CheckpointMark`, then reads
the source with `beam.io.Read(MySource())`.

The SDK wraps the reader in a splittable DoFn (SDF), allowing runners to pause and
resume reads. The reader supplies timestamps, watermarks, and checkpoint marks;
the wrapper handles the SDF lifecycle. After the runner commits output, bundle
finalization can invoke the checkpoint's acknowledgement hook. Each read
invocation is also limited so a busy source periodically yields to the runner.

## Polling for new files

Use [`Watch`](https://github.com/apache/beam/pull/39023) when a source can be
queried periodically. A poll function returns timestamped outputs, and `Watch`
handles duplicate suppression, scheduling, and termination.

[`MatchContinuously` now uses `Watch`](https://github.com/apache/beam/pull/39461)
for deduplicated file discovery. This local pipeline checks a directory every
five seconds and prints newly discovered file paths:

{{< highlight py >}}
import os

import apache_beam as beam
from apache_beam.io import fileio

with beam.Pipeline() as pipeline:
    (
        pipeline
        | fileio.MatchContinuously(os.path.join("incoming", "*.json"), interval=5)
        | beam.Map(lambda metadata: print(metadata.path))
    )
{{< /highlight >}}

The pipeline keeps polling until cancelled. By default, discovery deduplicates
by path, so later changes to an existing file do not emit it again.

## Limiting deduplication history

Repeated polls can return the same files. Remembering every output key prevents
duplicates, but that history grows as new files arrive. In `Watch`, setting
[`timestamp_cursor=True`](https://github.com/apache/beam/pull/39090) lets history
expire as event time advances. Outputs more than `allowed_lateness` behind the
greatest emitted event time are also skipped, including previously unseen ones.

This suits sources with sufficiently ordered timestamps. Retained state still
depends on how many keys fall within that time range. For `MatchContinuously`,
cursor mode uses file modification times and tracks updates to existing paths;
files discovered with older modification times can be skipped. The cursor
design was also [ported to Java](https://github.com/apache/beam/pull/39746).

## What runner validation revealed

Validation on DirectRunner, Prism, Flink, and Dataflow exposed issues in how
runners resume work. In Prism, a source could repeatedly emit records and pause
without advancing its watermark. Downstream scheduling depended on watermark
advancement, leaving records waiting while the source continued running. The
[fix](https://github.com/apache/beam/pull/39572) schedules consumers with new data
even when the source watermark stays unchanged, while retaining the readiness
checks needed for side inputs and aggregations.

Testing also led to fixes for
[Flink checkpoint state growth](https://github.com/apache/beam/pull/39191) and
[Prism resume delays](https://github.com/apache/beam/pull/39849), alongside
[SDF self-checkpointing support in portable Spark batch](https://github.com/apache/beam/pull/39331).
[Spark streaming SDF support](https://github.com/apache/beam/issues/19468) remains
open. Both Python APIs are experimental.

This work was developed during Google Summer of Code 2026 with guidance from
Yi Hu and the Apache Beam community. The
[full project report](https://github.com/Eliaaazzz/gsoc-2026-beam) includes the
contributions, validation details, and local benchmarks.
