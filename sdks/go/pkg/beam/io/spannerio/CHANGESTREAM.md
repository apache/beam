# Spanner Change Streams — Go Implementation Notes

This document describes the design of the `ReadChangeStream` connector, the correctness and
scalability guarantees it provides, and what an implementor needs to know when extending or
operating it.

---

## Architecture overview

`ReadChangeStream` is implemented as a **Splittable DoFn (SDF)** with an in-memory partition
queue encoded inside the restriction. There is no Spanner metadata table and no external
coordination service. The Beam runner's native checkpoint/restart mechanism provides all
durability.

```
beam.Impulse ──► readChangeStreamFn (SDF)
                      │
                      │  Restriction = PartitionQueueRestriction
                      │  {Pending: []PartitionWork, Bounded: bool}
                      │
                      ▼
               PCollection<DataChangeRecord>
```

### Partition lifecycle

Spanner change stream partitions form a tree. The root query (empty partition token) returns
`ChildPartitionsRecord` rows that name the initial leaf partitions. Each leaf may itself
return more `ChildPartitionsRecord` rows as Spanner internally reshards.

```
Root query (token="")
    └── ChildPartitionsRecord → [token-A, token-B, ...]
            ├── token-A  →  DataChangeRecords ...  →  PartitionEndRecord
            └── token-B  →  DataChangeRecords ...  →  ChildPartitionsRecord → [token-C]
                                                            └── token-C  →  ...
```

The SDF models this as a work queue (`PartitionQueueRestriction.Pending`). The root entry
(empty token) is enqueued at startup. When a `ChildPartitionsRecord` is received, the child
tokens are appended to the queue via `TryClaim`. When a natural end is reached, the active
partition is dequeued.

---

## Scalability: Aggressive TrySplit

A naive queue-based design would process all partitions sequentially on a single worker.
The Go implementation avoids this through **aggressive `TrySplit`**.

When the runner calls `TrySplit(fraction > 0)`:

- **Primary** restriction keeps only the currently active partition (`Pending[0]`).
- **Residual** restriction gets all remaining partitions (`Pending[1:]`).

The runner recursively splits the residual, eventually producing one restriction per partition.
Each restriction is dispatched to a separate worker, achieving **per-partition parallelism**
with no external coordination.

```
Initial:   [root, A, B, C, D]
Split 1:   Primary=[root]   Residual=[A, B, C, D]
Split 2:   Primary=[A]      Residual=[B, C, D]
Split 3:   Primary=[B]      Residual=[C, D]
...
```

> **Note:** The initial partition set is not known until the root query completes.
> Until the first `ChildPartitionsRecord` arrives, there is only one restriction on one worker.
> Splitting begins as soon as the first child tokens are enqueued (typically within one
> `defaultCheckpointInterval` = 10 seconds of startup).

For `TrySplit(fraction == 0)` (self-checkpoint), the primary becomes empty (done) and
the residual continues from the current restriction. This is how the SDF periodically yields
to allow checkpoint serialisation.

---

## Durability: At-least-once delivery

On every self-checkpoint (every 10 seconds by default), the Beam runner serialises the
`PartitionQueueRestriction` to durable storage. The restriction encodes:

- The current partition token and its last processed `StartTimestamp`.
- All queued child partitions and their start timestamps.

After a worker failure or restart, the runner deserialises the last committed restriction and
resumes `ProcessElement` from exactly the last claimed timestamp. No records are skipped, but
records between the last claimed timestamp and the failure may be re-emitted.

**Delivery guarantee: at-least-once.** Pipelines that require exactly-once semantics must
deduplicate downstream (e.g., using a Spanner UPSERT keyed on `(PartitionToken, CommitTimestamp, RecordSequence, ServerTransactionID)`).

---

## Watermark correctness

The watermark controls how downstream windowing operations advance. An incorrect watermark
can cause records to be dropped as "late data".

The `changeStreamWatermarkEstimator` tracks two values:

| Field         | Meaning                                                 | Sentinel                                |
| ------------- | ------------------------------------------------------- | --------------------------------------- |
| `maxObserved` | Highest commit/heartbeat timestamp seen so far          | `math.MinInt64` (not yet advanced)      |
| `minPending`  | Minimum `StartTimestamp` of all partitions in the queue | `math.MaxInt64` (no pending partitions) |

`CurrentWatermark()` returns `min(maxObserved, minPending)`.

This prevents the watermark from advancing past data that has not yet been emitted. Without
`minPending`, if partition A advances the watermark to T1, a queued partition B with data at
T0 < T1 would arrive as late data.

After aggressive `TrySplit`, each restriction holds exactly one partition, so in steady state
`minPending == maxObserved == the partition's current position`. The dual-state design is
necessary for correctness during the brief window before splitting occurs.

---

## Transient error resilience

Spanner streaming reads can return transient gRPC errors, particularly during:

- Spanner backend maintenance (`UNAVAILABLE`)
- Transaction contention or leadership changes (`ABORTED`)

Rather than failing the bundle, these errors trigger a checkpoint-and-retry:

```
UNAVAILABLE / ABORTED  →  ResumeProcessingIn(1 second)
```

The restriction records the last committed timestamp, so the retry resumes exactly from where
reading left off. Non-retryable errors fail the bundle normally.

---

## Public API

```go
records := spannerio.ReadChangeStream(
    s,
    "projects/my-project/instances/my-instance/databases/my-db",
    "MyStream",        // change stream name (must match [A-Za-z_][A-Za-z0-9_]*)
    startTime,         // inclusive start timestamp
    time.Time{},       // zero value = unbounded (runs indefinitely)
    10_000,            // heartbeat interval in milliseconds
)
// records is a beam.PCollection of spannerio.DataChangeRecord
```

### DataChangeRecord fields

| Field                                  | Type                | Description                                                              |
| -------------------------------------- | ------------------- | ------------------------------------------------------------------------ |
| `PartitionToken`                       | `string`            | Change stream partition that produced this record                        |
| `CommitTimestamp`                      | `time.Time`         | When the mutations were committed                                        |
| `RecordSequence`                       | `string`            | Monotonically increasing within a partition for a given commit timestamp |
| `ServerTransactionID`                  | `string`            | Globally unique transaction identifier                                   |
| `IsLastRecordInTransactionInPartition` | `bool`              | Whether this is the final record for this transaction in this partition  |
| `Table`                                | `string`            | Modified table name                                                      |
| `ColumnMetadata`                       | `[]*ColumnMetadata` | Column names, types, and key membership                                  |
| `Mods`                                 | `[]*Mod`            | Per-row changes with `Keys`, `OldValues`, `NewValues`                    |
| `ModType`                              | `ModType`           | `ModTypeInsert`, `ModTypeUpdate`, or `ModTypeDelete`                     |
| `ValueCaptureType`                     | `ValueCaptureType`  | Which values are captured (see Spanner docs)                             |
| `NumberOfRecordsInTransaction`         | `int32`             | Total `DataChangeRecord`s for this transaction across all partitions     |
| `NumberOfPartitionsInTransaction`      | `int32`             | Total partitions that produced records for this transaction              |
| `TransactionTag`                       | `string`            | Application-defined transaction tag                                      |
| `IsSystemTransaction`                  | `bool`              | True for Spanner-internal transactions (e.g., TTL)                       |

`Mod.Keys`, `Mod.OldValues`, and `Mod.NewValues` are slices of `*ModValue`. Each `ModValue`
holds a column name and its value as a JSON-encoded string using the Spanner JSON value format
(e.g., `"\"hello\""` for a string, `"42"` for a number).

---

## Beam metrics

Three counters are emitted under the namespace `spannerio.changestream`:

| Metric                 | Description                                                        |
| ---------------------- | ------------------------------------------------------------------ |
| `records_emitted`      | Total `DataChangeRecord`s emitted by this stage                    |
| `partitions_completed` | Total partitions that reached a natural end (`PartitionEndRecord`) |
| `errors_transient`     | Total transient errors that triggered a checkpoint-and-retry       |

Access these via the Beam metrics API or your runner's monitoring UI (e.g., Dataflow Monitoring).

---

## Logging

Structured log output is emitted using the Beam log package at the following levels:

| Level   | Event                                                                   |
| ------- | ----------------------------------------------------------------------- |
| `DEBUG` | `ProcessElement` start: partition token, start timestamp, pending count |
| `INFO`  | Partition completed                                                     |
| `INFO`  | Child partitions discovered: count and start timestamp                  |
| `WARN`  | Transient error triggering checkpoint-and-retry                         |
| `ERROR` | Non-retryable error causing bundle failure                              |

---

## Comparison to the Java implementation

The Java SDK uses a Spanner metadata table (`SpannerIO.readChangeStream` with
`MetadataDatabase`) to coordinate partitions across workers. The Go implementation
deliberately avoids this:

| Concern                 | Java                                   | Go                                                                                                           |
| ----------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Partition coordination  | Spanner metadata table                 | SDF restriction (Beam runner state)                                                                          |
| External dependencies   | Spanner metadata DB required           | None beyond the source database                                                                              |
| Durability              | Metadata table survives runner restart | Runner checkpoint storage                                                                                    |
| Partition deduplication | Metadata table tracks seen tokens      | Not needed (newer Spanner API guarantees each token appears in exactly one parent's `ChildPartitionsRecord`) |

The trade-off is that the Go implementation relies on the runner's checkpoint storage for
durability rather than a persistent external store. For Dataflow, checkpoint state is backed
by Google Cloud Storage and is as durable as the metadata table approach.

---

## Known limitations

1. **No initial parallelism.** All partitions are discovered dynamically from the root query.
   Until the first `ChildPartitionsRecord` arrives, there is one restriction on one worker.
   Expect ~10 seconds of single-worker operation at pipeline start.

2. **At-least-once delivery.** Records can be re-emitted after a worker failure. Deduplicate
   downstream if exactly-once semantics are required.

3. **Heartbeat records are not emitted.** Heartbeat records advance the watermark internally
   but are not output as pipeline elements. If you need explicit heartbeat visibility, add a
   side output in a downstream DoFn.

4. **SQL injection guard is name-pattern only.** `ReadChangeStream` panics if the change
   stream name does not match `[A-Za-z_][A-Za-z0-9_]*`. The name is still interpolated
   into SQL (Spanner does not accept parameterised function names), so the regex is the
   complete safety measure.
