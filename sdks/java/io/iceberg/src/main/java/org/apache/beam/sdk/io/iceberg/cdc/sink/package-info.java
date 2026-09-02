/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Iceberg CDC sink: applies a stream or batch of inserts, updates and deletes to Iceberg V2+
 * tables, exposed as {@code IcebergIO.writeCdcRows(catalogConfig)}. For the transform's API and
 * semantics see {@code WriteCdcRows}.
 *
 * <p>This is a merge-on-read sink. Within one commit window the writer collapses each primary key's
 * changes to their final state, so a superseded intermediate row is never written at all; a change
 * that reaches back into an already-committed snapshot is written as a PK-only <b>equality
 * delete</b>. No position deletes or deletion vectors are ever written.
 *
 * <h2>Monitoring</h2>
 *
 * <p>All metrics below are queryable through {@code PipelineResult.metrics()}; the namespace is the
 * owning class's fully-qualified name, so filter on {@code
 * org.apache.beam.sdk.io.iceberg.cdc.sink.*}.
 *
 * <h3>Committer volume and latency</h3>
 *
 * <p>The normal throughput picture. None of these indicate a problem by themselves; watch their
 * shape over time.
 *
 * <ul>
 *   <li><b>snapshotsCreated</b>: CDC snapshots committed. In streaming this should track one per
 *       commit window per destination. A flat line while data is flowing means commits have
 *       stalled.
 *   <li><b>committedDataFiles</b> / <b>committedDeleteFiles</b>: files published. Divide by {@code
 *       snapshotsCreated} for files per commit. A large ratio points to the small-file problem
 *       described below.
 *   <li><b>committedRecords</b>: rows committed.
 *   <li><b>committedEqualityDeleteRecords</b>: equality-delete rows committed.
 *   <li><b>committedBytes</b>: total bytes of committed files.
 *   <li><b>commitDurationMs</b> (distribution): wall-clock time spent waiting on the Iceberg commit
 *       operation. Its tail is your catalog's health: a growing maximum usually means catalog
 *       contention, not a Beam problem.
 *   <li><b>commitFailures</b>: fires whenever the commit path throws. Iceberg's own optimistic
 *       retry runs underneath, so a genuine commit collision is counted only once that is
 *       exhausted. The failure is rethrown, so the bundle fails and the runner retries it;
 *       occasional increments under heavy concurrent writing are survivable. A sustained rate means
 *       the table has too many concurrent writers, and the sink will not make progress past the
 *       failing window (commits are strictly ordered).
 *   <li><b>heartbeatCommits</b>: empty token-refresh commits emitted while a destination is idle
 *       (only if {@code withTokenHeartbeat} is configured). Each one is a real snapshot. With
 *       heartbeat enabled prefer cancel-and-resubmit over drain: the self-re-arming processing-time
 *       timer can keep a drain from completing.
 * </ul>
 *
 * <h3>Safety tripwires</h3>
 *
 * <p>These should be <b>zero</b>. A nonzero value is not necessarily an outage, but each one means
 * something specific happened that you want to know about.
 *
 * <ul>
 *   <li><b>alreadyCommittedWindowsSkipped</b>: a commit window was skipped because the table
 *       already carried a committed-through token at or past its end. Expected and benign after a
 *       restart or a retried bundle (we skip to ensure idempotency). Alarming in two cases: (1) in
 *       <b>batch</b>, where the whole load commits under one window, a nonzero value on a fresh
 *       load means a stable {@code sink_id} matched a <i>previous</i> load and this run wrote
 *       nothing at all. Use a unique {@code sink_id} per load, or omit it. Never carry a batch
 *       load's {@code sink_id} into a subsequent <b>streaming</b> run (the batch token is the
 *       global-window end, above every streaming window; the streaming committer will refuse a
 *       recovered batch token); and (2) in streaming, a persistently nonzero rate means data keeps
 *       arriving for windows that already closed, which is a source-lateness problem. The token
 *       proves that a window with that end committed, not that the skipped rows are the rows that
 *       committed. A logged warning <b>names up to 5 of the window's files plus a count</b>: your
 *       handle on the skipped rows until {@code remove_orphan_files} reclaims them.
 *   <li><b>orphanFiles</b>: files (data <i>and</i> delete) carried by a skipped window: potential
 *       orphans (a pure redelivery's files are the committed live ones). Wasted storage until
 *       {@code remove_orphan_files} runs.
 *   <li><b>tokenParseFailures</b>: an unparseable {@code beam.cdc.*} value was found while scanning
 *       snapshot ancestry. The committed-through scan continues to older ancestors rather than
 *       crash-looping; a bad max-seq or run-spec stamp just reads as absent. Nonzero means
 *       something wrote a malformed value into a snapshot summary; investigate before trusting the
 *       recovered position.
 *   <li><b>suspectedTokenExpiry</b>: the sink's own {@code beam.cdc.sink-id} marker was found in
 *       the ancestry but no committed-through token was. Strongly implies that {@code
 *       expire_snapshots} removed the token-bearing snapshots while the pipeline was down. Recovery
 *       then falls back to the beginning and may re-apply retained windows. If you see this, either
 *       lengthen snapshot retention or enable {@code withTokenHeartbeat} so the token stays young.
 *   <li><b>crossWindowSequenceInversions</b>: a committed window's minimum source sequence number
 *       was below an earlier window's committed maximum. This is the detector for a violated
 *       ordering contract (see below) and the only tripwire here that can mean <i>silently wrong
 *       table contents</i>. It has benign false positives when the two windows touch entirely
 *       disjoint primary keys, so treat it as "go and check the source's ordering", not as proof of
 *       corruption.
 *   <li><b>specMismatchedWindows</b>: signals the partition spec was evolved mid-run. Incremented
 *       when a committed window carries equality deletes and uses partition-spec ids different from
 *       the run's pinned spec. Those deletes may not reach rows written under the other spec.
 * </ul>
 *
 * <h3>Input health</h3>
 *
 * <p>These describe what is arriving, not what the sink did with it.
 *
 * <ul>
 *   <li><b>deadLetterRecords</b>: records whose grouped pane fired late (the watermark had already
 *       passed their commit window's end), diverted to the replayable dead-letter output ({@code
 *       IcebergWriteResult.getDeadLetterRows()}) instead of being applied out of order.
 *       <b>Every</b> late pane is diverted, including a window's first ({@code SplitLateData}'s
 *       javadoc explains why). Nonzero means your source is lagging past the commit window. The
 *       records are not lost, but nothing consumes them unless you wire that output somewhere.
 *       Records later than {@code withAllowedLateness} are a different case: see below.
 *   <li><b>failedRecords</b>: poison records diverted to {@code IcebergWriteResult.getFailedRows()}
 *       when {@code withErrorHandling()} is on: unknown change type, missing sequence number, null
 *       equality value, unresolvable destination. Without error handling these fail the pipeline
 *       instead. Like the dead letters above, the records are not lost, but you need to wire the
 *       output somewhere to avoid dropping them.
 *   <li><b>schemaDriftRebuilds</b>: the per-record projection had to be rebuilt because the
 *       incoming row schema changed. Normally zero. A steady rate is correct but slow: your source
 *       is emitting rows with an unstable schema.
 *   <li><b>upsertUpdateBeforeDropped</b>: {@code UPDATE_BEFORE} records discarded because the sink
 *       is in upsert mode, which needs only after-images. Tells you how much of your input was
 *       redundant.
 * </ul>
 *
 * <p>Records more than {@code withAllowedLateness} behind the watermark are dropped by the runner
 * at the {@code GroupByShardKey} step. Raising {@code withAllowedLateness} is the only way to
 * capture them (at the cost of more live window state per destination).
 *
 * <h2>Files and maintenance</h2>
 *
 * <p>File count per commit is governed by <b>{@code num_shards} &times; touched partitions</b>, and
 * total file count by that times the number of commit windows. A table with many partitions and a
 * short {@code triggering_frequency_seconds} produces a lot of small files very quickly, regardless
 * of the data rate. {@code num_shards} (default=16) is the sink's write-parallelism knob. Too low
 * may lead to a write bottleneck and commit backlog. Too high is fine for the sink but may lead to
 * slower downstream reads due to small files.
 *
 * <p>There are three levers that you can use:
 *
 * <ul>
 *   <li><b>{@code triggering_frequency_seconds}</b>: increasing it leads to fewer, larger commits,
 *       at the cost of end-to-end latency.
 *   <li><b>{@code num_shards}</b>: trades throughput for file count. Lowering it reduces file
 *       count, but also the write-parallelism ceiling.
 *   <li><b>{@code shards_per_partition}</b> (default={@code num_shards}, i.e. no cap): on a
 *       <b>partitioned</b> table, cap how many shards one partition's rows may occupy. A touched
 *       partition then writes about {@code min(shards_per_partition, distinct keys)} files per file
 *       kind per commit, with its write parallelism capped to match; {@code 1} is the pure
 *       partition-affine endpoint (one writer, about one data file, per partition).
 * </ul>
 *
 * <p><b>The trade-off a lowered {@code shards_per_partition} makes, stated plainly:</b> effective
 * write parallelism per destination becomes at most the cap times the number of distinct partitions
 * receiving data in a window: at {@code 1}, one writer per partition. A cap of {@code 1} is right
 * for a table with many partitions and <b>wrong for a table with three</b>, which it would hold at
 * three concurrent writers however many workers the pipeline has; such a table wants an
 * intermediate cap, sized so cap &times; touched partitions still covers the pipeline's write
 * parallelism. The cap is moot for an unpartitioned table, where it is simply ignored (with a WARN)
 * rather than funnelling everything through a single shard. At {@code 1} this is the same
 * limitation Flink's {@code hash} write-distribution mode carries; the dial exists because the
 * alternative (leaving {@code num_shards} as the only lever) forces the same trade with none of the
 * benefit. It is safe with {@code upsert}: both options require every partition source column to be
 * an equality column (see the partitioning section below), which makes a row's partition a pure
 * function of its primary key.
 *
 * <h3>Snapshot expiry bounds the sink's own cost, not just read performance</h3>
 *
 * <p>This sink creates <b>one snapshot per commit window per table</b>. The following sink costs
 * grow as snapshot count grows:
 *
 * <ul>
 *   <li>Commit latency: Every commit fire loads the destination's metadata once, then walks its
 *       ancestry to find the latest committed-through token. Each commit within that fire publishes
 *       a snapshot, which requires refreshing and re-parsing the metadata. Both scale with the
 *       number of snapshots in that file.
 *   <li>Worker heap: Each cached table pins a {@code TableMetadata} holding every retained
 *       snapshot, per worker, multiplied by the number of dynamic destinations that worker touches.
 * </ul>
 *
 * <h3>Orphan files</h3>
 *
 * <p>The writer stage writes data files and delete files and passes their metadata to the committer
 * stage in the pipeline. Every orphan is an ordinary data or delete file that no snapshot ended up
 * referencing. Orphan files can be produced in three ways:
 *
 * <ul>
 *   <li><b>A retried or failed writer bundle.</b> Each attempt names its files with a fresh UUID,
 *       so whichever attempt's output element loses the retry race leaves its files unreferenced.
 *   <li><b>A window skipped because the committed-through token already covers its end.</b> This
 *       happens when a relaunch reuses a stable {@code sink_id}. The incoming element is targeting
 *       a window that has already been committed with different files. If the incoming element's
 *       files contain different content than the committed ones, they may hold rows the table never
 *       received. Those files are left in place so you can recover them. Counted by {@code
 *       orphanFiles}; the skip WARN names up to 5 of them plus a count.
 *   <li><b>A failed commit attempt.</b> Iceberg normally deletes the manifests it wrote when a
 *       commit fails, but it cannot when the outcome is unknown (i.e. {@code
 *       CommitStateUnknownException}). The commit may in fact have succeeded, and deleting those
 *       manifests would corrupt a live snapshot. Only the manifests are left orphaned. The window's
 *       own data and delete files are untouched, because the pending bag survives the failure and
 *       the retry commits those same files.
 * </ul>
 *
 * <p>All of these are reclaimed by Iceberg's {@code remove_orphan_files}. The sink writes
 * everything through the table's own write path. Give the procedure an age threshold much longer
 * than your longest in-flight bundle to avoid deleting files a live pipeline is about to reference.
 *
 * <h3>Partition-spec evolution mid-run</h3>
 *
 * <p>Iceberg matches an equality delete to its data files by {@code (spec id, partition)}, so if a
 * table's spec changes during streaming writes, the sink will choose to keep writing with the old
 * spec. Otherwise, it could leave new deletes that never reach old-spec rows, which is data
 * corruption. If a window happens to still mix specs while carrying equality deletes, it commits
 * anyway, WARNs, and increments the {@code specMismatchedWindows} counter.
 *
 * <p>If you update a table's spec, run {@code rewrite_data_files} first so old-spec rows are
 * rewritten under the new spec. A relaunched pipeline or in-place update will adopt the new spec.
 *
 * <h2>Partitioning</h2>
 *
 * <p>Tables may be partitioned on any columns, key or not, with any transforms. Two options still
 * require every partition source column to be an equality column: {@code upsert} (before-images are
 * dropped, so a row that moved partitions could never be deleted from its old one) and a {@code
 * shards_per_partition} cap below {@code num_shards} (the shard is derived from the partition
 * tuple, which must therefore be a pure function of the primary key).
 *
 * <p>Non-key partitioning sharpens the input contract: every update must carry its {@code
 * UPDATE_BEFORE}, because a moved row whose before-image never arrives leaves a permanent duplicate
 * in the old partition that no later delete reaches; and {@code UPDATE_BEFORE}/{@code DELETE} rows
 * must carry the row's actual old values in the partition source columns, because a nulled non-key
 * column routes the equality delete to the null partition.
 *
 * <h2>The ordering contract</h2>
 *
 * <p>The sink orders each primary key's changes by the sequence-number column, and commits one
 * snapshot per commit window in ascending window order. That is only sound if the two orderings
 * agree at the source: <b>for a given key, an element's event time must be non-decreasing with its
 * (sequence number, kind rank), so equal-sequence records (an update's before and after images)
 * carry equal event times.</b> If a higher-sequence change lands in an <i>earlier</i> commit window
 * than a lower-sequence one, a stale equality delete can be committed after the row it should not
 * have touched, and the table's final contents are wrong.
 *
 * <p>This is a source contract. Violations are detected at commit time and visible with the {@code
 * crossWindowSequenceInversions} counter and warning logs, but the commit still proceeds. The fix
 * is in how event times are assigned upstream. Replaying dead letters is bound by the same
 * contract: see the replay caveat at {@code IcebergWriteResult#getDeadLetterRows}.
 */
package org.apache.beam.sdk.io.iceberg.cdc.sink;
