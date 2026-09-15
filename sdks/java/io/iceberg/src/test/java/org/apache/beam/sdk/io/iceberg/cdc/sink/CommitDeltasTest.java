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
package org.apache.beam.sdk.io.iceberg.cdc.sink;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.sdk.values.ValueKind.DELETE;
import static org.apache.beam.sdk.values.ValueKind.INSERT;
import static org.apache.beam.sdk.values.ValueKind.UPDATE_AFTER;
import static org.apache.beam.sdk.values.ValueKind.UPDATE_BEFORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.iceberg.IcebergCatalogConfig;
import org.apache.beam.sdk.io.iceberg.SerializableDataFile;
import org.apache.beam.sdk.io.iceberg.SerializableDeleteFile;
import org.apache.beam.sdk.io.iceberg.SnapshotInfo;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.primitives.Ints;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link CommitDeltas}. Each test drives a real HadoopCatalog table; committer
 * inputs are staged through a real {@link RecordDeltaTaskWriter} and {@link WriteDeltas#serialize}
 * (the production write path), and a {@link TestStream} drives windows and the watermark. Each test
 * uses a unique table identifier so the process-wide TableCache never sees a repeat.
 */
@RunWith(JUnit4.class)
public class CommitDeltasTest {

  @Rule public transient TestPipeline p = TestPipeline.create();
  @Rule public transient TemporaryFolder tmp = new TemporaryFolder();

  /** Captures the committer's own WARNs, so a test can assert one was NOT emitted. */
  @Rule public transient ExpectedLogs expectedLogs = ExpectedLogs.none(CommitDeltas.class);

  private static int tableCounter = 0;

  /** Committer outputs collected per sink id; static because the runner serializes DoFn fields. */
  private static final ConcurrentMap<String, List<SnapshotInfo>> EMITTED =
      new ConcurrentHashMap<>();

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private static final long TARGET_FILE_SIZE = 512L * 1024 * 1024;
  private static final Duration WINDOW = Duration.standardMinutes(1);

  private Catalog catalog;

  @Before
  public void setUp() {
    catalog = CdcSinkTestUtils.hadoopCatalog(tmp.getRoot());
  }

  @After
  public void resetTestSeams() {
    CommitDeltas.preCommitHookForTest = null;
    CommitDeltas.onFireForTest = null;
  }

  private IcebergCatalogConfig catalogConfig() {
    return CdcSinkTestUtils.catalogConfig(tmp.getRoot());
  }

  /**
   * A fresh table plus its {@code "db.name"} destination identifier (never re-derived from {@link
   * Table#name()}, which is catalog-qualified).
   */
  private static final class TestTable {
    final Table table;
    final String dest;

    TestTable(Table table, String dest) {
      this.table = table;
      this.dest = dest;
    }
  }

  /** A fresh unpartitioned table of the given format version, PK = {@code id}. */
  private TestTable table(int formatVersion) {
    TableIdentifier id = TableIdentifier.of("db", "t" + tableCounter++ + "_" + System.nanoTime());
    Table table =
        CdcSinkTestUtils.createTable(
            catalog, id, SCHEMA, ImmutableSet.of(1), formatVersion, PartitionSpec.unpartitioned());
    return new TestTable(table, id.toString());
  }

  /** A fresh V2 table. */
  private TestTable v2Table() {
    return table(2);
  }

  /** A fresh V3 table. */
  private TestTable v3Table() {
    return table(3);
  }

  private static Record rec(int id, String name, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("name", name);
    record.setField("data", data);
    return record;
  }

  /** A single {@code (Record, ValueKind)} change, for {@link #writeFiles}. */
  private static KV<Record, ValueKind> change(int id, String name, String data, ValueKind kind) {
    return KV.of(rec(id, name, data), kind);
  }

  /**
   * Writes {@code records} through a real {@link RecordDeltaTaskWriter} (files land on disk,
   * nothing commits) and returns the completed {@link WriteResult}. Each sort key's pk prefix
   * carries the record's encoded {@code id}, so same-id records form one collapse block.
   */
  private static WriteResult writeFiles(TestTable tt, List<KV<Record, ValueKind>> records) {
    RecordDeltaTaskWriter writer =
        CdcSinkTestUtils.deltaWriter(tt.table, ImmutableSet.of(1), false, TARGET_FILE_SIZE);
    try {
      long seq = 0;
      for (KV<Record, ValueKind> r : records) {
        byte[] pk = Ints.toByteArray((Integer) checkStateNotNull(r.getKey().getField("id")));
        writer.write(CdcSortKey.encode(pk, seq++, r.getValue()), r.getKey(), r.getValue());
      }
      return writer.complete();
    } catch (Exception e) {
      try {
        writer.abort();
      } catch (Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * One shard's {@link ShardDeltaFiles}: writes {@code records} and serializes the completed files
   * through the production {@link WriteDeltas#serialize} seam.
   */
  private ShardDeltaFiles stage(
      TestTable tt, long minSeq, long maxSeq, List<KV<Record, ValueKind>> records) {
    return WriteDeltas.serialize(tt.dest, tt.table, writeFiles(tt, records), minSeq, maxSeq);
  }

  /** The locations of the data files {@code files} carries, in order. */
  private static List<String> dataFilePaths(ShardDeltaFiles files) {
    return files.getDataFiles().stream()
        .map(SerializableDataFile::getPath)
        .collect(ImmutableList.toImmutableList());
  }

  /** The locations of the delete files {@code files} carries, in order. */
  private static List<String> deleteFilePaths(ShardDeltaFiles files) {
    return files.getDeleteFiles().stream()
        .map(SerializableDeleteFile::getLocation)
        .collect(ImmutableList.toImmutableList());
  }

  private static Coder<ShardDeltaFiles> filesCoder() {
    return WriteDeltas.shardDeltaFilesCoder();
  }

  private static long committedThroughMs(Snapshot s, String sinkId) {
    String v = s.summary().get("beam.cdc.committed-through-ms." + sinkId);
    return v == null ? Long.MIN_VALUE : Long.parseLong(v);
  }

  /** The end millis of the {@link #WINDOW}-aligned window containing {@code ts}. */
  private static long windowEndMs(Instant ts) {
    long size = WINDOW.getMillis();
    long start = (ts.getMillis() / size) * size;
    return start + size - 1; // BoundedWindow.maxTimestamp() = end - 1ms
  }

  /** Reads all live rows of {@code table} as sorted {@code id:name:data} strings. */
  private static List<String> readRows(Table table) {
    table.refresh();
    return ImmutableList.copyOf(IcebergGenerics.read(table).build()).stream()
        .map(r -> r.getField("id") + ":" + r.getField("name") + ":" + r.getField("data"))
        .sorted()
        .collect(ImmutableList.toImmutableList());
  }

  /** Refreshes {@code table} and returns its snapshots, oldest first. */
  private static List<Snapshot> snapshotsOf(Table table) {
    table.refresh();
    return Lists.newArrayList(table.snapshots());
  }

  /** Sums the named {@link CommitDeltas} counter committed by the pipeline. */
  private static long counter(PipelineResult result, String name) {
    Iterable<MetricResult<Long>> counters =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(CommitDeltas.class, name))
                    .build())
            .getCounters();
    long total = 0;
    for (MetricResult<Long> c : counters) {
      total += c.getCommitted();
    }
    return total;
  }

  /**
   * Seeds a real prior CDC snapshot for {@code sinkId}, token keys written as the committer would.
   * The keys are literal strings on purpose: they double as a pin of the on-disk contract.
   */
  private void seedCommittedSnapshot(
      TestTable tt,
      String sinkId,
      long windowEndMs,
      long maxSeq,
      List<KV<Record, ValueKind>> seed) {
    WriteResult result = writeFiles(tt, seed);
    RowDelta rowDelta = tt.table.newRowDelta();
    Arrays.stream(result.dataFiles()).forEach(rowDelta::addRows);
    Arrays.stream(result.deleteFiles()).forEach(rowDelta::addDeletes);
    rowDelta.set("beam.cdc.committed-through-ms." + sinkId, Long.toString(windowEndMs));
    rowDelta.set("beam.cdc.max-committed-seq." + sinkId, Long.toString(maxSeq));
    rowDelta.set("beam.cdc.sink-id", sinkId);
    rowDelta.commit();
  }

  // ---------------------------------------------------------------------------------------------
  // 1. Windows commit in ascending window-end order, one snapshot each.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void commitsWindowsInAscendingOrder() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    // W0 = [0, 60s), W1 = [60s, 120s).
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    Instant w1Ts = base.plus(Duration.standardSeconds(61));

    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    ShardDeltaFiles w1 = stage(tt, 20L, 20L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    // W1's result is delivered BEFORE W0's; the committer must still commit W0 first.
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w1, w1Ts))
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    p.run().waitUntilFinish();

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(2));
    assertThat(
        committedThroughMs(snaps.get(0), sinkId),
        lessThan(committedThroughMs(snaps.get(1), sinkId)));
    assertThat(snaps.get(0).sequenceNumber(), lessThan(snaps.get(1).sequenceNumber()));
    assertThat(snaps.get(0).summary().get("beam.cdc.max-committed-seq." + sinkId), equalTo("10"));
    assertThat(snaps.get(1).summary().get("beam.cdc.max-committed-seq." + sinkId), equalTo("20"));
    assertThat(readRows(t), contains("1:a:x", "2:b:y"));

    // The eager-committer gate: arrival order was W1-then-W0, so an on-arrival committer inverts
    // the ancestry tokens. They must be strictly descending newest -> oldest.
    List<Long> tokensNewestFirst = new ArrayList<>();
    Snapshot current = checkStateNotNull(t.currentSnapshot());
    for (Snapshot s : SnapshotUtil.ancestorsOf(current.snapshotId(), t::snapshot)) {
      tokensNewestFirst.add(committedThroughMs(s, sinkId));
    }
    assertThat(tokensNewestFirst, contains(windowEndMs(w1Ts), windowEndMs(w0Ts)));
  }

  // ---------------------------------------------------------------------------------------------
  // 2. Multiple shards of one window merge into ONE snapshot.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void mergesAllShardsForAWindowIntoOneSnapshot() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1)); // both in W0

    // Two separate writer outputs (shard 0 and shard 1) in the SAME window.
    ShardDeltaFiles shard0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles shard1 = stage(tt, 11L, 11L, Lists.newArrayList(change(2, "b", "y", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(shard0, w0Ts), TimestampedValue.of(shard1, w0Ts))
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    p.run().waitUntilFinish();

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(1));
    Snapshot snap = checkStateNotNull(t.currentSnapshot());
    assertThat(Lists.newArrayList(snap.addedDataFiles(t.io())), hasSize(2));
    assertThat(readRows(t), contains("1:a:x", "2:b:y"));
  }

  // ---------------------------------------------------------------------------------------------
  // 3. Replayed / already-committed window: loud skip, files left recoverable, no new snapshot.
  // ---------------------------------------------------------------------------------------------

  /**
   * The token proves a window with this end committed, not that the rows in hand are the rows that
   * committed, so the skip must leave the replay's data files on disk and name them: they are the
   * only remaining reference to those rows until {@code remove_orphan_files} reclaims them.
   */
  @Test
  public void skipsAlreadyCommittedWindowOnReplayAndLeavesItsDataFilesRecoverable() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    long w0End = windowEndMs(w0Ts);

    seedCommittedSnapshot(tt, sinkId, w0End, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    int snapsAfterSeed = snapshotsOf(t).size();

    // Replay W0 as TWO shard outputs => 2 orphaned data files.
    ShardDeltaFiles shard0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles shard1 = stage(tt, 11L, 11L, Lists.newArrayList(change(9, "z", "w", INSERT)));
    String shard0DataPath = Iterables.getOnlyElement(dataFilePaths(shard0));
    String shard1DataPath = Iterables.getOnlyElement(dataFilePaths(shard1));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(shard0, w0Ts), TimestampedValue.of(shard1, w0Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(snapsAfterSeed));
    assertThat(counter(result, "alreadyCommittedWindowsSkipped"), equalTo(1L));
    assertThat(counter(result, "orphanFiles"), equalTo(2L));
    // The files still exist. Nothing in the committer deletes files today, so this guards future
    // deletion-wiring: an abort() on the skip path would destroy the very rows the skip WARN
    // tells the operator to recover before remove_orphan_files runs.
    for (String path : new String[] {shard0DataPath, shard1DataPath}) {
      assertThat("expected to still exist: " + path, t.io().newInputFile(path).exists(), is(true));
    }
    // The skip WARN names exactly those files, so the operator can find them before then.
    String described =
        CommitDeltas.describeSkippedFiles(
            CommitDeltas.WindowedCommit.of(w0End, ImmutableList.of(shard0, shard1)));
    assertThat(described, containsString(shard0DataPath));
    assertThat(described, containsString(shard1DataPath));
  }

  // ---------------------------------------------------------------------------------------------
  // 4. Batch-rerun semantics: same sink id + same (global) window => loud no-op skip.
  // ---------------------------------------------------------------------------------------------

  /**
   * In batch all data commits under the global window, so a second run with the same {@code
   * sink_id} recovers that token and skips ALL writes: loudly, or a periodic batch load silently
   * loses data.
   */
  @Test
  public void batchRerunWithStableSinkIdSkipsLoudly() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "stable-" + System.nanoTime();

    long globalWindowEnd = GlobalWindow.INSTANCE.maxTimestamp().getMillis();

    // The prior batch run: a committed snapshot whose token is the global-window end.
    seedCommittedSnapshot(
        tt, sinkId, globalWindowEnd, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    int snapsAfterSeed = snapshotsOf(t).size();

    // The SECOND batch run (fresh committer state, same sinkId) with DIFFERENT files.
    ShardDeltaFiles rerun = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));

    PipelineResult result =
        p.apply(Create.of(rerun).withCoder(filesCoder())) // bounded => single global window
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(snapsAfterSeed));
    assertThat(counter(result, "alreadyCommittedWindowsSkipped"), equalTo(1L));
  }

  /**
   * A STREAMING run that recovers a batch token (the global-window end) must fail the destination
   * with a named error: every real-time window end is below that token, so the alternative is
   * skipping every window forever while reporting success. The bounded twin of this input keeps the
   * loud-no-op semantics instead ({@link #batchRerunWithStableSinkIdSkipsLoudly}).
   */
  @Test
  public void streamingRunRecoveringBatchTokenFailsWithNamedError() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "handoff-" + System.nanoTime();

    // The prior batch load: a committed snapshot whose token is the global-window end.
    long globalWindowEnd = GlobalWindow.INSTANCE.maxTimestamp().getMillis();
    seedCommittedSnapshot(
        tt, sinkId, globalWindowEnd, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    int snapsAfterSeed = snapshotsOf(t).size();

    // The streaming continuation, same sink_id: one ordinary real-time window.
    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    ShardDeltaFiles w0 = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));

    Exception e = assertThrows(Exception.class, () -> p.run().waitUntilFinish());

    // The named error names what was recovered, what it means, and what to do about it.
    String trace = Throwables.getStackTraceAsString(e);
    assertThat(trace, containsString("CDC sink '" + sinkId + "'"));
    assertThat(trace, containsString("table '" + tt.dest + "'"));
    assertThat(
        trace, containsString("equal to the global-window end (" + globalWindowEnd + " ms)"));
    assertThat(trace, containsString("last used by a batch (bounded) load"));
    assertThat(trace, containsString("skip every window forever"));
    assertThat(trace, containsString("different sink_id for the streaming continuation"));

    // Fail-fast, not loud-no-op: nothing was committed and the skip path never ran.
    assertThat(snapshotsOf(t), hasSize(snapsAfterSeed));
    expectedLogs.verifyNotLogged("skipping window-end");
  }

  // ---------------------------------------------------------------------------------------------
  // 5. Token recovery scans ancestry, not just currentSnapshot.
  // ---------------------------------------------------------------------------------------------

  /**
   * A foreign append (e.g. a compaction) after the sink's last commit leaves {@code
   * currentSnapshot()} without the sink's token; recovery must find it by ancestry scan.
   */
  @Test
  public void recoversTokenBehindForeignCommitAndCommitsNextWindow() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    Instant w1Ts = base.plus(Duration.standardSeconds(61));
    long w0End = windowEndMs(w0Ts);

    // 1) Commit W0 with the sink's token.
    seedCommittedSnapshot(tt, sinkId, w0End, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    // 2) A foreign append (compaction-like) commits AFTER us: currentSnapshot LACKS our token.
    WriteResult other = writeFiles(tt, Lists.newArrayList(change(5, "m", "n", INSERT)));
    AppendFiles append = t.newAppend();
    Arrays.stream(other.dataFiles()).forEach(append::appendFile);
    append.commit();
    t.refresh();
    Snapshot foreign = checkStateNotNull(t.currentSnapshot());
    assertThat(foreign.summary().get("beam.cdc.committed-through-ms." + sinkId), nullValue());
    int snapsAfterForeign = snapshotsOf(t).size();

    // 3) Re-feed W0 (must SKIP: the ancestry scan finds the token) and feed a new W1 (must COMMIT).
    ShardDeltaFiles replay = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles w1 = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(replay, w0Ts))
            .addElements(TimestampedValue.of(w1, w1Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    // Exactly one new snapshot (W1); the W0 replay was skipped via the recovered token.
    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(snapsAfterForeign + 1));
    assertThat(counter(result, "alreadyCommittedWindowsSkipped"), equalTo(1L));
    Snapshot newest = checkStateNotNull(t.currentSnapshot());
    assertThat(committedThroughMs(newest, sinkId), equalTo(windowEndMs(w1Ts)));
  }

  /**
   * The emitted {@link SnapshotInfo} must be the snapshot carrying THAT window's token, found in
   * the refreshed ancestry, not {@code currentSnapshot()}, which a concurrent writer can own by the
   * time the commit returns. The race itself is not reproducible in-process; a pre-existing foreign
   * commit keeps the assertion honest.
   */
  @Test
  public void singleWindowCommitReportsItsOwnTokenSnapshot() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    long w0End = windowEndMs(w0Ts);

    // A foreign append (compaction-like) is already the table's current snapshot.
    WriteResult other = writeFiles(tt, Lists.newArrayList(change(5, "m", "n", INSERT)));
    AppendFiles append = t.newAppend();
    Arrays.stream(other.dataFiles()).forEach(append::appendFile);
    append.commit();
    t.refresh();
    long foreignId = checkStateNotNull(t.currentSnapshot()).snapshotId();
    int snapsAfterForeign = snapshotsOf(t).size();

    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();

    String collectKey = sinkId;
    EMITTED.put(collectKey, new CopyOnWriteArrayList<>());
    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId))
        .apply("CollectSnapshotInfo", ParDo.of(new CollectSnapshotInfoFn(collectKey)));
    p.run().waitUntilFinish();
    List<SnapshotInfo> emitted = checkStateNotNull(EMITTED.get(collectKey));

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(snapsAfterForeign + 1));
    Snapshot ours = snaps.get(snaps.size() - 1);
    assertThat(committedThroughMs(ours, sinkId), equalTo(w0End));

    SnapshotInfo info = Iterables.getOnlyElement(emitted);
    assertThat(info.getSnapshotId(), equalTo(ours.snapshotId()));
    assertThat(info.getSnapshotId(), not(equalTo(foreignId)));
    Map<String, String> summary = checkStateNotNull(info.getSummary());
    assertThat(
        summary.get("beam.cdc.committed-through-ms." + sinkId), equalTo(Long.toString(w0End)));
  }

  /** Collects the committer's emitted {@link SnapshotInfo}s into {@link #EMITTED}. */
  private static final class CollectSnapshotInfoFn
      extends DoFn<KV<String, SnapshotInfo>, SnapshotInfo> {
    private final String collectKey;

    CollectSnapshotInfoFn(String collectKey) {
      this.collectKey = collectKey;
    }

    @ProcessElement
    public void process(@Element KV<String, SnapshotInfo> element) {
      checkStateNotNull(EMITTED.get(collectKey)).add(element.getValue());
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 6. Corrupt token: counted, logged, and recovery falls back to the older intact token.
  // ---------------------------------------------------------------------------------------------

  /**
   * A corrupt token value must not crash-loop the committer: it is logged and counted, and recovery
   * falls back to the older INTACT token.
   */
  @Test
  public void corruptTokenCountsParseFailureAndRecoversViaOlderIntactToken() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    Instant w1Ts = base.plus(Duration.standardSeconds(61));
    long w0End = windowEndMs(w0Ts);

    // Older snapshot: INTACT token for W0. Newer snapshot: unparseable token value.
    seedCommittedSnapshot(tt, sinkId, w0End, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    WriteResult corrupt = writeFiles(tt, Lists.newArrayList(change(5, "m", "n", INSERT)));
    RowDelta corruptDelta = t.newRowDelta();
    Arrays.stream(corrupt.dataFiles()).forEach(corruptDelta::addRows);
    corruptDelta.set("beam.cdc.committed-through-ms." + sinkId, "garbage");
    corruptDelta.set("beam.cdc.sink-id", sinkId);
    corruptDelta.commit();
    int snapsAfterSeeds = snapshotsOf(t).size();

    // Re-feed W0 (skipped via the OLDER intact token) and a fresh W1 (commits normally).
    ShardDeltaFiles replay = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles w1 = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(replay, w0Ts))
            .addElements(TimestampedValue.of(w1, w1Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(snapsAfterSeeds + 1));
    assertThat(counter(result, "alreadyCommittedWindowsSkipped"), equalTo(1L));
    assertThat(counter(result, "tokenParseFailures"), greaterThanOrEqualTo(1L));
    assertThat(
        committedThroughMs(checkStateNotNull(t.currentSnapshot()), sinkId),
        equalTo(windowEndMs(w1Ts)));
  }

  /**
   * The recovered token and max-committed-seq must come from the SAME snapshot. A newer snapshot
   * whose token is corrupt but whose max seq is intact must not lend that max seq to the older
   * token the walk falls back to: an inflated max seq makes the replayed windows look like sequence
   * inversions.
   */
  @Test
  public void corruptTokenDoesNotLendItsMaxSeqToTheOlderToken() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    // Older snapshot: intact token, max seq 10. Newer: corrupt token, intact max seq 999.
    seedCommittedSnapshot(
        tt, sinkId, 60_000L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    AppendFiles corrupt = t.newAppend();
    corrupt.set("beam.cdc.committed-through-ms." + sinkId, "garbage");
    corrupt.set("beam.cdc.max-committed-seq." + sinkId, "999");
    corrupt.set("beam.cdc.sink-id", sinkId);
    corrupt.commit();
    t.refresh();

    CommitToken.Recovered recovered = token(sinkId, "runId-n").recoverFrom(t, tt.dest);
    assertThat(recovered.committedThroughMs, equalTo(60_000L));
    assertThat(recovered.maxCommittedSeq, equalTo(10L));
  }

  // ---------------------------------------------------------------------------------------------
  // 7. Sink-id marker present but no parseable token anywhere: suspected expiry + fresh start.
  // ---------------------------------------------------------------------------------------------

  /**
   * Sink-id marker present but no parseable token anywhere (the token snapshots expired away):
   * recovery falls back to a fresh start but must count {@code suspectedTokenExpiry} first.
   */
  @Test
  public void suspectedTokenExpiryWhenSinkMarkerButNoToken() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    // Seed a snapshot carrying the sink-id marker but NO committed-through key.
    WriteResult seed = writeFiles(tt, Lists.newArrayList(change(1, "a", "x", INSERT)));
    RowDelta seedDelta = t.newRowDelta();
    Arrays.stream(seed.dataFiles()).forEach(seedDelta::addRows);
    seedDelta.set("beam.cdc.sink-id", sinkId);
    seedDelta.commit();
    int snapsAfterSeed = snapshotsOf(t).size();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    ShardDeltaFiles w0 = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(snapsAfterSeed + 1));
    assertThat(counter(result, "suspectedTokenExpiry"), greaterThanOrEqualTo(1L));
  }

  // ---------------------------------------------------------------------------------------------
  // 8. RowDelta vs AppendFiles selection by delete-file presence.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void rowDeltaUsedWhenDeleteFilesPresentAppendWhenNot() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1)); // W0 = data only
    Instant w1Ts = base.plus(Duration.standardSeconds(61)); // W1 = has a delete

    // W0: data only => append. W1: an update pair => an equality delete file => RowDelta.
    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles w1 =
        stage(
            tt,
            20L,
            21L,
            Lists.newArrayList(
                change(1, "a", "x", UPDATE_BEFORE), change(1, "a2", "x2", UPDATE_AFTER)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .addElements(TimestampedValue.of(w1, w1Ts))
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    p.run().waitUntilFinish();

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(2));
    // W0 (data only) => append fast path.
    assertThat(snaps.get(0).operation(), equalTo(DataOperations.APPEND));
    // W1 (delete files present) => RowDelta => overwrite.
    assertThat(snaps.get(1).operation(), equalTo(DataOperations.OVERWRITE));
  }

  // ---------------------------------------------------------------------------------------------
  // 9. Commit failure: triage context, nothing committed past the failure, retry run succeeds.
  // ---------------------------------------------------------------------------------------------

  /**
   * An injected commit failure must carry operator-triage context and leave NO snapshot (strict
   * ascending order halts W1 behind the failing W0); a second run re-feeds the same inputs and
   * commits both windows, proving the failure left the files intact. (The DirectRunner fails fast
   * rather than retrying {@code @RequiresStableInput} bundles, so the in-run retry is an IT
   * concern.)
   */
  @Test
  public void commitFailureCarriesTriageContextAndRetryRunCommitsInOrder() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();
    CommitDeltas.preCommitHookForTest =
        () -> {
          throw new RuntimeException("injected commit failure (test)");
        };

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    Instant w1Ts = base.plus(Duration.standardSeconds(61));

    // W0: a DELETE of a key never inserted => one equality delete file that removes nothing.
    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", DELETE)));
    ShardDeltaFiles w1 = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));

    p.apply(
            TestStream.create(filesCoder())
                .advanceWatermarkTo(base)
                .addElements(TimestampedValue.of(w0, w0Ts))
                .addElements(TimestampedValue.of(w1, w1Ts))
                .advanceWatermarkToInfinity())
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));

    Exception e = assertThrows(Exception.class, () -> p.run().waitUntilFinish());
    String trace = Throwables.getStackTraceAsString(e);
    assertThat(trace, containsString("injected commit failure"));
    assertThat(trace, containsString("failed to commit table"));
    assertThat(trace, containsString("pending window"));
    assertThat(snapshotsOf(t), empty());
    // The triage promises the retry re-fires with the same windows and the same files.
    assertThat(trace, containsString("The pending bag is untouched"));

    // The retry: a fresh run (same sink id, same inputs) with the failure cleared.
    CommitDeltas.preCommitHookForTest = null;
    TestPipeline retry = TestPipeline.create();
    retry.enableAbandonedNodeEnforcement(false);
    retry
        .apply(
            TestStream.create(filesCoder())
                .advanceWatermarkTo(base)
                .addElements(TimestampedValue.of(w0, w0Ts))
                .addElements(TimestampedValue.of(w1, w1Ts))
                .advanceWatermarkToInfinity())
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    retry.run().waitUntilFinish();

    // Committing (and reading back) those very files proves the failed run left them intact.
    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(2));
    assertThat(committedThroughMs(snaps.get(0), sinkId), equalTo(windowEndMs(w0Ts)));
    assertThat(committedThroughMs(snaps.get(1), sinkId), equalTo(windowEndMs(w1Ts)));
    assertThat(readRows(t), contains("2:b:y"));
  }

  // ---------------------------------------------------------------------------------------------
  // 10. Inheritance tripwire: committed files carry the snapshot's sequence number.
  // ---------------------------------------------------------------------------------------------

  /**
   * Every published file inherits the snapshot's sequence number; reconstructing files with the
   * writer's (unassigned) sequence number baked in would fail here.
   */
  @Test
  public void committedFilesCarrySnapshotSequenceNumber() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    // An update pair in one commit -> 1 data file + 1 equality-delete file.
    ShardDeltaFiles w0 =
        stage(
            tt,
            10L,
            11L,
            Lists.newArrayList(
                change(1, "a", "x", UPDATE_BEFORE), change(1, "a2", "x2", UPDATE_AFTER)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();
    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    p.run().waitUntilFinish();

    t.refresh();
    Snapshot snap = checkStateNotNull(t.currentSnapshot());
    long snapSeq = snap.sequenceNumber();
    List<DataFile> addedData = Lists.newArrayList(snap.addedDataFiles(t.io()));
    assertThat(addedData, not(empty()));
    for (DataFile f : addedData) {
      assertThat(f.dataSequenceNumber(), equalTo(snapSeq));
    }
    List<DeleteFile> addedDeletes = Lists.newArrayList(snap.addedDeleteFiles(t.io()));
    assertThat(addedDeletes, not(empty()));
    for (DeleteFile f : addedDeletes) {
      assertThat(f.dataSequenceNumber(), equalTo(snapSeq));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 11. Watermark gating: a window is not released before the watermark passes its end.
  // ---------------------------------------------------------------------------------------------

  /**
   * Convergence: exactly one commit for W0 once the watermark passes its end. NOT an
   * eager-committer gate (the result is only observable after the run); that gate is {@link
   * #commitsWindowsInAscendingOrder}'s ancestry-token-order probe.
   */
  @Test
  public void doesNotCommitWindowBeforeWatermarkPasses() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));

    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(30))) // still inside W0
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    p.run().waitUntilFinish();

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(1));
    assertThat(committedThroughMs(snaps.get(0), sinkId), equalTo(windowEndMs(w0Ts)));
  }

  // ---------------------------------------------------------------------------------------------
  // 12. Token key names are PINNED (update compatibility / on-disk contract).
  // ---------------------------------------------------------------------------------------------

  /**
   * The literal token key strings are an on-disk contract with already-written snapshots: renaming
   * them makes every existing table's tokens invisible to recovery (a full replay). Pinned
   * independently of the constants the implementation uses.
   */
  @Test
  public void tokenKeyNamesArePinned() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    ShardDeltaFiles w0 = stage(tt, 10L, 12L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();
    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    p.run().waitUntilFinish();

    t.refresh();
    Map<String, String> summary = checkStateNotNull(t.currentSnapshot()).summary();
    assertThat(summary.get("beam.cdc.sink-id"), equalTo(sinkId));
    assertThat(
        summary.get("beam.cdc.committed-through-ms." + sinkId),
        equalTo(Long.toString(windowEndMs(w0Ts))));
    assertThat(summary.get("beam.cdc.max-committed-seq." + sinkId), equalTo("12"));
  }

  // ---------------------------------------------------------------------------------------------
  // 13. Cross-window sequence-inversion detector.
  // ---------------------------------------------------------------------------------------------

  /**
   * A later window whose min sequence is below an earlier window's committed max is counted on
   * {@code crossWindowSequenceInversions} (benign false positives on disjoint keys).
   */
  @Test
  public void detectsCrossWindowSequenceInversion() {
    TestTable tt = v2Table();
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1)); // W0
    Instant w1Ts = base.plus(Duration.standardSeconds(61)); // W1 (later end)

    // W0 covers seq [10,100]; the LATER W1 covers seq [50,60], a cross-window inversion.
    ShardDeltaFiles w0 = stage(tt, 10L, 100L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    ShardDeltaFiles w1 = stage(tt, 50L, 60L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .addElements(TimestampedValue.of(w1, w1Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(counter(result, "crossWindowSequenceInversions"), equalTo(1L));
  }

  /** In-order sequence ranges across windows must NOT flag an inversion. */
  @Test
  public void noInversionForInOrderSequences() {
    TestTable tt = v2Table();
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    Instant w1Ts = base.plus(Duration.standardSeconds(61));

    ShardDeltaFiles w0 = stage(tt, 5L, 8L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    ShardDeltaFiles w1 = stage(tt, 10L, 20L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .addElements(TimestampedValue.of(w1, w1Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(counter(result, "crossWindowSequenceInversions"), equalTo(0L));
  }

  /**
   * The inversion detector is seeded from the recovered max-committed sequence across a restart; an
   * unseeded fresh run would start blind at MIN and count zero.
   */
  @Test
  public void seedsMaxCommittedSeqAcrossRestartForInversionDetection() {
    TestTable tt = v2Table();
    String sinkId = "sink-" + System.nanoTime();

    // A prior run's commit: committed-through 59999 ms with max sequence 100.
    seedCommittedSnapshot(
        tt, sinkId, 59_999L, 100L, Lists.newArrayList(change(2, "b", "y", INSERT)));

    Instant base = new Instant(0);
    Instant w1Ts = base.plus(Duration.standardSeconds(61)); // W1 = [60s,120s), end 119999 > 59999

    // A LATER window whose min seq (5) is below the recovered max (100): an inversion.
    ShardDeltaFiles w1 = stage(tt, 5L, 8L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w1, w1Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(counter(result, "crossWindowSequenceInversions"), equalTo(1L));
  }

  // ---------------------------------------------------------------------------------------------
  // 14. User snapshot properties appear in the summary alongside the token keys.
  // ---------------------------------------------------------------------------------------------

  /**
   * User snapshot properties are applied to every commit alongside (never instead of) the token
   * keys; user properties are applied FIRST, so the token keys win any collision.
   */
  @Test
  public void userSnapshotPropertiesAppearAlongsideTokenKeys() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();
    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(
            new CommitDeltas(
                catalogConfig(),
                sinkId,
                ImmutableMap.of("pipeline", "my-pipeline", "team", "cdc"),
                null));
    p.run().waitUntilFinish();

    t.refresh();
    Map<String, String> summary = checkStateNotNull(t.currentSnapshot()).summary();
    assertThat(summary.get("pipeline"), equalTo("my-pipeline"));
    assertThat(summary.get("team"), equalTo("cdc"));
    assertThat(summary.get("beam.cdc.sink-id"), equalTo(sinkId));
    assertThat(summary.get("beam.cdc.committed-through-ms." + sinkId), notNullValue());
    assertThat(summary.get("beam.cdc.max-committed-seq." + sinkId), notNullValue());
  }

  // ---------------------------------------------------------------------------------------------
  // Skip-path diagnostics: describeSkippedFiles lists up to 5 orphaned file paths.
  // ---------------------------------------------------------------------------------------------

  /**
   * The skip WARN names the actual orphans (up to five paths then a {@code (… N more)} tail), and a
   * window that wrote only delete files falls through to naming those, with the orphan COUNT
   * agreeing with the description (it used to count data files only).
   */
  @Test
  public void describeSkippedFilesListsUpToFivePathsAndFallsThroughToDeleteFiles() {
    TestTable tt = v2Table();

    // facet: first five data-file paths listed, the sixth omitted with a "(… 1 more)" tail.
    List<ShardDeltaFiles> results = new ArrayList<>();
    List<String> paths = new ArrayList<>();
    for (int i = 1; i <= 6; i++) {
      ShardDeltaFiles s = stage(tt, i, i, Lists.newArrayList(change(i, "n" + i, "d" + i, INSERT)));
      results.add(s);
      assertThat(s.getDataFiles(), hasSize(1));
      paths.add(Iterables.getOnlyElement(dataFilePaths(s)));
    }
    String desc =
        CommitDeltas.describeSkippedFiles(CommitDeltas.WindowedCommit.of(59_999L, results));
    for (int i = 0; i < 5; i++) {
      assertThat(desc, containsString(paths.get(i)));
    }
    assertThat(desc, not(containsString(paths.get(5))));
    assertThat(desc, containsString("(… 1 more)"));

    // facet: a delete-only window (a DELETE for a PK this fresh writer never inserted => an
    // equality delete file, no data file) names its delete files, and the count agrees.
    ShardDeltaFiles deletesOnly =
        stage(tt, 1L, 1L, Lists.newArrayList(change(1, "a", "x", DELETE)));
    assertThat(deletesOnly.getDataFiles(), empty());
    assertThat(deletesOnly.getDeleteFiles(), hasSize(1));

    CommitDeltas.WindowedCommit wc =
        CommitDeltas.WindowedCommit.of(59_999L, ImmutableList.of(deletesOnly));
    assertThat(
        CommitDeltas.describeSkippedFiles(wc),
        containsString(Iterables.getOnlyElement(deleteFilePaths(deletesOnly))));
    assertThat(CommitDeltas.filePaths(wc), hasSize(1));
    assertThat(
        CommitDeltas.filePaths(wc),
        contains(Iterables.getOnlyElement(deleteFilePaths(deletesOnly))));
  }

  // ---------------------------------------------------------------------------------------------
  // Commit volume metrics.
  // ---------------------------------------------------------------------------------------------

  /** A commit records its volume metrics: file/record counts, bytes, snapshots created. */
  @Test
  public void recordsCommitVolumeMetrics() {
    TestTable tt = v2Table();
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));

    // shard0: an update pair for id=1 -> 1 data file + 1 equality delete file.
    ShardDeltaFiles shard0 =
        stage(
            tt,
            1L,
            2L,
            Lists.newArrayList(
                change(1, "a", "x", UPDATE_BEFORE), change(1, "a2", "x2", UPDATE_AFTER)));
    // shard1: INSERT id=2 (1 data file, no delete).
    ShardDeltaFiles shard1 = stage(tt, 1L, 1L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    // shard2: DELETE id=3 with no insert to cancel -> 1 equality delete file.
    ShardDeltaFiles shard2 = stage(tt, 3L, 3L, Lists.newArrayList(change(3, "c", "z", DELETE)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(shard0, w0Ts),
                TimestampedValue.of(shard1, w0Ts),
                TimestampedValue.of(shard2, w0Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(counter(result, "snapshotsCreated"), equalTo(1L));
    assertThat(counter(result, "committedDataFiles"), equalTo(2L));
    assertThat(counter(result, "committedDeleteFiles"), equalTo(2L));
    // 2 data files carry 1 record each (id=1, id=2).
    assertThat(counter(result, "committedRecords"), equalTo(2L));
    assertThat(counter(result, "committedEqualityDeleteRecords"), equalTo(2L));
    assertThat(counter(result, "committedBytes"), greaterThan(0L));
  }

  // ---------------------------------------------------------------------------------------------
  // Sink-id namespacing isolates two sinks writing the same table.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void sinkIdNamespacingIsolatesTwoSinks() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkA = "sinkA-" + System.nanoTime();
    String sinkB = "sinkB-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    long w0End = windowEndMs(w0Ts);

    // sinkA has already committed W0 (its token present); sinkB has not.
    seedCommittedSnapshot(tt, sinkA, w0End, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    int snapsAfterSeed = snapshotsOf(t).size();

    // Feed W0 as sinkB: it must NOT see sinkA's token and so must commit W0.
    ShardDeltaFiles w0ForB = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0ForB, w0Ts))
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkB));
    p.run().waitUntilFinish();

    // sinkB committed a NEW snapshot carrying its own token, not sinkA's.
    assertThat(snapshotsOf(t), hasSize(snapsAfterSeed + 1));
    Snapshot newest = checkStateNotNull(t.currentSnapshot());
    assertThat(committedThroughMs(newest, sinkB), equalTo(w0End));
    assertThat(newest.summary().get("beam.cdc.committed-through-ms." + sinkA), nullValue());
  }

  // ---------------------------------------------------------------------------------------------
  // Timer re-arm: a later window is not stranded after an earlier one commits.
  // ---------------------------------------------------------------------------------------------

  /**
   * After W0 commits, the single commit timer must be re-armed for the still-pending W2 (no further
   * element arrives to arm it), or W2 is stranded forever. Gates re-arm existence only;
   * earliest-targeting is {@link #commitTimerTargetsEarliestPendingWindowNotLatestArrival}.
   */
  @Test
  public void doesNotStrandLaterWindowAfterEarlierCommits() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1)); // W0 = [0,60s)
    Instant w2Ts = base.plus(Duration.standardSeconds(121)); // W2 = [120s,180s); W1 is empty

    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles w2 = stage(tt, 30L, 30L, Lists.newArrayList(change(3, "c", "z", INSERT)));

    // W0 then W2 (W1 empty); the watermark passes W0, then W2 with NO further input; W2 commits
    // only if the timer re-armed.
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .addElements(TimestampedValue.of(w2, w2Ts))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(61))) // just past W0 end
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(181))) // past W2 end, no input
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    p.run().waitUntilFinish();

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(2));
    assertThat(committedThroughMs(snaps.get(0), sinkId), equalTo(windowEndMs(w0Ts)));
    assertThat(committedThroughMs(snaps.get(1), sinkId), equalTo(windowEndMs(w2Ts)));
    assertThat(readRows(t), contains("1:a:x", "3:c:z"));
  }

  /**
   * The single commit timer must always target the EARLIEST uncommitted pending window-end, never
   * the latest arrival; a wrong minimum is invisible to every other assertion in this class. {@link
   * CommitDeltas.OrderedCommitFn} is driven directly because through the full transform the
   * grouping releases a window only after the watermark passes it, so a later window can never
   * arrive behind a pending earlier one: correct targeting produces TWO committing fires, {@code
   * [W0]} then {@code [W2]}; a latest-arrival timer produces a single fire committing both.
   */
  @Test
  public void commitTimerTargetsEarliestPendingWindowNotLatestArrival() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    long w0End = windowEndMs(new Instant(1_000L)); // W0 = [0,60s)
    long w2End = windowEndMs(new Instant(121_000L)); // W2 = [120s,180s)

    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles w2 = stage(tt, 30L, 30L, Lists.newArrayList(change(3, "c", "z", INSERT)));

    List<KV<Long, List<Long>>> fires = new CopyOnWriteArrayList<>();
    CommitDeltas.onFireForTest = (fireWatermark, ends) -> fires.add(KV.of(fireWatermark, ends));

    TestStream<KV<String, CommitDeltas.WindowedCommit>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), CommitDeltas.windowedCommitCoder()))
            .advanceWatermarkTo(new Instant(0))
            .addElements(windowedCommit(tt, w0End, w0)) // earliest arrives first ...
            .addElements(windowedCommit(tt, w2End, w2)) // ... a later one must NOT move the timer
            .advanceWatermarkTo(new Instant(w0End + 1)) // past W0's end only
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(
            ParDo.of(
                new CommitDeltas.OrderedCommitFn(
                    catalogConfig(),
                    sinkId,
                    "runId",
                    ImmutableMap.of(),
                    /* heartbeatMillis= */ 0L,
                    System::currentTimeMillis,
                    /* streaming= */ true)))
        .setCoder(committerOutputCoder());
    p.run().waitUntilFinish();

    List<List<Long>> committing =
        fires.stream()
            .map(KV::getValue)
            .filter(ends -> !ends.isEmpty())
            .collect(Collectors.toList());
    assertThat(committing, contains(contains(w0End), contains(w2End)));
    assertThat(readRows(t), contains("1:a:x", "3:c:z"));
  }

  /** The committer {@code DoFn}'s output coder, which only the full transform sets for it. */
  private static Coder<KV<String, SnapshotInfo>> committerOutputCoder() {
    try {
      return KvCoder.of(
          StringUtf8Coder.of(), SchemaRegistry.createDefault().getSchemaCoder(SnapshotInfo.class));
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
  }

  /** One {@code (dest, window)} commit unit, timestamped at its window end (as the fold does). */
  private static TimestampedValue<KV<String, CommitDeltas.WindowedCommit>> windowedCommit(
      TestTable tt, long windowEndMs, ShardDeltaFiles staged) {
    return TimestampedValue.of(
        KV.of(tt.dest, CommitDeltas.WindowedCommit.of(windowEndMs, ImmutableList.of(staged))),
        new Instant(windowEndMs));
  }

  // ---------------------------------------------------------------------------------------------
  // End-to-end ordering: cross-window equality-delete sequencing produces the correct table.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void endToEndOrderingProducesCorrectTable() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1)); // W0: INSERT id=1
    Instant w1Ts = base.plus(Duration.standardSeconds(61)); // W1: DELETE id=1 (equality)

    // W1's DELETE (a fresh writer) becomes an equality delete that applies to W0's lower-seq data.
    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles w1 = stage(tt, 20L, 20L, Lists.newArrayList(change(1, "a", "x", DELETE)));

    // Deliver out of order (W1 before W0) to prove ordering is by window-end, not arrival.
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w1, w1Ts))
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    p.run().waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(2));
    // W0 committed first, so W1's higher-seq equality delete removes the row; inverted, it stays.
    assertThat(readRows(t), empty());
  }

  // ---------------------------------------------------------------------------------------------
  // Catch-up: a backlog of released windows drains as plain per-window commits in ONE fire.
  // ---------------------------------------------------------------------------------------------

  /**
   * A catch-up fire (the watermark jumps a 20-window backlog at once) drains ALL committable
   * windows in that ONE fire, each as its own plain single-window snapshot with its own token, in
   * ascending order. Run on V3 with an equality delete per window so every commit takes the {@code
   * RowDelta} path.
   */
  @Test
  public void catchUpFireDrainsWholeBacklogAsPlainPerWindowCommits() {
    TestTable tt = v3Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    // Pre-existing foreign snapshot: the drain must land on top of ancestry it does not own.
    WriteResult seed = writeFiles(tt, Lists.newArrayList(change(5, "m", "n", INSERT)));
    AppendFiles append = t.newAppend();
    Arrays.stream(seed.dataFiles()).forEach(append::appendFile);
    append.commit();

    int windows = 20;
    Instant base = new Instant(0);
    List<TimestampedValue<ShardDeltaFiles>> elements = new ArrayList<>();
    List<Long> expectedEnds = new ArrayList<>();
    List<String> expectedRows = new ArrayList<>();
    for (int i = 0; i < windows; i++) {
      Instant ts = base.plus(Duration.standardSeconds(1 + 60L * i));
      // A surviving row plus a DELETE of a never-inserted key => an equality delete => RowDelta.
      ShardDeltaFiles staged =
          stage(
              tt,
              10L * (i + 1),
              10L * (i + 1) + 2,
              Lists.newArrayList(
                  change(i, "n" + i, "d" + i, INSERT), change(1000 + i, "tmp", "tmp", DELETE)));
      elements.add(TimestampedValue.of(staged, ts));
      expectedEnds.add(windowEndMs(ts));
      expectedRows.add(i + ":n" + i + ":d" + i);
    }
    expectedRows.add("5:m:n");

    List<KV<Long, List<Long>>> fires = new CopyOnWriteArrayList<>();
    CommitDeltas.onFireForTest = (fireWatermark, ends) -> fires.add(KV.of(fireWatermark, ends));

    TestStream.Builder<ShardDeltaFiles> stream =
        TestStream.create(filesCoder()).advanceWatermarkTo(base);
    for (TimestampedValue<ShardDeltaFiles> element : elements) {
      stream = stream.addElements(element);
    }
    PCollection<KV<String, SnapshotInfo>> out =
        p.apply(stream.advanceWatermarkToInfinity())
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId));
    // One SnapshotInfo per window; sorted by sequence number they carry the tokens in window order.
    int expectedCount = windows;
    PAssert.that(out)
        .satisfies(
            infos -> {
              List<KV<String, SnapshotInfo>> outputs = Lists.newArrayList(infos);
              assertThat(outputs, hasSize(expectedCount));
              List<SnapshotInfo> bySeq =
                  outputs.stream()
                      .map(KV::getValue)
                      .sorted(Comparator.comparingLong(SnapshotInfo::getSequenceNumber))
                      .collect(Collectors.toList());
              for (int i = 0; i < expectedCount; i++) {
                Map<String, String> summary = checkStateNotNull(bySeq.get(i).getSummary());
                assertThat(
                    summary.get("beam.cdc.committed-through-ms." + sinkId),
                    equalTo(Long.toString(expectedEnds.get(i))));
              }
              return null;
            });
    PipelineResult result = p.run();
    result.waitUntilFinish();

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(windows + 1)); // + the foreign seed
    for (int i = 0; i < windows; i++) {
      assertThat(committedThroughMs(snaps.get(i + 1), sinkId), equalTo(expectedEnds.get(i)));
      assertThat(snaps.get(i).sequenceNumber(), lessThan(snaps.get(i + 1).sequenceNumber()));
    }
    assertThat(readRows(t), equalTo(expectedRows.stream().sorted().collect(Collectors.toList())));

    // Single-fire drain: exactly ONE fire committed windows, all of them, in ascending order.
    List<KV<Long, List<Long>>> committing =
        fires.stream().filter(f -> !f.getValue().isEmpty()).collect(Collectors.toList());
    assertThat(committing, hasSize(1));
    assertThat(committing.get(0).getValue(), equalTo(expectedEnds));
    assertThat(committing.get(0).getKey(), greaterThanOrEqualTo(expectedEnds.get(windows - 1)));
  }

  /** Steady state: a fire with a single committable window commits exactly that window. */
  @Test
  public void singleCommittableWindowCommitsInItsOwnFire() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    List<KV<Long, List<Long>>> fires = new CopyOnWriteArrayList<>();
    CommitDeltas.onFireForTest = (fireWatermark, ends) -> fires.add(KV.of(fireWatermark, ends));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(1));
    List<KV<Long, List<Long>>> committing =
        fires.stream().filter(f -> !f.getValue().isEmpty()).collect(Collectors.toList());
    assertThat(committing, hasSize(1));
    assertThat(committing.get(0).getValue(), contains(windowEndMs(w0Ts)));
  }

  /**
   * A failure on the Nth commit of a catch-up fire halts it exactly at N: the committed prefix
   * keeps its snapshots, the rest stays pending, and a retry run commits the remainder and ONLY the
   * remainder: every window exactly once, the prefix skipped via the recovered table token.
   */
  @Test
  public void catchUpFailureKeepsCommittedPrefixAndRetryCommitsRemainder() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    // Fail the SECOND commit: W0's plain commit has already landed when the fire halts.
    AtomicInteger commits = new AtomicInteger();
    CommitDeltas.preCommitHookForTest =
        () -> {
          if (commits.incrementAndGet() == 2) {
            throw new RuntimeException("injected commit failure (test)");
          }
        };

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    Instant w1Ts = base.plus(Duration.standardSeconds(61));
    Instant w2Ts = base.plus(Duration.standardSeconds(121));

    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles w1 = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    ShardDeltaFiles w2 = stage(tt, 30L, 30L, Lists.newArrayList(change(3, "c", "z", INSERT)));

    // The same backlog stream feeds both the failing run and the retry run.
    TestStream<ShardDeltaFiles> backlog =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(w0, w0Ts),
                TimestampedValue.of(w1, w1Ts),
                TimestampedValue.of(w2, w2Ts))
            .advanceWatermarkToInfinity();

    p.apply(backlog)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));

    Exception e = assertThrows(Exception.class, () -> p.run().waitUntilFinish());
    String trace = Throwables.getStackTraceAsString(e);
    assertThat(trace, containsString("injected commit failure"));
    assertThat(trace, containsString("failed to commit table"));
    // The triage names the failing window (W1, not W0), the committed-prefix semantics, and a
    // pending count covering only what the retry still has to commit.
    assertThat(trace, containsString("at window-end " + windowEndMs(w1Ts)));
    assertThat(trace, containsString("Windows committed earlier in this same fire stay committed"));
    assertThat(trace, containsString("2 pending window(s)"));

    List<Snapshot> afterFailure = snapshotsOf(t);
    assertThat(afterFailure, hasSize(1));
    assertThat(committedThroughMs(afterFailure.get(0), sinkId), equalTo(windowEndMs(w0Ts)));

    // The retry: a fresh run (same sink id, same inputs) with the failure cleared.
    CommitDeltas.preCommitHookForTest = null;
    TestPipeline retry = TestPipeline.create();
    retry.enableAbandonedNodeEnforcement(false);
    retry
        .apply(backlog)
        .apply(Window.into(FixedWindows.of(WINDOW)))
        .apply(new CommitDeltas(catalogConfig(), sinkId));
    PipelineResult retryResult = retry.run();
    retryResult.waitUntilFinish();

    // EXACTLY once each: the retry skipped W0's replay loudly and committed only W1 and W2.
    assertThat(counter(retryResult, "alreadyCommittedWindowsSkipped"), equalTo(1L));
    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(3));
    assertThat(committedThroughMs(snaps.get(0), sinkId), equalTo(windowEndMs(w0Ts)));
    assertThat(committedThroughMs(snaps.get(1), sinkId), equalTo(windowEndMs(w1Ts)));
    assertThat(committedThroughMs(snaps.get(2), sinkId), equalTo(windowEndMs(w2Ts)));
    assertThat(readRows(t), contains("1:a:x", "2:b:y", "3:c:z"));
  }

  // ---------------------------------------------------------------------------------------------
  // Twin parking: two panes of ONE window (same end, distinct files) commit once.
  // ---------------------------------------------------------------------------------------------

  /**
   * Two panes of the SAME window (early trigger: same end, distinct files) seen by one fire commit
   * exactly ONE snapshot; the losing twin is skipped loudly, its file counted as an orphan.
   */
  @Test
  public void duplicateWindowPanesCommitOnceAndSkipTwinLoudly() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));

    ShardDeltaFiles pane0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    ShardDeltaFiles pane1 = stage(tt, 11L, 11L, Lists.newArrayList(change(2, "b", "y", INSERT)));

    // Two SEPARATE addElements: the early trigger fires one pane per element, so the committer
    // receives two WindowedCommits with the SAME end.
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(pane0, w0Ts))
            .addElements(TimestampedValue.of(pane1, w0Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(
                Window.<ShardDeltaFiles>into(FixedWindows.of(WINDOW))
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(1));
    assertThat(committedThroughMs(snaps.get(0), sinkId), equalTo(windowEndMs(w0Ts)));
    assertThat(readRows(t), contains("1:a:x"));
    // The twin's file IS a genuine orphan: distinct files, not among the winner's.
    assertThat(counter(result, "alreadyCommittedWindowsSkipped"), greaterThanOrEqualTo(1L));
    assertThat(counter(result, "orphanFiles"), equalTo(1L));
  }

  /**
   * A genuine redelivery (the SAME files twice for one window end) commits once and is skipped
   * loudly, with NOTHING counted as an orphan. Cannot distinguish park-then-skip from
   * skip-during-triage, which look identical when the commit succeeds; that ORDER is pinned by
   * {@link #parkedTwinIsNotDeclaredCommittedWhenTheCommitFails}. Driven directly: through the full
   * transform the grouping releases a window once, so a redelivery cannot reach the committer.
   */
  @Test
  public void redeliveredDuplicateWindowCommitsOnceAndSkipsTwinAfterTheCommit() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    long w0End = windowEndMs(new Instant(1_000L)); // W0 = [0,60s)
    ShardDeltaFiles staged = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<KV<String, CommitDeltas.WindowedCommit>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), CommitDeltas.windowedCommitCoder()))
            .advanceWatermarkTo(new Instant(0))
            .addElements(windowedCommit(tt, w0End, staged))
            .addElements(windowedCommit(tt, w0End, staged)) // the SAME files again
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(
                ParDo.of(
                    new CommitDeltas.OrderedCommitFn(
                        catalogConfig(),
                        sinkId,
                        "runId",
                        ImmutableMap.of(),
                        /* heartbeatMillis= */ 0L,
                        System::currentTimeMillis,
                        /* streaming= */ true)))
            .setCoder(committerOutputCoder())
            .getPipeline()
            .run();
    result.waitUntilFinish();

    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(1));
    assertThat(committedThroughMs(snaps.get(0), sinkId), equalTo(w0End));
    assertThat(readRows(t), contains("1:a:x"));
    assertThat(counter(result, "alreadyCommittedWindowsSkipped"), equalTo(1L));
    // A redelivery names the very files the twin committed: live table data, never an orphan.
    assertThat(counter(result, "orphanFiles"), equalTo(0L));
    expectedLogs.verifyInfo("it is a pure redelivery and nothing is orphaned");
  }

  /**
   * The discriminating half: with the commit FAILING, the same-end twin must NOT have been declared
   * already-committed: {@code skipSameFireDuplicate}'s WARN asserts rows are in the table, so it
   * must come after the commit, an ordering only a failure makes observable. The second half
   * re-runs without the failure so the {@code verifyNotLogged} cannot pass vacuously.
   */
  @Test
  public void parkedTwinIsNotDeclaredCommittedWhenTheCommitFails() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();
    CommitDeltas.preCommitHookForTest =
        () -> {
          throw new RuntimeException("injected commit failure (test)");
        };

    long w0End = windowEndMs(new Instant(1_000L)); // W0 = [0,60s)
    ShardDeltaFiles staged = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<KV<String, CommitDeltas.WindowedCommit>> stream =
        TestStream.create(KvCoder.of(StringUtf8Coder.of(), CommitDeltas.windowedCommitCoder()))
            .advanceWatermarkTo(new Instant(0))
            .addElements(windowedCommit(tt, w0End, staged))
            .addElements(windowedCommit(tt, w0End, staged)) // the same-end twin
            .advanceWatermarkToInfinity();

    p.apply(stream)
        .apply(
            ParDo.of(
                new CommitDeltas.OrderedCommitFn(
                    catalogConfig(),
                    sinkId,
                    "runId",
                    ImmutableMap.of(),
                    /* heartbeatMillis= */ 0L,
                    System::currentTimeMillis,
                    /* streaming= */ true)))
        .setCoder(committerOutputCoder());

    Exception e = assertThrows(Exception.class, () -> p.run().waitUntilFinish());
    assertThat(Throwables.getStackTraceAsString(e), containsString("injected commit failure"));

    assertThat(snapshotsOf(t), empty());
    // The assertion that fails if the twin is skipped during triage instead of after the commit.
    expectedLogs.verifyNotLogged("already published a window with the same end");

    // Prove the message reachable from this exact input once the commit succeeds.
    CommitDeltas.preCommitHookForTest = null;
    TestPipeline retry = TestPipeline.create();
    retry.enableAbandonedNodeEnforcement(false);
    retry
        .apply(stream)
        .apply(
            ParDo.of(
                new CommitDeltas.OrderedCommitFn(
                    catalogConfig(),
                    sinkId,
                    "runId",
                    ImmutableMap.of(),
                    /* heartbeatMillis= */ 0L,
                    System::currentTimeMillis,
                    /* streaming= */ true)))
        .setCoder(committerOutputCoder());
    retry.run().waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(1));
    expectedLogs.verifyInfo("already published a window with the same end");
  }

  // ---------------------------------------------------------------------------------------------
  // Idle token-refresh heartbeat.
  // ---------------------------------------------------------------------------------------------

  /**
   * With {@code tokenHeartbeatMillis} set, an idle destination emits ONE empty append re-carrying
   * the three token keys with the SAME committed-through value (keeping the token snapshot young
   * against {@code expire_snapshots}) and re-stamps the run-spec pin, which the first commit's own
   * files seeded. The injected {@link CommitDeltas.Clock} skews "now" past the interval because
   * {@code TestStream} advances processing time only virtually.
   */
  @Test
  public void idleHeartbeatRecommitsTokenAndReStampsRunSpec() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();
    CommitDeltas.Clock skewedClock =
        () -> System.currentTimeMillis() + Duration.standardMinutes(10).getMillis();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    long w0End = windowEndMs(w0Ts);
    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(61))) // W0 commits; then idle
            .advanceProcessingTime(Duration.standardSeconds(90)) // one 60s interval of idleness
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(
                new CommitDeltas(catalogConfig(), sinkId, null, 60_000L, "runId-n")
                    .withClockForTest(skewedClock))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(counter(result, "heartbeatCommits"), equalTo(1L));
    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(2)); // W0's commit + one heartbeat
    Snapshot heartbeat = snaps.get(1);

    // facet: an EMPTY append re-carrying the three token keys with the SAME committed-through.
    assertThat(Lists.newArrayList(heartbeat.addedDataFiles(t.io())), empty());
    assertThat(
        heartbeat.summary().get("beam.cdc.committed-through-ms." + sinkId),
        equalTo(Long.toString(w0End)));
    assertThat(heartbeat.summary().get("beam.cdc.max-committed-seq." + sinkId), equalTo("10"));
    assertThat(heartbeat.summary().get("beam.cdc.sink-id"), equalTo(sinkId));

    // facet: W0's commit stamped the run-spec pin and the heartbeat re-stamped it from pin state,
    // so the newest ancestry stamp stays this run's through idle.
    String stamp = "runId-n:" + t.spec().specId();
    assertThat(snaps.get(0).summary().get("beam.cdc.run-spec." + sinkId), equalTo(stamp));
    assertThat(snaps.get(1).summary().get("beam.cdc.run-spec." + sinkId), equalTo(stamp));
  }

  /**
   * No heartbeat while a window is pending: a real commit is imminent. An early trigger delivers
   * the pane before the watermark passes, and a seeded prior commit sets last-committed state (the
   * other idle guard), isolating the pending-bag guard.
   */
  @Test
  public void heartbeatSkippedWhilePendingWindowsExist() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();
    CommitDeltas.Clock skewedClock =
        () -> System.currentTimeMillis() + Duration.standardMinutes(10).getMillis();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    Instant w1Ts = base.plus(Duration.standardSeconds(61));

    seedCommittedSnapshot(
        tt, sinkId, windowEndMs(w0Ts), 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    int snapsAfterSeed = snapshotsOf(t).size();
    ShardDeltaFiles w1 = stage(tt, 20L, 20L, Lists.newArrayList(change(2, "b", "y", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w1, w1Ts)) // early pane => pending, not committable
            .advanceProcessingTime(Duration.standardSeconds(90)) // heartbeat fires: must skip
            .advanceWatermarkToInfinity(); // then W1 commits normally

    PipelineResult result =
        p.apply(stream)
            .apply(
                Window.<ShardDeltaFiles>into(FixedWindows.of(WINDOW))
                    .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                    .withAllowedLateness(Duration.ZERO)
                    .discardingFiredPanes())
            .apply(
                new CommitDeltas(catalogConfig(), sinkId, null, 60_000L)
                    .withClockForTest(skewedClock))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    // No token-refresh; W1 then committed normally as a real (non-empty) snapshot.
    assertThat(counter(result, "heartbeatCommits"), equalTo(0L));
    List<Snapshot> snaps = snapshotsOf(t);
    assertThat(snaps, hasSize(snapsAfterSeed + 1));
    Snapshot newest = checkStateNotNull(t.currentSnapshot());
    assertThat(committedThroughMs(newest, sinkId), equalTo(windowEndMs(w1Ts)));
    assertThat(Lists.newArrayList(newest.addedDataFiles(t.io())), not(empty()));
  }

  /** With no {@code tokenHeartbeatMillis} configured, idleness never produces a heartbeat. */
  @Test
  public void heartbeatOffByDefaultNeverCommitsTokenRefresh() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "sink-" + System.nanoTime();

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    ShardDeltaFiles w0 = stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w0, w0Ts))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(61))) // W0 commits; then idle
            .advanceProcessingTime(Duration.standardSeconds(90)) // idleness with heartbeat OFF
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId)) // no heartbeat configured
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(counter(result, "heartbeatCommits"), equalTo(0L));
    assertThat(snapshotsOf(t), hasSize(1));
  }

  // ---------------------------------------------------------------------------------------------
  // 24. Per-file partition-spec reconstruction at the COMMITTER.
  // ---------------------------------------------------------------------------------------------

  /**
   * One window whose shards were written under DIFFERENT partition specs (spec evolved mid-run:
   * pinned workers on the old, fresh resolvers on the new) commits each file under the spec it was
   * written with. Pins {@code reconstructFiles}' commit-side spec resolution: narrowing it from
   * {@code table.specs()} to {@code table.spec()} passed the whole suite before this test.
   */
  @Test
  public void oneWindowCommitsFilesUnderTheirOwnPartitionSpecs() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "specs-" + System.nanoTime();

    // Shard A: written against the original (unpartitioned) spec 0.
    ShardDeltaFiles shardSpec0 =
        stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    assertThat(shardSpec0.getDataFiles(), hasSize(1));

    // The operator evolves the partition spec between the write and the commit.
    t.updateSpec().addField(Expressions.bucket("id", 4)).commit();
    t.refresh();
    assertThat(t.spec().specId(), equalTo(1));

    // Shard B: written against the evolved spec 1, in the same window.
    ShardDeltaFiles shardSpec1 =
        stage(tt, 11L, 11L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    assertThat(shardSpec1.getDataFiles(), hasSize(1));
    String spec0Path = Iterables.getOnlyElement(dataFilePaths(shardSpec0));
    String spec1Path = Iterables.getOnlyElement(dataFilePaths(shardSpec1));

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(shardSpec0, w0Ts), TimestampedValue.of(shardSpec1, w0Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(1));
    assertThat(readRows(t), contains("1:a:x", "2:b:y"));

    Map<String, DataFile> committedByPath = new HashMap<>();
    for (DataFile file : checkStateNotNull(t.currentSnapshot()).addedDataFiles(t.io())) {
      committedByPath.put(file.location(), file);
    }
    DataFile committedSpec0 = checkStateNotNull(committedByPath.get(spec0Path));
    DataFile committedSpec1 = checkStateNotNull(committedByPath.get(spec1Path));
    assertThat(committedSpec0.specId(), equalTo(0));
    assertThat(committedSpec1.specId(), equalTo(1));
    // Shard A's tuple stayed EMPTY rather than being read as the evolved 1-field tuple.
    assertThat(committedSpec0.partition().size(), equalTo(0));
    assertThat(committedSpec1.partition().size(), equalTo(1));
  }

  // ---------------------------------------------------------------------------------------------
  // 25. Spec pin: mixed-spec windows commit but WARN iff they carry an equality delete.
  // ---------------------------------------------------------------------------------------------

  /**
   * A window mixing spec ids while carrying an equality delete (which applies only to data files of
   * its own spec/partition) commits anyway but WARNs and counts {@code specMismatchedWindows}.
   */
  @Test
  public void specMismatchedWindowWarnsAndCommits() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "mixed-" + System.nanoTime();

    // Shard A under spec 0, with a cross-commit DELETE => an equality-delete file.
    ShardDeltaFiles shardSpec0 =
        stage(
            tt,
            10L,
            11L,
            Lists.newArrayList(change(1, "a", "x", INSERT), change(9, "z", "w", DELETE)));
    assertThat(shardSpec0.getDeleteFiles(), hasSize(1));

    t.updateSpec().addField(Expressions.bucket("id", 4)).commit();
    t.refresh();

    // Shard B under the evolved spec 1, in the same window.
    ShardDeltaFiles shardSpec1 =
        stage(tt, 12L, 12L, Lists.newArrayList(change(2, "b", "y", INSERT)));

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(shardSpec0, w0Ts), TimestampedValue.of(shardSpec1, w0Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(1));
    assertThat(readRows(t), contains("1:a:x", "2:b:y"));
    assertThat(counter(result, "specMismatchedWindows"), equalTo(1L));
    expectedLogs.verifyWarn("rewrite_data_files");
  }

  /** Mixed spec ids WITHOUT an equality delete are harmless: counter and WARN stay silent. */
  @Test
  public void dataOnlyMixedSpecWindowStaysSilent() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "mixed-silent-" + System.nanoTime();

    ShardDeltaFiles shardSpec0 =
        stage(tt, 10L, 10L, Lists.newArrayList(change(1, "a", "x", INSERT)));
    t.updateSpec().addField(Expressions.bucket("id", 4)).commit();
    t.refresh();
    ShardDeltaFiles shardSpec1 =
        stage(tt, 11L, 11L, Lists.newArrayList(change(2, "b", "y", INSERT)));
    assertThat(shardSpec0.getDeleteFiles(), empty());
    assertThat(shardSpec1.getDeleteFiles(), empty());

    Instant base = new Instant(0);
    Instant w0Ts = base.plus(Duration.standardSeconds(1));
    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(
                TimestampedValue.of(shardSpec0, w0Ts), TimestampedValue.of(shardSpec1, w0Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(1));
    assertThat(readRows(t), contains("1:a:x", "2:b:y"));
    assertThat(counter(result, "specMismatchedWindows"), equalTo(0L));
    expectedLogs.verifyNotLogged("rewrite_data_files");
  }

  /**
   * An in-place update regenerates the runId and the fleet re-resolves onto the current spec: the
   * committer re-pins rather than WARN forever against the previous run's spec, and the second
   * window pins that the updated pin persisted across fires.
   */
  @Test
  public void committerRePinsOnRunIdChange() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "repin-" + System.nanoTime();

    // The previous run (runId A) committed W0 under spec 0.
    Instant base = new Instant(0);
    seedCommittedSnapshot(
        tt,
        sinkId,
        windowEndMs(base.plus(Duration.standardSeconds(1))),
        10L,
        Lists.newArrayList(change(1, "a", "x", INSERT)));
    int snapsAfterSeed = snapshotsOf(t).size();

    // The rebuilt fleet (runId B) writes uniformly under the evolved spec 1, equality deletes
    // included, which WOULD warn if the committer kept a spec-0 pin.
    t.updateSpec().addField(Expressions.bucket("id", 4)).commit();
    t.refresh();
    ShardDeltaFiles w1 =
        stage(
            tt,
            20L,
            21L,
            Lists.newArrayList(change(2, "b", "y", INSERT), change(8, "q", "q", DELETE)));
    ShardDeltaFiles w2 =
        stage(
            tt,
            30L,
            31L,
            Lists.newArrayList(change(3, "c", "z", INSERT), change(9, "q", "q", DELETE)));
    Instant w1Ts = base.plus(Duration.standardSeconds(61));
    Instant w2Ts = base.plus(Duration.standardSeconds(121));

    TestStream<ShardDeltaFiles> stream =
        TestStream.create(filesCoder())
            .advanceWatermarkTo(base)
            .addElements(TimestampedValue.of(w1, w1Ts))
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(130))) // W1 commits, re-pin
            .addElements(TimestampedValue.of(w2, w2Ts))
            .advanceWatermarkToInfinity();

    PipelineResult result =
        p.apply(stream)
            .apply(Window.into(FixedWindows.of(WINDOW)))
            .apply(new CommitDeltas(catalogConfig(), sinkId, null, null, "runId-b"))
            .getPipeline()
            .run();
    result.waitUntilFinish();

    assertThat(snapshotsOf(t), hasSize(snapsAfterSeed + 2));
    assertThat(counter(result, "specMismatchedWindows"), equalTo(0L));
    expectedLogs.verifyNotLogged("rewrite_data_files");
    assertThat(readRows(t), contains("1:a:x", "2:b:y", "3:c:z"));
  }

  // ---------------------------------------------------------------------------------------------
  // 26. Run-spec stamp: written with the tokens, read back runId-gated and parse-tolerant.
  // ---------------------------------------------------------------------------------------------

  /** A {@link CommitToken} for driving the stamp directly (counters no-op outside a pipeline). */
  private static CommitToken token(String sinkId, String runId) {
    return new CommitToken(
        sinkId,
        runId,
        Metrics.counter(CommitDeltasTest.class, "tokenParseFailures"),
        Metrics.counter(CommitDeltasTest.class, "suspectedTokenExpiry"));
  }

  /**
   * {@code writeTo} writes {@code beam.cdc.run-spec.<sinkId>} = {@code <runId>:<specId>} (literal
   * strings on purpose: the on-disk contract, like the token keys) and {@code readRunSpec} under
   * the same runId returns the spec id.
   */
  @Test
  public void runSpecStampRoundTrips() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "stamp-" + System.nanoTime();

    AppendFiles op = t.newAppend();
    token(sinkId, "runId-n").writeTo(op, 123L, 5L, 7);
    op.commit();
    t.refresh();

    assertThat(
        checkStateNotNull(t.currentSnapshot()).summary().get("beam.cdc.run-spec." + sinkId),
        equalTo("runId-n:7"));
    assertThat(CommitToken.readRunSpec(t, sinkId, "runId-n"), equalTo(7));
  }

  /**
   * An absent stamp, an unparseable spec id, and another run's stamp (the newest stamp's runId
   * gates the whole read) all read as no stamp, never a throw.
   */
  @Test
  public void runSpecStampReadsAsNoStampWhenAbsentGarbageOrForeignRunId() {
    TestTable tt = v2Table();
    Table t = tt.table;
    String sinkId = "stamp-" + System.nanoTime();

    // facet: no snapshot at all, then a snapshot without the key.
    assertThat(CommitToken.readRunSpec(t, sinkId, "runId-n"), nullValue());
    t.newAppend().commit();
    t.refresh();
    assertThat(CommitToken.readRunSpec(t, sinkId, "runId-n"), nullValue());

    // facet: the right runId with an unparseable spec id.
    t.newAppend().set("beam.cdc.run-spec." + sinkId, "runId-n:banana").commit();
    t.refresh();
    assertThat(CommitToken.readRunSpec(t, sinkId, "runId-n"), nullValue());

    // facet: a real stamp read under another run's id.
    AppendFiles op = t.newAppend();
    token(sinkId, "runId-n").writeTo(op, 123L, 5L, 7);
    op.commit();
    t.refresh();
    assertThat(CommitToken.readRunSpec(t, sinkId, "runId-m"), nullValue());
  }
}
