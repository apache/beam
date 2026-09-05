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
package org.apache.beam.runners.spark.structuredstreaming.io.streaming;

import static org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset.COL_EVENT_TS;
import static org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset.COL_PAYLOAD;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import scala.reflect.ClassTag;

/**
 * Tests for the Spark 4 DataSourceV2 micro-batch source wrapping a Beam {@link UnboundedSource}.
 *
 * <p>Termination note: the epoch offsets of this source never settle, {@code
 * StreamingQuery.processAllAvailable()} would therefore block forever. Every test here drives the
 * query with {@code Trigger.ProcessingTime(100)} and stops it explicitly once the expected result
 * arrived or the poll deadline expired.
 */
@Category(StreamingTest.class)
@RunWith(JUnit4.class)
public class BeamMicroBatchSourceTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder temp = new TemporaryFolder();

  private static final AtomicInteger QUERY_COUNTER = new AtomicInteger();

  /** Rows collected per query name by {@link #startCollecting}, driver side. */
  private static final Map<String, List<Row>> COLLECTED = new ConcurrentHashMap<>();

  /** Rows per micro-batch per query name, see {@link #startCollectingBatches}, driver side. */
  private static final Map<String, List<List<Row>>> BATCHES = new ConcurrentHashMap<>();

  /** 2023-11-14T22:13:20Z, a plain modern timestamp with no rebase or DST subtleties. */
  private static final long BASE_MILLIS = 1_700_000_000_000L;

  private static final long INTERVAL_MILLIS = 1_000L;

  private static final long POLL_TIMEOUT_MILLIS = 120_000L;

  @After
  public void tearDown() {
    BeamReaderCache.invalidateAll();
    COLLECTED.clear();
    BATCHES.clear();
    ShardedListSource.FINALIZED.clear();
  }

  /**
   * THE TOP RISK OF THE POC: does Spark's {@code EventTimeWatermark} logical node survive a typed
   * transformation producing a Beam typed dataset, so a later stateful operator can still see it?
   *
   * <p>Verdict, observed on Spark 4: it does. The node stays at the bottom of both the logical and
   * the analyzed plan through one and through two typed maps, {@code DeserializeToObject /
   * MapElements / SerializeFromObject} are simply stacked on top of it.
   *
   * <p>One nuance the translators must be aware of: the per attribute delay marker (rendered as
   * {@code eventTimestamp#1-T1000ms} in the analyzed plan) only travels with the timestamp
   * attribute itself. Once a typed map projects the timestamp column away, downstream attributes
   * carry no marker. Operators that read the marker off an attribute, for example {@code
   * groupBy(window(...))} or a stream to stream join, would therefore not see it. Operators that
   * read the query wide watermark, which is what {@code transformWithState} in {@code
   * TimeMode.EventTime} does, are unaffected, see {@link
   * #testWatermarkIsTrackedAtRuntimeAfterTypedMap}.
   */
  @Test
  public void testEventTimeWatermarkSurvivesTypedMap() {
    Dataset<Row> rows = rows(4, 1_000L);
    assertTrue("source dataset must be streaming", rows.isStreaming());

    assertWatermark("directly after withWatermark, logical plan", logical(rows));
    assertWatermark("directly after withWatermark, analyzed plan", analyzed(rows));

    // A typed map producing a Beam-ish (opaque bytes) dataset, exactly what the read translator
    // will do to turn rows into WindowedValue bytes.
    Dataset<byte[]> typed =
        rows.map((MapFunction<Row, byte[]>) row -> row.getAs(COL_PAYLOAD), Encoders.BINARY());

    assertWatermark("after a typed map, logical plan", logical(typed));
    assertWatermark("after a typed map, analyzed plan", analyzed(typed));

    // And once more after a second typed stage, mimicking chained operators.
    Dataset<byte[]> chained =
        typed.map((MapFunction<byte[], byte[]>) bytes -> bytes, Encoders.BINARY());

    assertWatermark("after two chained typed maps, logical plan", logical(chained));
    assertWatermark("after two chained typed maps, analyzed plan", analyzed(chained));
  }

  /**
   * Runtime counterpart of the plan inspection above, the watermark must actually be tracked by the
   * running query, not merely present as a plan node.
   */
  @Test
  public void testWatermarkIsTrackedAtRuntimeAfterTypedMap() throws Exception {
    Dataset<Row> rows = rows(8, 0L);
    Dataset<byte[]> typed =
        rows.map((MapFunction<Row, byte[]>) row -> row.getAs(COL_PAYLOAD), Encoders.BINARY());

    String queryName = "beam_wm_" + QUERY_COUNTER.incrementAndGet();
    StreamingQuery query = startDiscarding(typed, queryName);
    try {
      String watermark = awaitWatermark(query);
      assertNotNull("query never reported an event time watermark", watermark);
      assertFalse(
          "watermark never advanced past the epoch, it is not being tracked: " + watermark,
          watermark.startsWith("1970-"));
    } finally {
      stopQuietly(query);
    }
  }

  /** Reads a finite set of elements through the DSv2 source and checks payloads and timestamps. */
  @Test
  public void testReadsElementsFromUnboundedSource() throws Exception {
    int count = 8;
    Dataset<Row> rows = rows(count, 0L);

    String queryName = "beam_read_" + QUERY_COUNTER.incrementAndGet();
    StreamingQuery query = startCollecting(rows, queryName);
    List<Row> collected;
    try {
      collected = awaitRows(queryName, count);
    } finally {
      stopQuietly(query);
    }

    assertEquals("unexpected number of rows", count, collected.size());

    Coder<WindowedValue<String>> coder = coder();
    List<String> values = new ArrayList<>();
    for (Row row : collected) {
      byte[] payload = row.getAs(COL_PAYLOAD);
      Timestamp eventTs = row.getAs(COL_EVENT_TS);
      WindowedValue<String> windowedValue = CoderUtils.decodeFromByteArray(coder, payload);
      values.add(windowedValue.getValue());
      assertEquals(
          "eventTimestamp column must match the timestamp inside the encoded WindowedValue",
          windowedValue.getTimestamp().getMillis(),
          eventTs.getTime());
      assertEquals(
          "elements are read into the global window",
          Collections.singletonList(GlobalWindow.INSTANCE),
          new ArrayList<>(windowedValue.getWindows()));
    }

    Collections.sort(values);
    List<String> expected = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      expected.add(element(i));
    }
    assertEquals(expected, values);
  }

  /** The default record limit of -1 means unlimited, an available source drains in one batch. */
  @Test
  public void testUnlimitedRecordsPerBatchByDefault() throws Exception {
    int count = 2500;
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.create().as(SparkStructuredStreamingPipelineOptions.class);
    options.setWatermarkDelayMillis(0L);
    options.setMaxBatchDurationMillis(5_000L);
    Dataset<Row> rows =
        UnboundedSourceDataset.of(
            SESSION.getSession(), new ListSource(count), coder(), options, "Read(ListSource)");

    String queryName = "beam_nolimit_" + QUERY_COUNTER.incrementAndGet();
    List<Long> batchSizes = Collections.synchronizedList(new ArrayList<>());
    StreamingQuery query =
        rows.writeStream()
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>)
                    (batch, batchId) -> {
                      long size = batch.count();
                      if (size > 0) {
                        batchSizes.add(size);
                      }
                    })
            .queryName(queryName)
            .outputMode("append")
            .option("checkpointLocation", temp.newFolder(queryName).getAbsolutePath())
            .trigger(Trigger.ProcessingTime(100))
            .start();
    try {
      long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MILLIS;
      while (System.currentTimeMillis() < deadline && batchSizes.isEmpty()) {
        Thread.sleep(100L);
      }
    } finally {
      stopQuietly(query);
    }
    assertEquals(
        "without a limit all elements must arrive in the first non-empty micro-batch",
        Collections.singletonList((long) count),
        new ArrayList<>(batchSizes));
  }

  /** The offset is an opaque, strictly increasing epoch counter, its JSON is the bare number. */
  @Test
  public void testEpochOffsetRoundTrip() {
    BeamOffset offset = new BeamOffset(42L);
    assertEquals("42", offset.json());
    assertEquals(42L, BeamOffset.fromJson("42").epoch());
    assertEquals(0L, BeamOffset.ZERO.epoch());
    assertEquals(new BeamOffset(7L), new BeamOffset(7L));
    assertThrows(IllegalArgumentException.class, () -> BeamOffset.fromJson("x"));
  }

  @Test
  public void testEpochFastForwardsPastDeserializedOffset() throws Exception {
    BeamMicroBatchStream<?> stream = newStream(temp.newFolder("ff-offset").getAbsolutePath());
    stream.deserializeOffset("7");
    BeamOffset next = (BeamOffset) stream.latestOffset();
    assertTrue("latestOffset must move past the replayed epoch 7, got " + next, next.epoch() > 7L);
  }

  @Test
  public void testEpochFastForwardsPastPlannedOffsets() throws Exception {
    BeamMicroBatchStream<?> stream = newStream(temp.newFolder("ff-plan").getAbsolutePath());
    InputPartition[] partitions =
        stream.planInputPartitions(new BeamOffset(3L), new BeamOffset(9L));
    assertTrue("at least one partition expected", partitions.length > 0);
    BeamOffset next = (BeamOffset) stream.latestOffset();
    assertTrue("latestOffset must move past the planned epoch 9, got " + next, next.epoch() > 9L);
  }

  /** The batch quota is divided over the splits, remainder first, every split gets at least one. */
  @Test
  public void testSplitQuotas() {
    assertArrayEquals(new long[] {1, 1, 1, 1, 1, 1, 1, 1}, BeamMicroBatchStream.splitQuotas(3, 8));
    assertArrayEquals(new long[] {4, 3, 3}, BeamMicroBatchStream.splitQuotas(10, 3));
    assertArrayEquals(new long[] {5, 5}, BeamMicroBatchStream.splitQuotas(10, 2));
    assertArrayEquals(new long[] {0, 0, 0}, BeamMicroBatchStream.splitQuotas(0, 3));
    assertArrayEquals(new long[] {-1, -1, -1}, BeamMicroBatchStream.splitQuotas(-1, 3));
    long[] many = BeamMicroBatchStream.splitQuotas(1, 200);
    assertEquals(200, many.length);
    for (long quota : many) {
      assertEquals(1L, quota);
    }
  }

  /** Rendezvous hashing: a joining executor only takes splits for itself, order does not matter. */
  @Test
  public void testSplitAssignmentIsStableWhenExecutorJoins() {
    List<String> three = Arrays.asList("executor_a_1", "executor_b_2", "executor_c_3");
    List<String> shuffled = Arrays.asList("executor_c_3", "executor_a_1", "executor_b_2");
    List<String> four = new ArrayList<>(three);
    four.add("executor_d_4");
    int splits = 200;
    int moved = 0;
    for (int split = 0; split < splits; split++) {
      String before = BeamMicroBatchStream.assign(split, three);
      String after = BeamMicroBatchStream.assign(split, four);
      assertTrue(three.contains(before));
      assertEquals(before, BeamMicroBatchStream.assign(split, shuffled));
      if (!after.equals(before)) {
        assertEquals("split " + split + " moved to an old executor", "executor_d_4", after);
        moved++;
      }
    }
    assertTrue("the new executor took no split", moved > 0);
    assertTrue("the new executor took every split", moved < splits);
    assertEquals(
        "executor_a_1", BeamMicroBatchStream.assign(7, Collections.singletonList("executor_a_1")));
  }

  /** The record limit of a micro-batch is a total over all splits, not a per split allowance. */
  @Test
  public void testMaxRecordsPerBatchIsSharedAcrossSplits() throws Exception {
    int shards = 2;
    int count = 30;
    long limit = 10L;
    String queryName = "beam_shared_limit_" + QUERY_COUNTER.incrementAndGet();
    Dataset<Row> rows = shardedRows(queryName, shards, count, limit);
    StreamingQuery query = startCollectingBatches(rows, queryName, temp.newFolder(queryName));
    try {
      await("all rows", () -> values(batches(queryName)).size() >= count);
    } finally {
      stopQuietly(query);
    }

    List<Integer> sizes = new ArrayList<>();
    for (List<Row> batch : batches(queryName)) {
      if (!batch.isEmpty()) {
        sizes.add(batch.size());
      }
    }
    assertFalse("no rows arrived", sizes.isEmpty());
    assertTrue("first batch exceeds the shared limit: " + sizes, sizes.get(0) <= limit);
    for (int size : sizes) {
      assertTrue("batch exceeds the shared limit: " + sizes, size <= limit);
    }
    List<String> values = values(batches(queryName));
    assertEquals(count, values.size());
    assertEquals(ShardedListSource.elements(shards, count), new HashSet<>(values));
  }

  /**
   * A restart resumes every split from the durable mark of the last committed batch, replaying at
   * most the one uncommitted batch.
   */
  @Test
  public void testRestartResumesFromCommittedMark() throws Exception {
    int shards = 2;
    int count = 80;
    long limit = 4L;
    File checkpointDir = temp.newFolder("restart");
    Set<String> all = ShardedListSource.elements(shards, count);

    String first = "beam_restart_a_" + QUERY_COUNTER.incrementAndGet();
    StreamingQuery query =
        startCollectingBatches(shardedRows(first, shards, count, limit), first, checkpointDir);
    try {
      await("two commits", () -> committedBatchIds(checkpointDir).size() >= 2);
    } finally {
      stopQuietly(query);
    }
    BeamReaderCache.invalidateAll();
    List<String> firstValues = values(batches(first));

    String second = "beam_restart_b_" + QUERY_COUNTER.incrementAndGet();
    query =
        startCollectingBatches(shardedRows(second, shards, count, limit), second, checkpointDir);
    try {
      await(
          "union of both runs",
          () -> {
            Set<String> union = new HashSet<>(firstValues);
            union.addAll(values(batches(second)));
            return union.containsAll(all);
          });
    } finally {
      stopQuietly(query);
    }
    List<String> secondValues = values(batches(second));

    Set<String> union = new HashSet<>(firstValues);
    union.addAll(secondValues);
    assertEquals(all, union);
    assertTrue(
        "more than the uncommitted batch replayed: "
            + firstValues.size()
            + " + "
            + secondValues.size()
            + " rows",
        firstValues.size() + secondValues.size() <= count + limit);
    for (int shard = 0; shard < shards; shard++) {
      int min = Integer.MAX_VALUE;
      for (String value : secondValues) {
        if (ShardedListSource.shardOf(value) == shard) {
          min = Math.min(min, ShardedListSource.indexOf(value));
        }
      }
      assertTrue("run 2 delivered nothing for shard " + shard, min < Integer.MAX_VALUE);
      assertTrue("run 2 restarted shard " + shard + " from element 0", min > 0);
    }
  }

  /**
   * Every finalized position of a split is at most the position in that split's mark at the end
   * epoch of the highest committed batch, so no mark is finalized before Spark commits its batch.
   */
  @Test
  public void testMarksAreFinalizedOnlyAfterSparkCommit() throws Exception {
    int shards = 2;
    // Never exhausted while the test runs, positions strictly increase with the epoch.
    int count = 4_000;
    File checkpointDir = temp.newFolder("finalize");
    String queryName = "beam_finalize_" + QUERY_COUNTER.incrementAndGet();
    StreamingQuery query =
        startCollectingBatches(shardedRows(queryName, shards, count, 4L), queryName, checkpointDir);
    try {
      await("three commits", () -> committedBatchIds(checkpointDir).size() >= 3);
    } finally {
      stopQuietly(query);
    }

    long committedEpoch =
        endEpoch(checkpointDir, Collections.max(committedBatchIds(checkpointDir)));
    BeamSourceCheckpoint files = sourceCheckpoint(checkpointDir);
    int finalizations = 0;
    for (int shard = 0; shard < shards; shard++) {
      byte[] coded = files.readMark(shard, committedEpoch);
      assertNotNull("no mark at committed epoch " + committedEpoch + " for split " + shard, coded);
      int committedPosition =
          CoderUtils.decodeFromByteArray(ShardedListSource.MARK_CODER, coded).next;
      List<Integer> finalized = ShardedListSource.finalized(queryName, shard);
      for (int position : finalized) {
        assertTrue(
            "split "
                + shard
                + " finalized position "
                + position
                + " beyond committed position "
                + committedPosition,
            position <= committedPosition);
      }
      finalizations += finalized.size();
    }
    assertTrue("no mark was finalized", finalizations > 0);
  }

  /**
   * Spark reports the commit of batch N-1 when it constructs batch N, so after a run the surviving
   * marks of every split are at or above the end epoch of the batch before the last constructed
   * one, the mark at the highest committed end epoch exists, and the mark of batch 0 is gone.
   */
  @Test
  public void testMarksBelowCommittedOffsetArePurged() throws Exception {
    int shards = 2;
    int count = 4_000;
    File checkpointDir = temp.newFolder("purge");
    String queryName = "beam_purge_" + QUERY_COUNTER.incrementAndGet();
    StreamingQuery query =
        startCollectingBatches(shardedRows(queryName, shards, count, 4L), queryName, checkpointDir);
    try {
      await("three commits", () -> committedBatchIds(checkpointDir).size() >= 3);
    } finally {
      stopQuietly(query);
    }

    long lastConstructed = Collections.max(batchIds(new File(checkpointDir, "offsets")));
    long purgeFloor = endEpoch(checkpointDir, lastConstructed - 1);
    long committedEpoch =
        endEpoch(checkpointDir, Collections.max(committedBatchIds(checkpointDir)));
    long firstEpoch = endEpoch(checkpointDir, 0);
    assertTrue(committedEpoch >= purgeFloor);
    assertTrue(purgeFloor > firstEpoch);
    File sourceDir = new File(checkpointDir, "sources/0");
    // The last purge runs asynchronously on the driver and may still be in flight after stop.
    awaitQuietly(
        10_000L,
        () -> {
          for (int shard = 0; shard < shards; shard++) {
            for (long epoch : batchIds(new File(sourceDir, "marks/" + shard))) {
              if (epoch < purgeFloor) {
                return false;
              }
            }
          }
          return true;
        });
    for (int shard = 0; shard < shards; shard++) {
      Set<Long> remaining = batchIds(new File(sourceDir, "marks/" + shard));
      assertTrue(
          "split "
              + shard
              + " lost the mark at committed epoch "
              + committedEpoch
              + ": "
              + remaining,
          remaining.contains(committedEpoch));
      for (long epoch : remaining) {
        assertTrue(
            "split " + shard + " kept mark " + epoch + " below purge floor " + purgeFloor,
            epoch >= purgeFloor);
      }
      assertFalse("split " + shard + " kept the mark of batch 0", remaining.contains(firstEpoch));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private Dataset<Row> rows(int count, long watermarkDelayMillis) {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.create().as(SparkStructuredStreamingPipelineOptions.class);
    options.setWatermarkDelayMillis(watermarkDelayMillis);
    options.setMaxRecordsPerBatch(1000L);
    options.setMaxBatchDurationMillis(200L);
    return UnboundedSourceDataset.of(
        SESSION.getSession(), new ListSource(count), coder(), options, "Read(ListSource)");
  }

  /** Builds the driver side stream through the table, with real broadcasts from the session. */
  private static BeamMicroBatchStream<?> newStream(String checkpointLocation) {
    SparkSession session = SESSION.getSession();
    Broadcast<SerializablePipelineOptions> options =
        session
            .sparkContext()
            .broadcast(
                new SerializablePipelineOptions(PipelineOptionsFactory.create()),
                ClassTag.apply(SerializablePipelineOptions.class));
    Broadcast<SerializableConfiguration> hadoopConf =
        session
            .sparkContext()
            .broadcast(
                new SerializableConfiguration(new Configuration()),
                ClassTag.apply(SerializableConfiguration.class));
    BeamSourceSpec<String> spec =
        new BeamSourceSpec<>(
            new ListSource(4),
            coder(),
            options,
            hadoopConf,
            2,
            -1L,
            200L,
            600_000L,
            "Read(ListSource)");
    MicroBatchStream stream =
        new BeamStreamingTable(spec)
            .newScanBuilder(CaseInsensitiveStringMap.empty())
            .build()
            .toMicroBatchStream(checkpointLocation);
    return (BeamMicroBatchStream<?>) stream;
  }

  private Dataset<Row> shardedRows(String tag, int shards, int count, long maxRecordsPerBatch) {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.create().as(SparkStructuredStreamingPipelineOptions.class);
    options.setWatermarkDelayMillis(0L);
    options.setMaxRecordsPerBatch(maxRecordsPerBatch);
    options.setMaxBatchDurationMillis(1_000L);
    return UnboundedSourceDataset.of(
        SESSION.getSession(),
        new ShardedListSource(tag, shards, count),
        coder(),
        options,
        "Read(ShardedListSource)");
  }

  /** Starts a query collecting every micro-batch as one list into {@link #BATCHES}. */
  private static StreamingQuery startCollectingBatches(
      Dataset<Row> dataset, String queryName, File checkpointDir) throws Exception {
    BATCHES.put(queryName, Collections.synchronizedList(new ArrayList<>()));
    return dataset
        .writeStream()
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (batch, batchId) -> {
                  List<List<Row>> target = BATCHES.get(queryName);
                  if (target != null) {
                    target.add(batch.collectAsList());
                  }
                })
        .queryName(queryName)
        .outputMode("append")
        .option("checkpointLocation", checkpointDir.getAbsolutePath())
        .trigger(Trigger.ProcessingTime(100))
        .start();
  }

  private static List<List<Row>> batches(String queryName) {
    List<List<Row>> batches = BATCHES.getOrDefault(queryName, Collections.emptyList());
    synchronized (batches) {
      return new ArrayList<>(batches);
    }
  }

  private static List<String> values(List<List<Row>> batches) {
    Coder<WindowedValue<String>> coder = coder();
    List<String> values = new ArrayList<>();
    for (List<Row> batch : batches) {
      for (Row row : batch) {
        byte[] payload = row.getAs(COL_PAYLOAD);
        try {
          values.add(CoderUtils.decodeFromByteArray(coder, payload).getValue());
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return values;
  }

  private static void await(String what, BooleanSupplier condition) throws Exception {
    if (!awaitQuietly(POLL_TIMEOUT_MILLIS, condition)) {
      throw new AssertionError("timed out waiting for " + what);
    }
  }

  private static boolean awaitQuietly(long timeoutMillis, BooleanSupplier condition)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (System.currentTimeMillis() < deadline) {
      if (condition.getAsBoolean()) {
        return true;
      }
      Thread.sleep(50L);
    }
    return condition.getAsBoolean();
  }

  /** Numeric file names in a Spark log directory, temp and hidden files excluded. */
  private static Set<Long> batchIds(File dir) {
    Set<Long> ids = new TreeSet<>();
    String[] names = dir.list();
    if (names == null) {
      return ids;
    }
    for (String name : names) {
      if (!name.startsWith(".") && !name.endsWith(".tmp")) {
        try {
          ids.add(Long.parseLong(name));
        } catch (NumberFormatException e) {
          // not a log entry
        }
      }
    }
    return ids;
  }

  private static Set<Long> committedBatchIds(File checkpointDir) {
    return batchIds(new File(checkpointDir, "commits"));
  }

  /** The end epoch of a batch, the offset line of the single source in {@code offsets/<id>}. */
  private static long endEpoch(File checkpointDir, long batchId) throws IOException {
    File file = new File(new File(checkpointDir, "offsets"), Long.toString(batchId));
    List<String> lines = new ArrayList<>();
    for (String line : Files.readAllLines(file.toPath(), StandardCharsets.UTF_8)) {
      if (!line.trim().isEmpty()) {
        lines.add(line.trim());
      }
    }
    assertTrue("offset log entry too short: " + lines, lines.size() >= 3);
    assertEquals("one source expected in " + lines, 3, lines.size());
    return BeamOffset.fromJson(lines.get(2)).epoch();
  }

  private static BeamSourceCheckpoint sourceCheckpoint(File checkpointDir) {
    return new BeamSourceCheckpoint(
        new File(checkpointDir, "sources/0").getAbsolutePath(), new Configuration());
  }

  private static Coder<WindowedValue<String>> coder() {
    return WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
  }

  private static String element(int index) {
    return "element-" + index;
  }

  private static long timestampMillis(int index) {
    return BASE_MILLIS + index * INTERVAL_MILLIS;
  }

  /**
   * Starts a query that throws its output away.
   *
   * <p>The {@code noop} sink is used on purpose: these tests observe the source through a {@code
   * foreachBatch} or through the query's own progress, never through the sink, so there is no
   * reason to buffer rows anywhere. That matches what the streaming evaluation context does for
   * real pipelines.
   */
  private StreamingQuery startDiscarding(Dataset<?> dataset, String queryName) throws Exception {
    return dataset
        .writeStream()
        .format("noop")
        .queryName(queryName)
        .outputMode("append")
        .option("checkpointLocation", temp.newFolder(queryName).getAbsolutePath())
        .trigger(Trigger.ProcessingTime(100))
        .start();
  }

  /**
   * Starts a query collecting every micro-batch into {@link #COLLECTED} under {@code queryName}.
   */
  private StreamingQuery startCollecting(Dataset<Row> dataset, String queryName) throws Exception {
    COLLECTED.put(queryName, Collections.synchronizedList(new ArrayList<>()));
    return dataset
        .writeStream()
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (batch, batchId) -> {
                  // Exactly one action per micro-batch: any second action would re-execute the
                  // batch and advance the Beam reader past records that were never collected.
                  List<Row> target = COLLECTED.get(queryName);
                  if (target != null) {
                    target.addAll(batch.collectAsList());
                  }
                })
        .queryName(queryName)
        .outputMode("append")
        .option("checkpointLocation", temp.newFolder(queryName).getAbsolutePath())
        .trigger(Trigger.ProcessingTime(100))
        .start();
  }

  private static void stopQuietly(StreamingQuery query) {
    try {
      query.stop();
    } catch (Exception e) {
      // Nothing useful to do while tearing a test query down.
    }
  }

  /** Polls the collected batches until {@code expected} rows arrived or the deadline expires. */
  private static List<Row> awaitRows(String queryName, int expected) throws Exception {
    long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MILLIS;
    List<Row> rows = COLLECTED.getOrDefault(queryName, Collections.emptyList());
    while (System.currentTimeMillis() < deadline) {
      synchronized (rows) {
        if (rows.size() >= expected) {
          return new ArrayList<>(rows);
        }
      }
      Thread.sleep(100L);
    }
    synchronized (rows) {
      return new ArrayList<>(rows);
    }
  }

  /** Polls the query progress until it reports an event time watermark past the epoch. */
  private static @Nullable String awaitWatermark(StreamingQuery query) throws Exception {
    long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MILLIS;
    String last = null;
    while (System.currentTimeMillis() < deadline) {
      for (StreamingQueryProgress progress : query.recentProgress()) {
        Map<String, String> eventTime = progress.eventTime();
        String watermark = eventTime.get("watermark");
        if (watermark != null) {
          last = watermark;
          if (!watermark.startsWith("1970-")) {
            return watermark;
          }
        }
      }
      Thread.sleep(100L);
    }
    return last;
  }

  private static LogicalPlan logical(Dataset<?> dataset) {
    return ((org.apache.spark.sql.classic.Dataset<?>) dataset).queryExecution().logical();
  }

  private static LogicalPlan analyzed(Dataset<?> dataset) {
    return ((org.apache.spark.sql.classic.Dataset<?>) dataset).queryExecution().analyzed();
  }

  private static void assertWatermark(String what, LogicalPlan plan) {
    assertTrue(
        "no EventTimeWatermark node found " + what + ":\n" + plan.treeString(),
        containsWatermark(plan));
  }

  private static boolean containsWatermark(LogicalPlan plan) {
    if (plan instanceof EventTimeWatermark) {
      return true;
    }
    scala.collection.Iterator<LogicalPlan> children = plan.children().iterator();
    while (children.hasNext()) {
      if (containsWatermark(children.next())) {
        return true;
      }
    }
    return false;
  }

  // ---------------------------------------------------------------------------------------------
  // a minimal in-memory UnboundedSource
  // ---------------------------------------------------------------------------------------------

  /**
   * A trivial single split {@link UnboundedSource} over a fixed number of synthetic elements with
   * evenly spaced event timestamps. It is exhausted after the last element, further calls to {@code
   * advance()} simply report that no data is available.
   */
  private static class ListSource extends UnboundedSource<String, ListSource.Mark> {
    private static final long serialVersionUID = 1L;

    private final int count;

    ListSource(int count) {
      this.count = count;
    }

    @Override
    public List<ListSource> split(int desiredNumSplits, PipelineOptions options) {
      return Arrays.asList(this);
    }

    @Override
    public UnboundedReader<String> createReader(PipelineOptions options, @Nullable Mark mark) {
      return new ListReader(this, mark == null ? 0 : mark.next);
    }

    @Override
    public Coder<Mark> getCheckpointMarkCoder() {
      return SerializableCoder.of(Mark.class);
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    /** Position of the next element to read. */
    static class Mark implements UnboundedSource.CheckpointMark, Serializable {
      private static final long serialVersionUID = 1L;

      private final int next;

      Mark(int next) {
        this.next = next;
      }

      @Override
      public void finalizeCheckpoint() {}
    }

    private static class ListReader extends UnboundedReader<String> {
      private final ListSource source;
      private int next;
      private int current = -1;

      ListReader(ListSource source, int next) {
        this.source = source;
        this.next = next;
      }

      @Override
      public boolean start() {
        return advance();
      }

      @Override
      public boolean advance() {
        if (next < source.count) {
          current = next++;
          return true;
        }
        return false;
      }

      @Override
      public String getCurrent() throws NoSuchElementException {
        if (current < 0) {
          throw new NoSuchElementException();
        }
        return element(current);
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current < 0) {
          throw new NoSuchElementException();
        }
        return new Instant(timestampMillis(current));
      }

      @Override
      public Instant getWatermark() {
        return current < 0
            ? BoundedWindow.TIMESTAMP_MIN_VALUE
            : new Instant(timestampMillis(current));
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new Mark(next);
      }

      @Override
      public UnboundedSource<String, ?> getCurrentSource() {
        return source;
      }

      @Override
      public void close() throws IOException {}
    }
  }

  // ---------------------------------------------------------------------------------------------
  // a multi split in-memory UnboundedSource with finalization counting marks
  // ---------------------------------------------------------------------------------------------

  /**
   * Splits into one sub source per shard, each over {@code count / shards} elements named {@code
   * shard-N-element-M} with evenly spaced timestamps. Marks are not Java serializable, they carry
   * the shard's read position and record it under {@code <tag>/<shard>} when finalized.
   */
  static class ShardedListSource extends UnboundedSource<String, ShardedListSource.ShardMark> {
    private static final long serialVersionUID = 1L;

    static final Coder<ShardMark> MARK_CODER = new MarkCoder();

    /** Finalized positions keyed by {@code <tag>/<shard>}. */
    static final ConcurrentMap<String, List<Integer>> FINALIZED = new ConcurrentHashMap<>();

    private final String tag;
    private final int shard;
    private final int shards;
    private final int perShard;

    ShardedListSource(String tag, int shards, int count) {
      this(tag, -1, shards, count / shards);
    }

    private ShardedListSource(String tag, int shard, int shards, int perShard) {
      this.tag = tag;
      this.shard = shard;
      this.shards = shards;
      this.perShard = perShard;
    }

    static Set<String> elements(int shards, int count) {
      Set<String> elements = new HashSet<>();
      for (int shard = 0; shard < shards; shard++) {
        for (int index = 0; index < count / shards; index++) {
          elements.add(element(shard, index));
        }
      }
      return elements;
    }

    static String element(int shard, int index) {
      return "shard-" + shard + "-element-" + index;
    }

    static int shardOf(String element) {
      return Integer.parseInt(element.substring("shard-".length(), element.indexOf("-element-")));
    }

    static int indexOf(String element) {
      return Integer.parseInt(element.substring(element.lastIndexOf('-') + 1));
    }

    static List<Integer> finalized(String tag, int shard) {
      List<Integer> positions = FINALIZED.get(key(tag, shard));
      if (positions == null) {
        return Collections.emptyList();
      }
      synchronized (positions) {
        return new ArrayList<>(positions);
      }
    }

    private static String key(String tag, int shard) {
      return tag + "/" + shard;
    }

    @Override
    public List<ShardedListSource> split(int desiredNumSplits, PipelineOptions options) {
      if (shard >= 0) {
        return Collections.singletonList(this);
      }
      List<ShardedListSource> splits = new ArrayList<>();
      for (int i = 0; i < shards; i++) {
        splits.add(new ShardedListSource(tag, i, shards, perShard));
      }
      return splits;
    }

    @Override
    public UnboundedReader<String> createReader(PipelineOptions options, @Nullable ShardMark mark) {
      if (shard < 0) {
        throw new IllegalStateException("split before reading");
      }
      return new ShardReader(this, mark == null ? 0 : mark.next);
    }

    @Override
    public Coder<ShardMark> getCheckpointMarkCoder() {
      return MARK_CODER;
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    /** Position of the next element of a shard, deliberately not {@link Serializable}. */
    static final class ShardMark implements UnboundedSource.CheckpointMark {
      private final String key;
      final int next;

      ShardMark(String key, int next) {
        this.key = key;
        this.next = next;
      }

      @Override
      public void finalizeCheckpoint() {
        FINALIZED
            .computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>()))
            .add(next);
      }
    }

    private static final class MarkCoder extends CustomCoder<ShardMark> {
      private static final long serialVersionUID = 1L;

      @Override
      public void encode(ShardMark mark, OutputStream out) throws IOException {
        StringUtf8Coder.of().encode(mark.key, out);
        VarIntCoder.of().encode(mark.next, out);
      }

      @Override
      public ShardMark decode(InputStream in) throws IOException {
        return new ShardMark(StringUtf8Coder.of().decode(in), VarIntCoder.of().decode(in));
      }
    }

    private static final class ShardReader extends UnboundedReader<String> {
      private final ShardedListSource source;
      private int next;
      private int current = -1;

      ShardReader(ShardedListSource source, int next) {
        this.source = source;
        this.next = next;
      }

      @Override
      public boolean start() {
        return advance();
      }

      @Override
      public boolean advance() {
        if (next < source.perShard) {
          current = next++;
          return true;
        }
        return false;
      }

      @Override
      public String getCurrent() throws NoSuchElementException {
        if (current < 0) {
          throw new NoSuchElementException();
        }
        return element(source.shard, current);
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current < 0) {
          throw new NoSuchElementException();
        }
        return new Instant(timestampMillis(source.shard * source.perShard + current));
      }

      @Override
      public Instant getWatermark() {
        return current < 0 ? BoundedWindow.TIMESTAMP_MIN_VALUE : getCurrentTimestamp();
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new ShardMark(key(source.tag, source.shard), next);
      }

      @Override
      public UnboundedSource<String, ?> getCurrentSource() {
        return source;
      }

      @Override
      public void close() {}
    }
  }
}
