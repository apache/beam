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
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset.BeamInputPartition;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset.BeamMicroBatchStream;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset.BeamOffset;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset.BeamPartitionReader;
import org.apache.beam.runners.spark.structuredstreaming.io.streaming.UnboundedSourceDataset.BeamTable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.CountingSource;
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
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
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
 * <p>The epoch offsets of this source never settle, so {@code processAllAvailable()} would block
 * forever. Every query runs with {@code Trigger.ProcessingTime(100)} and is stopped explicitly once
 * the expected result arrived or the poll deadline expired.
 */
@Category(StreamingTest.class)
@RunWith(JUnit4.class)
public class BeamMicroBatchSourceTest implements Serializable {

  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder temp = new TemporaryFolder();

  private static final AtomicInteger TAGS = new AtomicInteger();

  /** Rows per micro-batch per query name, driver side. */
  private static final Map<String, List<List<Row>>> BATCHES = new ConcurrentHashMap<>();

  private static final Coder<WindowedValue<String>> CODER =
      WindowedValues.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);

  /** 2023-11-14T22:13:20Z, a plain modern timestamp with no rebase or DST subtleties. */
  private static final long BASE_MILLIS = 1_700_000_000_000L;

  private static final long INTERVAL_MILLIS = 1_000L;

  private static final long POLL_TIMEOUT_MILLIS = 120_000L;

  private static Broadcast<SerializablePipelineOptions> optionsBroadcast;
  private static Broadcast<SerializableConfiguration> hadoopConfBroadcast;

  private String tag;

  @BeforeClass
  public static void broadcastOnce() {
    SparkSession session = SESSION.getSession();
    Configuration conf =
        ((org.apache.spark.sql.classic.SparkSession) session).sessionState().newHadoopConf();
    optionsBroadcast =
        session
            .sparkContext()
            .broadcast(
                new SerializablePipelineOptions(PipelineOptionsFactory.create()),
                ClassTag.apply(SerializablePipelineOptions.class));
    hadoopConfBroadcast =
        session
            .sparkContext()
            .broadcast(
                new SerializableConfiguration(conf),
                ClassTag.apply(SerializableConfiguration.class));
  }

  @Before
  public void setUp() {
    tag = "src" + TAGS.incrementAndGet();
  }

  @After
  public void tearDown() {
    BeamReaderCache.invalidateAll();
    BATCHES.clear();
    TestSource.forget(tag);
  }

  /** The {@code EventTimeWatermark} node survives typed maps in the logical and analyzed plan. */
  @Test
  public void testEventTimeWatermarkSurvivesTypedMap() {
    Dataset<Row> rows = rows(1, 4, limited(1_000L, 200L));
    assertTrue("source dataset must be streaming", rows.isStreaming());
    assertWatermark("directly after withWatermark, logical plan", logical(rows));
    assertWatermark("directly after withWatermark, analyzed plan", analyzed(rows));

    Dataset<byte[]> typed =
        rows.map((MapFunction<Row, byte[]>) row -> row.getAs(COL_PAYLOAD), Encoders.BINARY());
    assertWatermark("after a typed map, logical plan", logical(typed));
    assertWatermark("after a typed map, analyzed plan", analyzed(typed));

    Dataset<byte[]> chained =
        typed.map((MapFunction<byte[], byte[]>) bytes -> bytes, Encoders.BINARY());
    assertWatermark("after two chained typed maps, logical plan", logical(chained));
    assertWatermark("after two chained typed maps, analyzed plan", analyzed(chained));
  }

  /** A running query tracks the event time watermark past a typed map. */
  @Test
  public void testWatermarkIsTrackedAtRuntimeAfterTypedMap() throws Exception {
    Dataset<byte[]> typed =
        rows(1, 8, limited(1_000L, 200L))
            .map((MapFunction<Row, byte[]>) row -> row.getAs(COL_PAYLOAD), Encoders.BINARY());
    StreamingQuery query =
        typed
            .writeStream()
            .format("noop")
            .queryName(tag)
            .outputMode("append")
            .option("checkpointLocation", temp.newFolder(tag).getAbsolutePath())
            .trigger(Trigger.ProcessingTime(100))
            .start();
    try {
      String watermark = awaitWatermark(query);
      assertNotNull("query never reported an event time watermark", watermark);
      assertFalse("watermark stuck at the epoch: " + watermark, watermark.startsWith("1970-"));
    } finally {
      stopQuietly(query);
    }
  }

  /** Payloads decode to the source elements and the timestamp column matches the element. */
  @Test
  public void testReadsElementsFromUnboundedSource() throws Exception {
    int count = 8;
    StreamingQuery query = start(rows(1, count, limited(1_000L, 200L)), tag, temp.newFolder(tag));
    try {
      await("all rows", () -> values(batches(tag)).size() >= count);
    } finally {
      stopQuietly(query);
    }
    List<String> values = new ArrayList<>();
    for (List<Row> batch : batches(tag)) {
      for (Row row : batch) {
        WindowedValue<String> value = CoderUtils.decodeFromByteArray(CODER, row.getAs(COL_PAYLOAD));
        values.add(value.getValue());
        assertEquals(
            value.getTimestamp().getMillis(), row.<Timestamp>getAs(COL_EVENT_TS).getTime());
        assertEquals(
            Collections.singletonList(GlobalWindow.INSTANCE), new ArrayList<>(value.getWindows()));
        assertEquals(
            BASE_MILLIS + TestSource.indexOf(value.getValue()) * INTERVAL_MILLIS,
            value.getTimestamp().getMillis());
      }
    }
    assertEquals(count, values.size());
    assertEquals(TestSource.elements(tag, 1, count), new HashSet<>(values));
  }

  /** The default record limit is unlimited, an available source drains in one micro-batch. */
  @Test
  public void testUnlimitedRecordsPerBatchByDefault() throws Exception {
    int count = 2500;
    StreamingQuery query = start(rows(1, count, options(5_000L)), tag, temp.newFolder(tag));
    try {
      await("a non empty batch", () -> !nonEmptySizes(batches(tag)).isEmpty());
    } finally {
      stopQuietly(query);
    }
    assertEquals(Collections.singletonList(count), nonEmptySizes(batches(tag)));
  }

  /** The offset is an opaque epoch counter whose JSON is the bare number. */
  @Test
  public void testEpochOffsetRoundTrip() {
    BeamOffset offset = new BeamOffset(42L);
    assertEquals("42", offset.json());
    assertEquals(42L, BeamOffset.fromJson("42").epoch());
    assertEquals(0L, BeamOffset.ZERO.epoch());
    assertEquals(new BeamOffset(7L), new BeamOffset(7L));
    assertThrows(IllegalArgumentException.class, () -> BeamOffset.fromJson("x"));
  }

  /** A deserialized offset moves the epoch counter past itself. */
  @Test
  public void testEpochFastForwardsPastDeserializedOffset() throws Exception {
    BeamMicroBatchStream<?> stream = newStream(temp.newFolder("ff-offset").getAbsolutePath());
    stream.deserializeOffset("7");
    BeamOffset next = (BeamOffset) stream.latestOffset();
    assertTrue("latestOffset must move past the replayed epoch 7, got " + next, next.epoch() > 7L);
  }

  /** A planned end offset moves the epoch counter past itself. */
  @Test
  public void testEpochFastForwardsPastPlannedOffsets() throws Exception {
    BeamMicroBatchStream<?> stream = newStream(temp.newFolder("ff-plan").getAbsolutePath());
    InputPartition[] partitions =
        stream.planInputPartitions(new BeamOffset(3L), new BeamOffset(9L));
    assertTrue("at least one partition expected", partitions.length > 0);
    BeamOffset next = (BeamOffset) stream.latestOffset();
    assertTrue("latestOffset must move past the planned epoch 9, got " + next, next.epoch() > 9L);
  }

  /** The batch quota is divided over the splits and the remainder rotates with the epoch. */
  @Test
  public void testSplitQuotas() {
    assertArrayEquals(
        new long[] {1, 1, 1, 0, 0, 0, 0, 0}, BeamMicroBatchStream.splitQuotas(3, 8, 0));
    assertArrayEquals(
        new long[] {0, 0, 0, 0, 0, 1, 1, 1}, BeamMicroBatchStream.splitQuotas(3, 8, 3));
    assertArrayEquals(new long[] {4, 3, 3}, BeamMicroBatchStream.splitQuotas(10, 3, 0));
    assertArrayEquals(new long[] {3, 4, 3}, BeamMicroBatchStream.splitQuotas(10, 3, 2));
    assertArrayEquals(new long[] {5, 5}, BeamMicroBatchStream.splitQuotas(10, 2, 0));
    assertArrayEquals(new long[] {-1, -1, -1}, BeamMicroBatchStream.splitQuotas(0, 3, 0));
    assertArrayEquals(new long[] {-1, -1, -1}, BeamMicroBatchStream.splitQuotas(-1, 3, 0));
    long[] many = BeamMicroBatchStream.splitQuotas(1, 200, 0);
    assertEquals(200, many.length);
    assertEquals(1L, many[0]);
    assertEquals(1L, Arrays.stream(many).sum());
  }

  /** The record limit of a micro-batch is a total over all splits. */
  @Test
  public void testMaxRecordsPerBatchIsSharedAcrossSplits() throws Exception {
    int shards = 2;
    int count = 30;
    StreamingQuery query =
        start(rows(shards, count, limited(10L, 1_000L)), tag, temp.newFolder(tag));
    try {
      await("all rows", () -> values(batches(tag)).size() >= count);
    } finally {
      stopQuietly(query);
    }
    List<Integer> sizes = nonEmptySizes(batches(tag));
    assertFalse("no rows arrived", sizes.isEmpty());
    assertTrue("batch exceeds the shared limit: " + sizes, Collections.max(sizes) <= 10);
    assertEquals(TestSource.elements(tag, shards, count), new HashSet<>(values(batches(tag))));
  }

  /**
   * A limit below the split count emits at most the limit per batch and rotates over the splits.
   */
  @Test
  public void testQuotaBelowSplitCountRotates() throws Exception {
    int shards = 4;
    StreamingQuery query = start(rows(shards, 40, limited(1L, 1_000L)), tag, temp.newFolder(tag));
    try {
      await("every shard", () -> shardsOf(values(batches(tag))).size() == shards);
    } finally {
      stopQuietly(query);
    }
    List<Integer> sizes = nonEmptySizes(batches(tag));
    assertTrue("batch exceeds the limit of 1: " + sizes, Collections.max(sizes) <= 1);
  }

  /** A restart resumes every split from the last committed mark, replaying at most one batch. */
  @Test
  public void testRestartResumesFromCommittedMark() throws Exception {
    int shards = 2;
    int count = 80;
    long limit = 4L;
    File checkpointDir = temp.newFolder("restart");
    String first = tag + "_a";
    String second = tag + "_b";

    StreamingQuery query = start(rows(shards, count, limited(limit, 1_000L)), first, checkpointDir);
    try {
      await("two commits", () -> committedBatchIds(checkpointDir).size() >= 2);
    } finally {
      stopQuietly(query);
    }
    BeamReaderCache.invalidateAll();
    List<String> firstValues = values(batches(first));

    Set<String> all = TestSource.elements(tag, shards, count);
    query = start(rows(shards, count, limited(limit, 1_000L)), second, checkpointDir);
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
        "more than one batch replayed: " + firstValues.size() + " + " + secondValues.size(),
        firstValues.size() + secondValues.size() <= count + limit);
    for (int shard = 0; shard < shards; shard++) {
      int min = Integer.MAX_VALUE;
      for (String value : secondValues) {
        if (TestSource.shardOf(value) == shard) {
          min = Math.min(min, TestSource.indexOf(value));
        }
      }
      assertTrue("run 2 delivered nothing for shard " + shard, min < Integer.MAX_VALUE);
      assertTrue("run 2 restarted shard " + shard + " from element 0", min > 0);
    }
  }

  /** No split finalizes a position beyond its mark at the end epoch of the last committed batch. */
  @Test
  public void testMarksAreFinalizedOnlyAfterSparkCommit() throws Exception {
    File checkpointDir = temp.newFolder("finalize");
    runUntilCommits(checkpointDir, 3);

    int finalizations = 0;
    for (int shard = 0; shard < 2; shard++) {
      int committed = committedPosition(checkpointDir, shard);
      List<Integer> finalized = TestSource.finalized(tag, shard);
      assertTrue(
          "shard " + shard + " finalized " + finalized + " beyond committed " + committed,
          finalized.isEmpty() || Collections.max(finalized) <= committed);
      finalizations += finalized.size();
    }
    assertTrue("no mark was finalized", finalizations > 0);
  }

  /**
   * After a run the mark at the last committed epoch exists, every surviving mark is at or above
   * the end epoch of the batch before the last constructed one, and the mark of batch 0 is gone.
   */
  @Test
  public void testMarksBelowCommittedOffsetArePurged() throws Exception {
    File checkpointDir = temp.newFolder("purge");
    runUntilCommits(checkpointDir, 3);

    long lastConstructed = Collections.max(batchIds(new File(checkpointDir, "offsets")));
    long purgeFloor = endEpoch(checkpointDir, lastConstructed - 1);
    long committedEpoch = committedEpoch(checkpointDir);
    long firstEpoch = endEpoch(checkpointDir, 0);
    assertTrue(committedEpoch >= purgeFloor);
    assertTrue(purgeFloor > firstEpoch);
    File sourceDir = sourceDir(checkpointDir);
    awaitQuietly(
        10_000L,
        () -> {
          for (int shard = 0; shard < 2; shard++) {
            TreeSet<Long> epochs = markEpochs(sourceDir, shard);
            if (epochs.isEmpty() || epochs.first() < purgeFloor) {
              return false;
            }
          }
          return true;
        });
    for (int shard = 0; shard < 2; shard++) {
      TreeSet<Long> remaining = markEpochs(sourceDir, shard);
      assertTrue(
          "shard "
              + shard
              + " lost the mark at committed epoch "
              + committedEpoch
              + ": "
              + remaining,
          remaining.contains(committedEpoch));
      assertTrue(
          "shard " + shard + " kept marks below purge floor " + purgeFloor + ": " + remaining,
          remaining.first() >= purgeFloor);
      assertFalse("shard " + shard + " kept the mark of batch 0", remaining.contains(firstEpoch));
    }
  }

  /**
   * After a stop an idle sweep finalizes exactly the marks of the last committed epoch, {@link
   * BeamReaderCache#closeIdle(long)} is the one white box hook these tests use.
   */
  @Test
  public void testStoppedQueryFinalizesLastCommittedMarks() throws Exception {
    File checkpointDir = temp.newFolder("stopped");
    runUntilCommits(checkpointDir, 3);
    BeamReaderCache.closeIdle(Long.MAX_VALUE);

    for (int shard = 0; shard < 2; shard++) {
      int committed = committedPosition(checkpointDir, shard);
      List<Integer> finalized = TestSource.finalized(tag, shard);
      assertTrue(
          "shard " + shard + " finalized " + finalized + ", committed " + committed,
          finalized.contains(committed) && Collections.max(finalized) == committed);
    }
  }

  /** A retried batch restarts from the durable mark at its start and finalizes nothing. */
  @Test
  public void testRetriedBatchRestartsFromDurableMark() throws Exception {
    String location = sourceDir(temp.newFolder("protocol")).getAbsolutePath();
    assertEquals(shardZero(0, 1, 2), readBatch(partition(location, 0, 1)));
    assertEquals(shardZero(0, 1, 2), readBatch(partition(location, 0, 1)));
    assertEquals(Collections.emptyList(), TestSource.finalized(tag, 0));
    assertEquals(2, TestSource.created(tag));
  }

  /** A start epoch above zero without a durable mark is an invariant violation. */
  @Test
  public void testMissingMarkThrows() throws Exception {
    String location = sourceDir(temp.newFolder("protocol")).getAbsolutePath();
    assertThrows(
        IllegalStateException.class, () -> new BeamPartitionReader<>(partition(location, 5, 6)));
    assertEquals(0, TestSource.created(tag));
  }

  /** A failed mark write fails the batch after its rows, the retry recreates the reader. */
  @Test
  public void testRetryAfterFailedMarkWriteRecreatesReader() throws Exception {
    File location = sourceDir(temp.newFolder("protocol"));
    assertTrue(location.getParentFile().mkdirs() && location.createNewFile());
    String file = location.getAbsolutePath();
    assertEquals(shardZero(0, 1, 2), drainUntilFailure(partition(file, 0, 1), IOException.class));
    assertEquals(shardZero(0, 1, 2), drainUntilFailure(partition(file, 0, 1), IOException.class));
    assertEquals(Collections.emptyList(), TestSource.finalized(tag, 0));
    assertEquals(2, TestSource.created(tag));
  }

  // ---------------------------------------------------------------------------------------------
  // query helpers
  // ---------------------------------------------------------------------------------------------

  private static SparkStructuredStreamingPipelineOptions options(long maxBatchDurationMillis) {
    SparkStructuredStreamingPipelineOptions options =
        PipelineOptionsFactory.create().as(SparkStructuredStreamingPipelineOptions.class);
    options.setWatermarkDelayMillis(0L);
    options.setMaxBatchDurationMillis(maxBatchDurationMillis);
    return options;
  }

  private static SparkStructuredStreamingPipelineOptions limited(
      long maxRecordsPerBatch, long maxBatchDurationMillis) {
    SparkStructuredStreamingPipelineOptions options = options(maxBatchDurationMillis);
    options.setMaxRecordsPerBatch(maxRecordsPerBatch);
    return options;
  }

  private Dataset<Row> rows(
      int shards, int count, SparkStructuredStreamingPipelineOptions options) {
    return UnboundedSourceDataset.of(
        SESSION.getSession(),
        new TestSource(tag, shards, count),
        CODER,
        options,
        "Read(TestSource)");
  }

  /** Builds the driver side stream through the table, with the session's broadcasts. */
  private static BeamMicroBatchStream<?> newStream(String checkpointLocation) {
    BeamTable<Long> table =
        new BeamTable<>(
            CountingSource.unbounded(),
            WindowedValues.getFullCoder(VarLongCoder.of(), GlobalWindow.Coder.INSTANCE),
            optionsBroadcast,
            hadoopConfBroadcast,
            2,
            -1L,
            200L,
            600_000L,
            "Read(CountingSource)");
    return (BeamMicroBatchStream<?>)
        table
            .newScanBuilder(CaseInsensitiveStringMap.empty())
            .build()
            .toMicroBatchStream(checkpointLocation);
  }

  /** Starts a query collecting every micro-batch as one list into {@link #BATCHES}. */
  private static StreamingQuery start(Dataset<Row> dataset, String queryName, File checkpointDir)
      throws Exception {
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

  /** Runs two shards with a limit of 4 over a source that never drains until Spark committed. */
  private void runUntilCommits(File checkpointDir, int commits) throws Exception {
    StreamingQuery query = start(rows(2, 4_000, limited(4L, 1_000L)), tag, checkpointDir);
    try {
      await(commits + " commits", () -> committedBatchIds(checkpointDir).size() >= commits);
    } finally {
      stopQuietly(query);
    }
  }

  private static void stopQuietly(StreamingQuery query) {
    try {
      query.stop();
    } catch (Exception e) {
      // Nothing useful to do while tearing a test query down.
    }
  }

  private static List<List<Row>> batches(String queryName) {
    List<List<Row>> batches = BATCHES.getOrDefault(queryName, Collections.emptyList());
    synchronized (batches) {
      return new ArrayList<>(batches);
    }
  }

  private static List<String> values(List<List<Row>> batches) {
    List<String> values = new ArrayList<>();
    for (List<Row> batch : batches) {
      for (Row row : batch) {
        values.add(decode(row.getAs(COL_PAYLOAD)));
      }
    }
    return values;
  }

  private static String decode(byte[] payload) {
    try {
      return CoderUtils.decodeFromByteArray(CODER, payload).getValue();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private static List<Integer> nonEmptySizes(List<List<Row>> batches) {
    List<Integer> sizes = new ArrayList<>();
    for (List<Row> batch : batches) {
      if (!batch.isEmpty()) {
        sizes.add(batch.size());
      }
    }
    return sizes;
  }

  private static Set<Integer> shardsOf(List<String> values) {
    Set<Integer> shards = new HashSet<>();
    for (String value : values) {
      shards.add(TestSource.shardOf(value));
    }
    return shards;
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

  /** Polls the query progress until it reports an event time watermark past the epoch. */
  private static @Nullable String awaitWatermark(StreamingQuery query) throws Exception {
    long deadline = System.currentTimeMillis() + POLL_TIMEOUT_MILLIS;
    String last = null;
    while (System.currentTimeMillis() < deadline) {
      for (StreamingQueryProgress progress : query.recentProgress()) {
        String watermark = progress.eventTime().get("watermark");
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
  // checkpoint helpers
  // ---------------------------------------------------------------------------------------------

  private static File sourceDir(File checkpointDir) {
    return new File(checkpointDir, "sources/0");
  }

  /** Numeric file names in a Spark log directory, temp and hidden files excluded. */
  private static TreeSet<Long> batchIds(File dir) {
    TreeSet<Long> ids = new TreeSet<>();
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

  /** Epochs under {@code marks/<epoch>/} holding a mark file of {@code shard}. */
  private static TreeSet<Long> markEpochs(File sourceDir, int shard) {
    TreeSet<Long> epochs = new TreeSet<>();
    for (long epoch : batchIds(new File(sourceDir, "marks"))) {
      if (new File(sourceDir, "marks/" + epoch + "/" + shard).exists()) {
        epochs.add(epoch);
      }
    }
    return epochs;
  }

  private static TreeSet<Long> committedBatchIds(File checkpointDir) {
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
    assertEquals("one source expected in " + lines, 3, lines.size());
    return BeamOffset.fromJson(lines.get(2)).epoch();
  }

  private static long committedEpoch(File checkpointDir) throws IOException {
    return endEpoch(checkpointDir, committedBatchIds(checkpointDir).last());
  }

  /** The position in the mark of {@code shard} at the end epoch of the last committed batch. */
  private static int committedPosition(File checkpointDir, int shard) throws IOException {
    long epoch = committedEpoch(checkpointDir);
    BeamSourceCheckpoint checkpoint =
        new BeamSourceCheckpoint(sourceDir(checkpointDir).getAbsolutePath(), new Configuration());
    byte[] coded = checkpoint.readMark(shard, epoch);
    assertNotNull("no mark at committed epoch " + epoch + " for shard " + shard, coded);
    return CoderUtils.decodeFromByteArray(TestSource.MARK_CODER, coded).next;
  }

  // ---------------------------------------------------------------------------------------------
  // hand built partition helpers
  // ---------------------------------------------------------------------------------------------

  /** Split 0 of a single shard source of 100 elements from epoch {@code start} to {@code end}. */
  private BeamInputPartition<String> partition(String location, long start, long end) {
    TestSource split = new TestSource(tag, 1, 100).split(1, PipelineOptionsFactory.create()).get(0);
    return new BeamInputPartition<>(
        split,
        CODER,
        optionsBroadcast,
        hadoopConfBroadcast,
        location,
        0,
        start,
        end,
        3L,
        30_000L,
        600_000L);
  }

  private static List<String> readBatch(BeamInputPartition<String> partition) throws IOException {
    List<String> values = new ArrayList<>();
    drainInto(new BeamPartitionReader<>(partition), values);
    return values;
  }

  private static void drainInto(BeamPartitionReader<String> reader, List<String> values)
      throws IOException {
    while (reader.next()) {
      InternalRow row = reader.get();
      values.add(decode(row.getBinary(0)));
    }
    reader.close();
  }

  /** Opens and drains a batch expected to fail, returns what it delivered before failing. */
  private static List<String> drainUntilFailure(
      BeamInputPartition<String> partition, Class<? extends Exception> failure) throws IOException {
    BeamPartitionReader<String> reader = new BeamPartitionReader<>(partition);
    List<String> values = new ArrayList<>();
    assertThrows(failure, () -> drainInto(reader, values));
    return values;
  }

  private List<String> shardZero(int... indexes) {
    List<String> elements = new ArrayList<>();
    for (int index : indexes) {
      elements.add(TestSource.element(tag, 0, index));
    }
    return elements;
  }

  // ---------------------------------------------------------------------------------------------
  // the shared in memory UnboundedSource
  // ---------------------------------------------------------------------------------------------

  /**
   * Splits into one sub source per shard, each over {@code count / shards} elements named {@code
   * <tag>-<shard>-<index>} with evenly spaced timestamps. Marks are not Java serializable, they
   * record the position they finalize under {@code <tag>/<shard>}, readers are counted per tag.
   */
  static final class TestSource extends UnboundedSource<String, TestSource.Mark> {
    private static final long serialVersionUID = 1L;

    static final Coder<Mark> MARK_CODER = new MarkCoder();

    private static final ConcurrentMap<String, List<Integer>> FINALIZED = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, AtomicInteger> CREATED = new ConcurrentHashMap<>();

    private final String tag;
    private final int shard;
    private final int shards;
    private final int perShard;

    TestSource(String tag, int shards, int count) {
      this(tag, -1, shards, count / shards);
    }

    private TestSource(String tag, int shard, int shards, int perShard) {
      this.tag = tag;
      this.shard = shard;
      this.shards = shards;
      this.perShard = perShard;
    }

    static Set<String> elements(String tag, int shards, int count) {
      Set<String> elements = new HashSet<>();
      for (int shard = 0; shard < shards; shard++) {
        for (int index = 0; index < count / shards; index++) {
          elements.add(element(tag, shard, index));
        }
      }
      return elements;
    }

    static String element(String tag, int shard, int index) {
      return tag + "-" + shard + "-" + index;
    }

    static int shardOf(String element) {
      String head = element.substring(0, element.lastIndexOf('-'));
      return Integer.parseInt(head.substring(head.lastIndexOf('-') + 1));
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

    static int created(String tag) {
      AtomicInteger created = CREATED.get(tag);
      return created == null ? 0 : created.get();
    }

    static void forget(String tag) {
      FINALIZED.keySet().removeIf(key -> key.startsWith(tag + "/"));
      CREATED.remove(tag);
    }

    private static String key(String tag, int shard) {
      return tag + "/" + shard;
    }

    @Override
    public List<TestSource> split(int desiredNumSplits, PipelineOptions options) {
      if (shard >= 0) {
        return Collections.singletonList(this);
      }
      List<TestSource> splits = new ArrayList<>();
      for (int i = 0; i < shards; i++) {
        splits.add(new TestSource(tag, i, shards, perShard));
      }
      return splits;
    }

    @Override
    public UnboundedReader<String> createReader(PipelineOptions options, @Nullable Mark mark) {
      if (shard < 0) {
        throw new IllegalStateException("split before reading");
      }
      CREATED.computeIfAbsent(tag, t -> new AtomicInteger()).incrementAndGet();
      return new Reader(this, mark == null ? 0 : mark.next);
    }

    @Override
    public Coder<Mark> getCheckpointMarkCoder() {
      return MARK_CODER;
    }

    @Override
    public Coder<String> getOutputCoder() {
      return StringUtf8Coder.of();
    }

    /** Position of the next element of a shard, deliberately not {@link Serializable}. */
    static final class Mark implements UnboundedSource.CheckpointMark {
      private final String tag;
      private final int shard;
      final int next;

      Mark(String tag, int shard, int next) {
        this.tag = tag;
        this.shard = shard;
        this.next = next;
      }

      @Override
      public void finalizeCheckpoint() {
        FINALIZED
            .computeIfAbsent(key(tag, shard), k -> Collections.synchronizedList(new ArrayList<>()))
            .add(next);
      }
    }

    private static final class MarkCoder extends CustomCoder<Mark> {
      private static final long serialVersionUID = 1L;

      @Override
      public void encode(Mark mark, OutputStream out) throws IOException {
        StringUtf8Coder.of().encode(mark.tag, out);
        VarIntCoder.of().encode(mark.shard, out);
        VarIntCoder.of().encode(mark.next, out);
      }

      @Override
      public Mark decode(InputStream in) throws IOException {
        return new Mark(
            StringUtf8Coder.of().decode(in),
            VarIntCoder.of().decode(in),
            VarIntCoder.of().decode(in));
      }
    }

    private static final class Reader extends UnboundedReader<String> {
      private final TestSource source;
      private int next;
      private int current = -1;

      Reader(TestSource source, int next) {
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
        return element(source.tag, source.shard, current);
      }

      @Override
      public Instant getCurrentTimestamp() throws NoSuchElementException {
        if (current < 0) {
          throw new NoSuchElementException();
        }
        return new Instant(
            BASE_MILLIS + (source.shard * source.perShard + current) * INTERVAL_MILLIS);
      }

      @Override
      public Instant getWatermark() {
        return current < 0 ? BoundedWindow.TIMESTAMP_MIN_VALUE : getCurrentTimestamp();
      }

      @Override
      public CheckpointMark getCheckpointMark() {
        return new Mark(source.tag, source.shard, next);
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
