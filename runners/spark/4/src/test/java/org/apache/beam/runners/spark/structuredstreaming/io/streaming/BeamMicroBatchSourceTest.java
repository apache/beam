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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.SparkStructuredStreamingPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

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

  /** 2023-11-14T22:13:20Z, a plain modern timestamp with no rebase or DST subtleties. */
  private static final long BASE_MILLIS = 1_700_000_000_000L;

  private static final long INTERVAL_MILLIS = 1_000L;

  private static final long POLL_TIMEOUT_MILLIS = 120_000L;

  @After
  public void tearDown() {
    BeamReaderCache.invalidateAll();
    COLLECTED.clear();
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
  @Test(timeout = 300_000)
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
  @Test(timeout = 300_000)
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
  @Test(timeout = 300_000)
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
  @Test(timeout = 300_000)
  public void testUnlimitedRecordsPerBatchByDefault() throws Exception {
    int count = 2500;
    SparkStructuredStreamingPipelineOptions options =
        org.apache.beam.sdk.options.PipelineOptionsFactory.create()
            .as(SparkStructuredStreamingPipelineOptions.class);
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

  /** The offset is an opaque, strictly increasing epoch counter. */
  @Test(timeout = 300_000)
  public void testEpochOffsetRoundTrip() {
    BeamOffset offset = new BeamOffset(42L);
    assertEquals("{\"epoch\":42}", offset.json());
    assertEquals(offset, BeamOffset.fromJson(offset.json()));
    assertEquals(0L, BeamOffset.ZERO.epoch());
  }

  // ---------------------------------------------------------------------------------------------
  // helpers
  // ---------------------------------------------------------------------------------------------

  private Dataset<Row> rows(int count, long watermarkDelayMillis) {
    SparkStructuredStreamingPipelineOptions options =
        org.apache.beam.sdk.options.PipelineOptionsFactory.create()
            .as(SparkStructuredStreamingPipelineOptions.class);
    options.setWatermarkDelayMillis(watermarkDelayMillis);
    options.setMaxRecordsPerBatch(1000L);
    options.setMaxBatchDurationMillis(200L);
    return UnboundedSourceDataset.of(
        SESSION.getSession(), new ListSource(count), coder(), options, "Read(ListSource)");
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
}
