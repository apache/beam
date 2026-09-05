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
package org.apache.beam.runners.spark.structuredstreaming.translation.streaming.state;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.beam.runners.spark.StreamingTest;
import org.apache.beam.runners.spark.structuredstreaming.SparkSessionRule;
import org.apache.beam.runners.spark.structuredstreaming.translation.streaming.TwsTransformFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.joda.time.Duration;
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
 * Proves that {@link BeamStatefulProcessor} really runs inside Spark 4's {@code transformWithState}
 * from Java, on Scala 2.13, with a RocksDB state store and an event time watermark, and that both
 * hosted execution stacks produce the Beam results they are supposed to.
 *
 * <p>The streaming source is a plain file source over JSON files with {@code maxFilesPerTrigger=1},
 * so the micro-batch boundaries and therefore the watermark progression are deterministic and no
 * test depends on the wall clock. Each JSON record carries a Beam element timestamp in millis plus
 * a Base64 encoded {@link TwsTransformFactory} input row.
 *
 * <p>Results are collected with {@code foreachBatch}. The {@code memory} sink cannot be used, Spark
 * test JVMs run with {@code spark.kryo.registrationRequired=true} and its commit message is not a
 * registered class.
 */
@Category(StreamingTest.class)
@RunWith(JUnit4.class)
public class BeamStatefulProcessorTest implements Serializable {

  /**
   * Deliberately runs with the module default of {@code spark.kryo.registrationRequired=true} (see
   * {@code runners/spark/spark_runner.gradle}). Spark 4 broadcasts its own {@code
   * org.apache.spark.sql.execution.streaming.state.StateSchemaMetadata} to the executors through
   * the user Kryo instance for every {@code transformWithState} query, so a stateful query only
   * survives its first micro-batch because {@code SparkSessionFactory.SparkKryoRegistrator}
   * registers that class. Keeping the strict flag on here is what stops that registration from
   * silently rotting.
   */
  @ClassRule public static final SparkSessionRule SESSION = new SparkSessionRule();

  @Rule public transient TemporaryFolder temp = new TemporaryFolder();

  /** Output rows collected per query name, driver side. */
  private static final Map<String, List<byte[]>> COLLECTED = new ConcurrentHashMap<>();

  /** 2023-11-14T22:13:20Z, aligned on a ten second fixed window boundary. */
  private static final long BASE_MILLIS = 1_700_000_000_000L;

  private static final TupleTag<Object> MAIN_TAG = new TupleTag<Object>("main") {};

  @After
  public void tearDown() {
    COLLECTED.clear();
  }

  // ---------------------------------------------------------------------------------------------
  // Row codec, no Spark involved.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void testInputRowCodecRoundTrip() {
    byte[] key = "the-key".getBytes(UTF_8);
    byte[] payload = new byte[] {1, 2, 3, 0, -7};

    byte[] row = TwsTransformFactory.encodeInputRow(key, payload);
    assertArrayEquals(key, TwsTransformFactory.inputKey(row));
    assertArrayEquals(payload, TwsTransformFactory.inputPayload(row));
  }

  @Test
  public void testInputRowCodecHandlesEmptyKeyAndPayload() {
    byte[] row = TwsTransformFactory.encodeInputRow(new byte[0], new byte[0]);
    assertEquals(0, TwsTransformFactory.inputKey(row).length);
    assertEquals(0, TwsTransformFactory.inputPayload(row).length);
  }

  @Test
  public void testOutputRowCodecRoundTrip() {
    byte[] payload = new byte[] {9, 8, 7};
    for (int index : new int[] {0, 1, 127, 128, 100_000}) {
      byte[] row = TwsTransformFactory.encodeOutputRow(index, payload);
      assertEquals(index, TwsTransformFactory.outputTagIndex(row));
      assertArrayEquals(payload, TwsTransformFactory.outputPayload(row));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Real transformWithState queries.
  // ---------------------------------------------------------------------------------------------

  /**
   * A stateful {@code ParDo} in the global window: the running sum per key must survive across
   * micro-batches, which is only possible if the Beam state really landed in the Spark state store
   * and was read back on the next batch.
   */
  @Test
  public void testStatefulParDoRunsInTransformWithState() throws Exception {
    WindowingStrategy<?, ?> strategy = WindowingStrategy.globalDefault();
    Coder<WindowedValue<Long>> valueCoder =
        WindowedValues.getFullCoder(VarLongCoder.of(), GlobalWindow.Coder.INSTANCE);
    Coder<WindowedValue<KV<String, Long>>> outputCoder =
        WindowedValues.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of()), GlobalWindow.Coder.INSTANCE);

    BeamStatefulProcessorConfig config =
        BeamStatefulProcessorConfig.builder()
            .setMode(BeamStatefulProcessorConfig.Mode.STATEFUL_PARDO)
            .setDoFn(new RunningSumFn())
            .setKeyCoder(StringUtf8Coder.of())
            .setValueCoder(VarLongCoder.of())
            .setWindowingStrategy(strategy)
            .setMainOutputTag(MAIN_TAG)
            .setOutputCoders(
                Collections.singletonMap(
                    MAIN_TAG, KvCoder.of(StringUtf8Coder.of(), VarLongCoder.of())))
            .setOptionsSupplier(PipelineOptionsFactory::create)
            .setStepName("running-sum")
            .build();

    List<List<String>> batches = new ArrayList<>();
    batches.add(
        Lists.newArrayList(
            globalRecord("a", 1L, BASE_MILLIS),
            globalRecord("b", 10L, BASE_MILLIS + 1),
            globalRecord("a", 2L, BASE_MILLIS + 2)));
    batches.add(
        Lists.newArrayList(
            globalRecord("a", 4L, BASE_MILLIS + 1_000),
            globalRecord("b", 20L, BASE_MILLIS + 1_001)));

    List<byte[]> rows = runQuery("stateful-pardo", batches, config);

    List<KV<String, Long>> emitted = new ArrayList<>();
    for (byte[] row : rows) {
      assertEquals("only the main output tag is used", 0, TwsTransformFactory.outputTagIndex(row));
      emitted.add(
          CoderUtils.decodeFromByteArray(outputCoder, TwsTransformFactory.outputPayload(row))
              .getValue());
    }

    // Per key the running sums must be exactly the prefix sums, in order.
    Map<String, List<Long>> byKey = new HashMap<>();
    for (KV<String, Long> kv : emitted) {
      byKey.computeIfAbsent(kv.getKey(), k -> new ArrayList<>()).add(kv.getValue());
    }
    assertEquals("two keys expected", 2, byKey.size());
    assertEquals(Lists.newArrayList(1L, 3L, 7L), byKey.get("a"));
    assertEquals(Lists.newArrayList(10L, 30L), byKey.get("b"));
    assertFalse("the value coder must have been used", valueCoder.toString().isEmpty());
  }

  /**
   * A windowed {@code GroupByKey}: three ten second fixed windows worth of data, driven so that the
   * watermark passes the end of the first window while the query is still running. The grouped
   * output can only appear if the end-of-window timer was registered with Spark, survived a
   * checkpoint of the RocksDB timer state and fired through {@code handleExpiredTimer}.
   */
  @Test
  public void testGroupAlsoByWindowFiresOnTheEndOfWindowTimer() throws Exception {
    WindowingStrategy<?, ?> strategy =
        WindowingStrategy.of(FixedWindows.of(Duration.standardSeconds(10)))
            .withAllowedLateness(Duration.ZERO);
    Coder<WindowedValue<KV<String, Iterable<String>>>> outputCoder =
        WindowedValues.getFullCoder(
            KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of())),
            IntervalWindow.getCoder());

    BeamStatefulProcessorConfig config =
        BeamStatefulProcessorConfig.builder()
            .setMode(BeamStatefulProcessorConfig.Mode.GROUP_ALSO_BY_WINDOW)
            .setKeyCoder(StringUtf8Coder.of())
            .setValueCoder(StringUtf8Coder.of())
            .setWindowingStrategy(strategy)
            .setMainOutputTag(MAIN_TAG)
            .setOutputCoders(
                Collections.singletonMap(
                    MAIN_TAG,
                    KvCoder.of(StringUtf8Coder.of(), IterableCoder.of(StringUtf8Coder.of()))))
            .setOptionsSupplier(PipelineOptionsFactory::create)
            .setStepName("gabw")
            .build();

    IntervalWindow firstWindow =
        new IntervalWindow(new Instant(BASE_MILLIS), new Instant(BASE_MILLIS + 10_000));

    List<List<String>> batches = new ArrayList<>();
    // Batch 1: the whole first window. The watermark is still at zero here.
    batches.add(
        Lists.newArrayList(
            windowedRecord("a", "x", BASE_MILLIS, firstWindow),
            windowedRecord("a", "y", BASE_MILLIS + 3_000, firstWindow),
            windowedRecord("a", "z", BASE_MILLIS + 9_999, firstWindow)));
    // Batch 2: a much later element, which pushes the watermark past the first window's end.
    IntervalWindow lateWindow =
        new IntervalWindow(new Instant(BASE_MILLIS + 20_000), new Instant(BASE_MILLIS + 30_000));
    batches.add(
        Lists.newArrayList(windowedRecord("sentinel", "s", BASE_MILLIS + 20_000, lateWindow)));
    // Batch 3: one more element, so there is a micro-batch that actually sees the advanced
    // watermark and can therefore expire the first window's timer.
    IntervalWindow lastWindow =
        new IntervalWindow(new Instant(BASE_MILLIS + 30_000), new Instant(BASE_MILLIS + 40_000));
    batches.add(
        Lists.newArrayList(windowedRecord("sentinel", "t", BASE_MILLIS + 30_000, lastWindow)));

    List<byte[]> rows = runQuery("gabw", batches, config);

    List<WindowedValue<KV<String, Iterable<String>>>> emitted = new ArrayList<>();
    for (byte[] row : rows) {
      assertEquals(0, TwsTransformFactory.outputTagIndex(row));
      emitted.add(
          CoderUtils.decodeFromByteArray(outputCoder, TwsTransformFactory.outputPayload(row)));
    }

    // Only the first window is asserted on. Whether the sentinel's own window also fires depends on
    // Spark scheduling a no-data batch after the last file, which processAllAvailable does not
    // promise to wait for.
    List<WindowedValue<KV<String, Iterable<String>>>> forKeyA = new ArrayList<>();
    for (WindowedValue<KV<String, Iterable<String>>> candidate : emitted) {
      if ("a".equals(candidate.getValue().getKey())) {
        forKeyA.add(candidate);
      }
    }

    assertEquals("exactly one pane for the completed window, got " + emitted, 1, forKeyA.size());
    WindowedValue<KV<String, Iterable<String>>> pane = forKeyA.get(0);
    List<String> grouped = Lists.newArrayList(pane.getValue().getValue());
    Collections.sort(grouped);
    assertEquals(Lists.newArrayList("x", "y", "z"), grouped);
    assertEquals(
        "the pane must carry the window it belongs to",
        Collections.singletonList(firstWindow),
        Lists.newArrayList(pane.getWindows()));
    assertTrue("the on-time pane must be the first one", pane.getPaneInfo().isFirst());
    assertEquals(PaneInfo.Timing.ON_TIME, pane.getPaneInfo().getTiming());
  }

  // ---------------------------------------------------------------------------------------------
  // Harness.
  // ---------------------------------------------------------------------------------------------

  /** A stateful DoFn keeping a running sum per key, the simplest thing that needs Beam state. */
  private static class RunningSumFn extends DoFn<KV<String, Long>, KV<String, Long>> {

    @StateId("sum")
    private final StateSpec<ValueState<Long>> sumSpec = StateSpecs.value(VarLongCoder.of());

    @ProcessElement
    public void process(
        @Element KV<String, Long> element,
        @StateId("sum") ValueState<Long> sum,
        OutputReceiver<KV<String, Long>> out) {
      Long current = sum.read();
      long updated = (current == null ? 0L : current) + element.getValue();
      sum.write(updated);
      out.output(KV.of(element.getKey(), updated));
    }
  }

  /**
   * Runs one {@code transformWithState} query over {@code batches}, one JSON file per batch and one
   * file per trigger, and returns every output row the query produced.
   */
  private List<byte[]> runQuery(
      String queryName, List<List<String>> batches, BeamStatefulProcessorConfig config)
      throws Exception {

    File input = temp.newFolder(queryName + "-input");
    long now = System.currentTimeMillis();
    for (int i = 0; i < batches.size(); i++) {
      File file = new File(input, String.format("%03d.json", i));
      Files.write(file.toPath(), String.join("\n", batches.get(i)).getBytes(UTF_8));
      // Spark's file stream source orders files by modification time only. Three files written in
      // the same millisecond tie and are then consumed in an arbitrary order, which for an event
      // time test means arbitrary watermark progression. Space the timestamps out explicitly.
      assertTrue(
          "could not set the modification time of " + file,
          file.setLastModified(now - (batches.size() - i) * 60_000L));
    }

    COLLECTED.put(queryName, Collections.synchronizedList(new ArrayList<>()));

    Dataset<Row> raw =
        SESSION
            .getSession()
            .readStream()
            .schema("ts BIGINT, payload STRING")
            .option("maxFilesPerTrigger", 1)
            .option("latestFirst", false)
            .json(input.getAbsolutePath());

    Dataset<byte[]> keyed =
        raw.withColumn("eventTime", functions.expr("timestamp_millis(ts)"))
            .withWatermark("eventTime", "0 seconds")
            .map(
                (MapFunction<Row, byte[]>)
                    row -> Base64.getDecoder().decode(row.<String>getAs("payload")),
                Encoders.BINARY());

    Dataset<byte[]> transformed = TwsTransformFactory.transform(keyed, config);

    StreamingQuery query =
        transformed
            .writeStream()
            .foreachBatch(
                (VoidFunction2<Dataset<byte[]>, Long>)
                    (batch, batchId) -> {
                      List<byte[]> target = COLLECTED.get(queryName);
                      if (target != null) {
                        target.addAll(batch.collectAsList());
                      }
                    })
            .queryName(queryName)
            .outputMode("append")
            .option("checkpointLocation", temp.newFolder(queryName + "-cp").getAbsolutePath())
            .start();

    try {
      query.processAllAvailable();
    } finally {
      query.stop();
    }
    if (query.exception().isDefined()) {
      throw new IllegalStateException(
          "streaming query failed: " + query.exception().get().toString());
    }
    return new ArrayList<>(COLLECTED.get(queryName));
  }

  /** One JSON record holding a global window element. */
  private static String globalRecord(String key, long value, long timestampMs) throws IOException {
    WindowedValue<Long> windowedValue =
        WindowedValues.of(
            value, new Instant(timestampMs), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
    return record(
        timestampMs,
        key,
        StringUtf8Coder.of(),
        windowedValue,
        WindowedValues.getFullCoder(VarLongCoder.of(), GlobalWindow.Coder.INSTANCE));
  }

  /** One JSON record holding an element already assigned to {@code window}. */
  private static String windowedRecord(
      String key, String value, long timestampMs, BoundedWindow window) throws IOException {
    WindowedValue<String> windowedValue =
        WindowedValues.of(value, new Instant(timestampMs), window, PaneInfo.NO_FIRING);
    return record(
        timestampMs,
        key,
        StringUtf8Coder.of(),
        windowedValue,
        WindowedValues.getFullCoder(StringUtf8Coder.of(), IntervalWindow.getCoder()));
  }

  private static <K, V> String record(
      long timestampMs,
      K key,
      Coder<K> keyCoder,
      WindowedValue<V> value,
      Coder<WindowedValue<V>> valueCoder)
      throws IOException {
    byte[] row =
        TwsTransformFactory.encodeInputRow(
            CoderUtils.encodeToByteArray(keyCoder, key),
            CoderUtils.encodeToByteArray(valueCoder, value));
    return "{\"ts\": "
        + timestampMs
        + ", \"payload\": \""
        + Base64.getEncoder().encodeToString(row)
        + "\"}";
  }
}
