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
package org.apache.beam.sdk.loadtests;

import static org.apache.beam.sdk.io.synthetic.SyntheticOptions.fromJsonString;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.LengthPrefixCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.io.synthetic.SyntheticBoundedSource;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticUnboundedSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Distributed Dataflow benchmark pipeline for validating Wave 1 core Beam performance optimizations
 * ({@link Row} builders, {@link LengthPrefixCoder}, {@link BigEndianLongCoder}, {@link
 * org.apache.beam.sdk.coders.InstantCoder}, {@link org.apache.beam.sdk.values.WindowedValues},
 * {@link org.apache.beam.sdk.transforms.windowing.PaneInfo}, {@link
 * org.apache.beam.sdk.transforms.DoFnOutputReceivers}, and {@link
 * org.apache.beam.runners.core.SimpleDoFnRunner}).
 */
@SuppressWarnings({"nullness"})
public class Wave1PerfBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(Wave1PerfBenchmark.class);

  private static final long BASE_TIMESTAMP_MS = 1_700_000_000_000L;
  private static final int NUM_WINDOWS = 10;

  private static final String[] REGIONS = {"us-central1", "us-east1", "europe-west1", "asia-east1"};
  private static final String[] EVENT_TYPES = {"click", "view", "purchase", "checkout", "search"};
  private static final String[] DEVICE_OS = {"android", "ios", "web", "desktop"};

  private static final Schema EVENT_SCHEMA =
      Schema.builder()
          .addStringField("event_id")
          .addInt64Field("user_id")
          .addStringField("region")
          .addStringField("event_type")
          .addStringField("device_os")
          .addInt64Field("amount_cents")
          .addInt32Field("latency_ms")
          .addDoubleField("score")
          .addBooleanField("is_valid")
          .addByteArrayField("payload")
          .build();

  private static final Schema SUMMARY_SCHEMA =
      Schema.builder()
          .addByteArrayField("group_key")
          .addStringField("sample_label")
          .addInt64Field("event_count")
          .addInt64Field("total_amount")
          .addInt64Field("max_amount")
          .addDoubleField("avg_amount")
          .build();

  private static final Coder<KV<byte[], KV<String, Long>>> SHUFFLE_KV_CODER =
      KvCoder.of(
          LengthPrefixCoder.of(ByteArrayCoder.of()),
          LengthPrefixCoder.of(KvCoder.of(StringUtf8Coder.of(), BigEndianLongCoder.of())));

  private static final TupleTag<Row> MAIN_ROW_TAG = new TupleTag<Row>() {};
  private static final TupleTag<Row> AUDIT_ROW_TAG = new TupleTag<Row>() {};

  private static final TupleTag<KV<byte[], KV<String, Long>>> MAIN_KV_TAG =
      new TupleTag<KV<byte[], KV<String, Long>>>() {};
  private static final TupleTag<KV<byte[], KV<String, Long>>> AUDIT_KV_TAG =
      new TupleTag<KV<byte[], KV<String, Long>>>() {};

  /** Pipeline options for {@link Wave1PerfBenchmark}. */
  public interface Options extends PipelineOptions {
    @Description(
        "Workload profile: 'etl' (Schema Row + ParDo + Shuffle) or 'core' (non-Row ParDo + Shuffle)")
    @Default.String("etl")
    String getWorkload();

    void setWorkload(String workload);

    @Description("Total number of synthetic input records")
    @Default.Long(300_000_000L)
    Long getNumRecords();

    void setNumRecords(Long numRecords);

    @Description("Number of initial bundles for SyntheticBoundedSource")
    @Default.Integer(128)
    Integer getInitialBundles();

    void setInitialBundles(Integer initialBundles);

    @Description("Number of distinct shuffle keys per window")
    @Default.Integer(500_000)
    Integer getNumShuffleKeys();

    void setNumShuffleKeys(Integer numShuffleKeys);

    @Description("Emit 1 out of every shuffleMod records to GroupByKey (1 = 100%, 2 = 50%)")
    @Default.Integer(2)
    Integer getShuffleMod();

    void setShuffleMod(Integer shuffleMod);

    @Description("Number of chained ParDo steps in 'core' workload")
    @Default.Integer(4)
    Integer getParDoChainDepth();

    void setParDoChainDepth(Integer depth);

    @Description("Fixed window size in seconds when running with --streaming=true")
    @Default.Integer(30)
    Integer getStreamingWindowSec();

    void setStreamingWindowSec(Integer streamingWindowSec);

    @Description("Whether to block waiting for pipeline completion")
    @Default.Boolean(false)
    Boolean getWaitUntilFinish();

    void setWaitUntilFinish(Boolean waitUntilFinish);
  }

  public static void main(String[] args) throws IOException {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline pipeline = Pipeline.create(options);

    String sourceJson =
        String.format(
            "{\"numRecords\": %d, \"keySizeBytes\": 16, \"valueSizeBytes\": 64, \"forceNumInitialBundles\": %d}",
            options.getNumRecords(), options.getInitialBundles());
    SyntheticSourceOptions sourceOptions = fromJsonString(sourceJson, SyntheticSourceOptions.class);

    boolean isStreaming = options.as(StreamingOptions.class).isStreaming();
    PCollection<KV<byte[], byte[]>> input =
        pipeline.apply(
            "ReadSyntheticInput",
            isStreaming
                ? Read.from(new SyntheticUnboundedSource(sourceOptions))
                : Read.from(new SyntheticBoundedSource(sourceOptions)));

    String workload = options.getWorkload();
    if ("etl".equalsIgnoreCase(workload)) {
      buildEtlPipeline(
          input,
          options.getNumShuffleKeys(),
          options.getShuffleMod(),
          isStreaming,
          options.getStreamingWindowSec());
    } else if ("core".equalsIgnoreCase(workload)) {
      buildCorePipeline(
          input,
          options.getNumShuffleKeys(),
          options.getShuffleMod(),
          options.getParDoChainDepth(),
          isStreaming,
          options.getStreamingWindowSec());
    } else {
      throw new IllegalArgumentException("Unsupported workload: " + workload);
    }

    PipelineResult result = pipeline.run();
    LOG.info("Submitted Wave1PerfBenchmark ({}) result: {}", workload, result);
    if (options.getWaitUntilFinish()) {
      PipelineResult.State state = result.waitUntilFinish();
      LOG.info("Pipeline finished with state: {}", state);
      if (state != PipelineResult.State.DONE) {
        throw new RuntimeException("Pipeline failed with state: " + state);
      }
    }
  }

  private static void buildEtlPipeline(
      PCollection<KV<byte[], byte[]>> input,
      int numShuffleKeys,
      int shuffleMod,
      boolean isStreaming,
      int streamingWindowSec) {
    Duration windowDuration =
        isStreaming ? Duration.standardSeconds(streamingWindowSec) : Duration.standardMinutes(1);
    PCollection<Row> parsedRows =
        input
            .apply("ParseEventRow", ParDo.of(new ParseEventRowFn(isStreaming)))
            .setCoder(RowCoder.of(EVENT_SCHEMA));

    PCollectionTuple routed =
        parsedRows.apply(
            "EnrichAndRouteRow",
            ParDo.of(new EnrichAndRouteRowFn())
                .withOutputTags(MAIN_ROW_TAG, TupleTagList.of(AUDIT_ROW_TAG)));

    routed
        .get(AUDIT_ROW_TAG)
        .setCoder(RowCoder.of(EVENT_SCHEMA))
        .apply("CountAuditRows", ParDo.of(new CountRowSinkFn("audit_rows", "amount_cents")));

    PCollection<Row> windowedRows =
        routed
            .get(MAIN_ROW_TAG)
            .setCoder(RowCoder.of(EVENT_SCHEMA))
            .apply("WindowFixed1m", Window.into(FixedWindows.of(windowDuration)))
            .apply("ValidateRowPass", ParDo.of(new PassThroughRowFn()))
            .setCoder(RowCoder.of(EVENT_SCHEMA));

    PCollection<KV<byte[], KV<String, Long>>> keyed =
        windowedRows
            .apply("ExtractShuffleKV", ParDo.of(new RowToShuffleKvFn(numShuffleKeys, shuffleMod)))
            .setCoder(SHUFFLE_KV_CODER);

    keyed
        .apply("GroupByShuffleKey", GroupByKey.create())
        .apply("AggregateSummaryRow", ParDo.of(new AggregateToSummaryRowFn()))
        .setCoder(RowCoder.of(SUMMARY_SCHEMA))
        .apply("CountSummaryRows", ParDo.of(new CountRowSinkFn("summary_rows", "total_amount")));
  }

  private static void buildCorePipeline(
      PCollection<KV<byte[], byte[]>> input,
      int numShuffleKeys,
      int shuffleMod,
      int parDoChainDepth,
      boolean isStreaming,
      int streamingWindowSec) {
    Duration windowDuration =
        isStreaming ? Duration.standardSeconds(streamingWindowSec) : Duration.standardMinutes(1);
    PCollection<KV<byte[], KV<String, Long>>> current =
        input
            .apply(
                "AssignTimestampsAndExtractKV",
                ParDo.of(new RawToShuffleKvFn(numShuffleKeys, shuffleMod, isStreaming)))
            .setCoder(SHUFFLE_KV_CODER)
            .apply("WindowFixed1m", Window.into(FixedWindows.of(windowDuration)));

    for (int step = 0; step < parDoChainDepth; step++) {
      PCollectionTuple tuple =
          current.apply(
              "CoreParDoStep_" + step,
              ParDo.of(new CoreTransformKvFn(step))
                  .withOutputTags(MAIN_KV_TAG, TupleTagList.of(AUDIT_KV_TAG)));
      tuple
          .get(AUDIT_KV_TAG)
          .setCoder(SHUFFLE_KV_CODER)
          .apply("CountAuditKv_" + step, ParDo.of(new CountKvSinkFn("audit_kv_" + step)));
      current = tuple.get(MAIN_KV_TAG).setCoder(SHUFFLE_KV_CODER);
    }

    current
        .apply("GroupByShuffleKey", GroupByKey.create())
        .apply("AggregateGroupedKV", ParDo.of(new AggregateKvFn()))
        .setCoder(SHUFFLE_KV_CODER)
        .apply("PostAggTransform", ParDo.of(new CorePostAggFn()))
        .setCoder(SHUFFLE_KV_CODER)
        .apply("CountFinalKV", ParDo.of(new CountKvSinkFn("final_kv")));
  }

  private static long bytesToLong(byte[] bytes) {
    long h = 0L;
    int len = Math.min(bytes.length, 8);
    for (int i = 0; i < len; i++) {
      h = (h << 8) | (bytes[i] & 0xFFL);
    }
    return h;
  }

  private static byte[] makeGroupKey(long userId, int numShuffleKeys) {
    int bucket = Math.floorMod(userId, numShuffleKeys);
    byte[] out = new byte[12];
    out[0] = (byte) 'k';
    out[1] = (byte) 'e';
    out[2] = (byte) 'y';
    out[3] = (byte) ':';
    ByteBuffer.wrap(out, 4, 8).putLong(bucket);
    return out;
  }

  private static final class ParseEventRowFn extends DoFn<KV<byte[], byte[]>, Row> {
    private final boolean isStreaming;
    private final Counter inputCounter = Metrics.counter("wave1_perf", "input_records");
    private final Distribution sourceTimeMs = Metrics.distribution("wave1_perf", "source_time_ms");
    private long localCount;

    ParseEventRowFn(boolean isStreaming) {
      this.isStreaming = isStreaming;
    }

    @StartBundle
    public void startBundle() {
      localCount = 0;
    }

    @ProcessElement
    public void processElement(@Element KV<byte[], byte[]> elem, OutputReceiver<Row> out) {
      localCount++;
      long hash = bytesToLong(elem.getKey());
      long absHash = hash == Long.MIN_VALUE ? 0L : Math.abs(hash);

      Row row =
          Row.withSchema(EVENT_SCHEMA)
              .withFieldValue("event_id", "evt-" + Long.toHexString(absHash))
              .withFieldValue("user_id", absHash)
              .withFieldValue("region", REGIONS[(int) (absHash % REGIONS.length)])
              .withFieldValue(
                  "event_type", EVENT_TYPES[(int) ((absHash >>> 4) % EVENT_TYPES.length)])
              .withFieldValue("device_os", DEVICE_OS[(int) ((absHash >>> 8) % DEVICE_OS.length)])
              .withFieldValue("amount_cents", (absHash % 100_000L) + 1L)
              .withFieldValue("latency_ms", (int) (absHash % 5_000L))
              .withFieldValue("score", (absHash % 10_000L) / 100.0)
              .withFieldValue("is_valid", (absHash & 1L) == 0L)
              .withFieldValue("payload", Arrays.copyOf(elem.getValue(), 24))
              .build();

      if (isStreaming) {
        out.output(row);
      } else {
        int windowIdx = (int) (absHash % NUM_WINDOWS);
        long tsMs = BASE_TIMESTAMP_MS + (windowIdx * 60_000L) + (absHash % 59_000L);
        out.outputWithTimestamp(row, new Instant(tsMs));
      }
    }

    @FinishBundle
    public void finishBundle() {
      if (localCount > 0) {
        inputCounter.inc(localCount);
        sourceTimeMs.update(System.currentTimeMillis());
      }
    }
  }

  private static final class EnrichAndRouteRowFn extends DoFn<Row, Row> {
    @ProcessElement
    public void processElement(@Element Row row, @Timestamp Instant ts, MultiOutputReceiver out) {
      long userId = row.getInt64("user_id");
      double updatedScore = row.getDouble("score") * 1.05 + 0.1;
      String enrichedRegion = row.getString("region") + "-v1";
      Row enriched =
          Row.fromRow(row)
              .withFieldValue("region", enrichedRegion)
              .withFieldValue("score", updatedScore)
              .withFieldValue("is_valid", true)
              .build();

      out.get(MAIN_ROW_TAG).outputWithTimestamp(enriched, ts);
      if (userId % 20 == 0) {
        out.get(AUDIT_ROW_TAG).output(enriched);
      }
    }
  }

  private static final class PassThroughRowFn extends DoFn<Row, Row> {
    @ProcessElement
    public void processElement(@Element Row row, @Timestamp Instant ts, OutputReceiver<Row> out) {
      if (Boolean.TRUE.equals(row.getBoolean("is_valid"))) {
        out.outputWithTimestamp(row, ts);
      }
    }
  }

  private static final class RowToShuffleKvFn extends DoFn<Row, KV<byte[], KV<String, Long>>> {
    private final int numShuffleKeys;
    private final int shuffleMod;

    RowToShuffleKvFn(int numShuffleKeys, int shuffleMod) {
      this.numShuffleKeys = numShuffleKeys;
      this.shuffleMod = Math.max(1, shuffleMod);
    }

    @ProcessElement
    public void processElement(@Element Row row, OutputReceiver<KV<byte[], KV<String, Long>>> out) {
      long userId = row.getInt64("user_id");
      if (shuffleMod > 1 && (userId % shuffleMod) != 0) {
        return;
      }
      byte[] key = makeGroupKey(userId, numShuffleKeys);
      String label = row.getString("region") + ":" + row.getString("event_type");
      long amount = row.getInt64("amount_cents");
      out.output(KV.of(key, KV.of(label, amount)));
    }
  }

  private static final class AggregateToSummaryRowFn
      extends DoFn<KV<byte[], Iterable<KV<String, Long>>>, Row> {
    @ProcessElement
    public void processElement(
        @Element KV<byte[], Iterable<KV<String, Long>>> elem, OutputReceiver<Row> out) {
      long count = 0;
      long total = 0;
      long max = Long.MIN_VALUE;
      String sampleLabel = "";
      for (KV<String, Long> kv : elem.getValue()) {
        long v = kv.getValue();
        count++;
        total += v;
        if (v > max) {
          max = v;
        }
        if (count == 1) {
          sampleLabel = kv.getKey();
        }
      }
      if (count == 0) {
        return;
      }
      Row summary =
          Row.withSchema(SUMMARY_SCHEMA)
              .withFieldValue("group_key", elem.getKey())
              .withFieldValue("sample_label", sampleLabel)
              .withFieldValue("event_count", count)
              .withFieldValue("total_amount", total)
              .withFieldValue("max_amount", max)
              .withFieldValue("avg_amount", (double) total / (double) count)
              .build();
      out.output(summary);
    }
  }

  private static final class RawToShuffleKvFn
      extends DoFn<KV<byte[], byte[]>, KV<byte[], KV<String, Long>>> {
    private final int numShuffleKeys;
    private final int shuffleMod;
    private final boolean isStreaming;
    private final Counter inputCounter = Metrics.counter("wave1_perf", "input_records");
    private final Distribution sourceTimeMs = Metrics.distribution("wave1_perf", "source_time_ms");
    private long localCount;

    RawToShuffleKvFn(int numShuffleKeys, int shuffleMod, boolean isStreaming) {
      this.numShuffleKeys = numShuffleKeys;
      this.shuffleMod = Math.max(1, shuffleMod);
      this.isStreaming = isStreaming;
    }

    @StartBundle
    public void startBundle() {
      localCount = 0;
    }

    @ProcessElement
    public void processElement(
        @Element KV<byte[], byte[]> elem, OutputReceiver<KV<byte[], KV<String, Long>>> out) {
      localCount++;
      long hash = bytesToLong(elem.getKey());
      long absHash = hash == Long.MIN_VALUE ? 0L : Math.abs(hash);
      if (shuffleMod > 1 && (absHash % shuffleMod) != 0) {
        return;
      }
      byte[] key = makeGroupKey(absHash, numShuffleKeys);
      String label =
          REGIONS[(int) (absHash % REGIONS.length)]
              + ":"
              + EVENT_TYPES[(int) ((absHash >>> 4) % EVENT_TYPES.length)];
      long amount = (absHash % 100_000L) + 1L;
      KV<byte[], KV<String, Long>> outVal = KV.of(key, KV.of(label, amount));
      if (isStreaming) {
        out.output(outVal);
      } else {
        int windowIdx = (int) (absHash % NUM_WINDOWS);
        long tsMs = BASE_TIMESTAMP_MS + (windowIdx * 60_000L) + (absHash % 59_000L);
        out.outputWithTimestamp(outVal, new Instant(tsMs));
      }
    }

    @FinishBundle
    public void finishBundle() {
      if (localCount > 0) {
        inputCounter.inc(localCount);
        sourceTimeMs.update(System.currentTimeMillis());
      }
    }
  }

  private static final class CoreTransformKvFn
      extends DoFn<KV<byte[], KV<String, Long>>, KV<byte[], KV<String, Long>>> {
    private final int step;

    CoreTransformKvFn(int step) {
      this.step = step;
    }

    @ProcessElement
    public void processElement(
        @Element KV<byte[], KV<String, Long>> elem,
        @Timestamp Instant ts,
        MultiOutputReceiver out) {
      KV<String, Long> val = elem.getValue();
      long updated = val.getValue() + step + 1L;
      KV<byte[], KV<String, Long>> next = KV.of(elem.getKey(), KV.of(val.getKey(), updated));
      if ((step & 1) == 0) {
        out.get(MAIN_KV_TAG).output(next);
      } else {
        out.get(MAIN_KV_TAG).outputWithTimestamp(next, ts);
      }
      if ((updated & 0x3FL) == 0L) {
        out.get(AUDIT_KV_TAG).output(next);
      }
    }
  }

  private static final class AggregateKvFn
      extends DoFn<KV<byte[], Iterable<KV<String, Long>>>, KV<byte[], KV<String, Long>>> {
    @ProcessElement
    public void processElement(
        @Element KV<byte[], Iterable<KV<String, Long>>> elem,
        OutputReceiver<KV<byte[], KV<String, Long>>> out) {
      long total = 0;
      String sample = "";
      for (KV<String, Long> kv : elem.getValue()) {
        if (total == 0) {
          sample = kv.getKey();
        }
        total += kv.getValue();
      }
      out.output(KV.of(elem.getKey(), KV.of(sample, total)));
    }
  }

  private static final class CorePostAggFn
      extends DoFn<KV<byte[], KV<String, Long>>, KV<byte[], KV<String, Long>>> {
    @ProcessElement
    public void processElement(
        @Element KV<byte[], KV<String, Long>> elem,
        @Timestamp Instant ts,
        OutputReceiver<KV<byte[], KV<String, Long>>> out) {
      out.outputWithTimestamp(elem, ts);
    }
  }

  private static final class CountRowSinkFn extends DoFn<Row, Void> {
    private final Counter rowCounter;
    private final Counter checksumCounter;
    private final Distribution sinkTimeMs;
    private final String valueFieldName;
    private long localRows;
    private long localChecksum;

    CountRowSinkFn(String name, String valueFieldName) {
      this.rowCounter = Metrics.counter("wave1_perf", name + "_count");
      this.checksumCounter = Metrics.counter("wave1_perf", name + "_checksum");
      this.sinkTimeMs = Metrics.distribution("wave1_perf", name + "_time_ms");
      this.valueFieldName = valueFieldName;
    }

    @StartBundle
    public void startBundle() {
      localRows = 0;
      localChecksum = 0;
    }

    @ProcessElement
    public void processElement(@Element Row row) {
      localRows++;
      localChecksum += row.getInt64(valueFieldName);
    }

    @FinishBundle
    public void finishBundle() {
      if (localRows > 0) {
        rowCounter.inc(localRows);
        checksumCounter.inc(localChecksum);
        sinkTimeMs.update(System.currentTimeMillis());
      }
    }
  }

  private static final class CountKvSinkFn extends DoFn<KV<byte[], KV<String, Long>>, Void> {
    private final Counter kvCounter;
    private final Counter sumCounter;
    private final Distribution sinkTimeMs;
    private long localCount;
    private long localSum;

    CountKvSinkFn(String name) {
      this.kvCounter = Metrics.counter("wave1_perf", name + "_count");
      this.sumCounter = Metrics.counter("wave1_perf", name + "_sum");
      this.sinkTimeMs = Metrics.distribution("wave1_perf", name + "_time_ms");
    }

    @StartBundle
    public void startBundle() {
      localCount = 0;
      localSum = 0;
    }

    @ProcessElement
    public void processElement(@Element KV<byte[], KV<String, Long>> elem) {
      localCount++;
      localSum += elem.getValue().getValue();
    }

    @FinishBundle
    public void finishBundle() {
      if (localCount > 0) {
        kvCounter.inc(localCount);
        sumCounter.inc(localSum);
        sinkTimeMs.update(System.currentTimeMillis());
      }
    }
  }
}
