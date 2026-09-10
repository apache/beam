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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.ValueKind;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link CommitWindows} (with {@link SplitLateData}), stage 2 of the CDC sink: event-time
 * commit windowing, {@code GroupByKey} per {@code (destination, shard)}, the late-pane dead-letter
 * split, and the {@code SortValues} sort by the byte {@code (pk, seq, kind)} key.
 *
 * <p>Pure-Beam tests, no Iceberg catalog: {@link TestStream} drives the watermark for the streaming
 * cases, {@link Create} provides bounded input for the batch cases, and {@link PAssert} checks
 * {@link CommitWindows.Result#getSortedGroups()} and {@link
 * CommitWindows.Result#getDeadLetterRows()}.
 */
@RunWith(JUnit4.class)
public class CommitWindowsTest {

  @Rule public transient TestPipeline p = TestPipeline.create();

  /** The CDC data schema for these tests: {@code id INT32}, {@code name STRING}. */
  private static final Schema DATA_SCHEMA =
      Schema.builder().addInt32Field("id").addStringField("name").build();

  /** {@link #DATA_SCHEMA} nested under {@code record}, plus the dead-letter metadata columns. */
  private static final Schema EXPECTED_DEAD_LETTER_SCHEMA =
      Schema.builder()
          .addRowField("record", DATA_SCHEMA)
          .addStringField("change_type")
          .addInt64Field("sequence_number")
          .addStringField("destination")
          .build();

  /** The stage-1 ({@link AssignCdcKeys#KEYED}) element coder. */
  private static final KvCoder<KV<String, Integer>, KV<byte[], CdcRecord>> INPUT_CODER =
      KvCoder.of(
          KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()),
          KvCoder.of(ByteArrayCoder.of(), CdcRecordCoder.of(DATA_SCHEMA)));

  /** The streaming commit-window size. */
  private static final Duration WINDOW = Duration.standardSeconds(60);

  /** A test-specific lateness bound, large enough to keep every late test pane in-window. */
  private static final Duration TEST_ALLOWED_LATENESS = Duration.standardDays(7);

  // ---------------------------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------------------------

  private static CdcWriteConfig config() {
    return CdcWriteConfig.builder().setSinkId("test-sink").setSorterMemoryMB(16).build();
  }

  /** Boundedness is derived from the input {@link PCollection}, not configured. */
  private static CommitWindows batchWindows() {
    return new CommitWindows(config(), /* triggeringFrequency= */ null, Duration.ZERO);
  }

  private static CommitWindows streamingWindows() {
    return new CommitWindows(config(), WINDOW, TEST_ALLOWED_LATENESS);
  }

  private static Row data(int id, String name) {
    return Row.withSchema(DATA_SCHEMA).addValues(id, name).build();
  }

  /** One primary key shared by every element, so a group's records sort purely by (seq, kind). */
  private static final byte[] PK = {42};

  /** Builds one stage-1 output element: {@code KV<KV<dest, shard>, KV<sortKey, CdcRecord>>}. */
  private static KV<KV<String, Integer>, KV<byte[], CdcRecord>> element(
      String dest, int shard, int id, String name, long seq, ValueKind kind) {
    return KV.of(
        KV.of(dest, shard),
        KV.of(CdcSortKey.encode(PK, seq, kind), CdcRecord.of(data(id, name), kind, seq)));
  }

  /** A {@link TimestampedValue} wrapping {@link #element}, for use with {@link TestStream}. */
  private static TimestampedValue<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> at(
      Instant ts, String dest, int shard, int id, String name, long seq, ValueKind kind) {
    return TimestampedValue.of(element(dest, shard, id, name, seq, kind), ts);
  }

  /** Sequence numbers, in encounter order, of a group's {@link CdcRecord}s. */
  private static List<Long> seqsOf(KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> group) {
    List<Long> seqs = new ArrayList<>();
    for (KV<byte[], CdcRecord> kv : group.getValue()) {
      seqs.add(kv.getValue().getSequenceNumber());
    }
    return seqs;
  }

  /** Change kinds, in encounter order, of a group's {@link CdcRecord}s. */
  private static List<ValueKind> kindsOf(
      KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> group) {
    List<ValueKind> kinds = new ArrayList<>();
    for (KV<byte[], CdcRecord> kv : group.getValue()) {
      kinds.add(kv.getValue().getKind());
    }
    return kinds;
  }

  /** Data-row {@code name} values, in encounter order, of a group's {@link CdcRecord}s. */
  private static List<String> namesOf(
      KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> group) {
    List<String> names = new ArrayList<>();
    for (KV<byte[], CdcRecord> kv : group.getValue()) {
      names.add(kv.getValue().getData().getString("name"));
    }
    return names;
  }

  /** Sums the committed values of the named {@link SplitLateData} counter (0 if never fired). */
  private static long counterTotal(PipelineResult result, String name) {
    Iterable<MetricResult<Long>> counters =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(SplitLateData.class, name))
                    .build())
            .getCounters();
    long total = 0;
    for (MetricResult<Long> counter : counters) {
      total += counter.getCommitted();
    }
    return total;
  }

  // ---------------------------------------------------------------------------------------------
  // 1. Batch (bounded): one global-window group per (dest, shard), sorted
  // ---------------------------------------------------------------------------------------------

  @Test
  public void batchGlobalWindowGroupsAndSortsBySeqAndKind() {
    // Records deliberately out of order, including an equal-seq (9, 9) pair where the
    // UPDATE_AFTER is added BEFORE the UPDATE_BEFORE: the byte sort key must order by seq,
    // then before-image (UPDATE_BEFORE) ahead of after-image (UPDATE_AFTER) at an equal seq.
    List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> input =
        ImmutableList.of(
            element("db.t", 0, 1, "b7", 7L, ValueKind.UPDATE_AFTER),
            element("db.t", 0, 1, "a5", 5L, ValueKind.INSERT),
            element("db.t", 0, 2, "ua9", 9L, ValueKind.UPDATE_AFTER),
            element("db.t", 0, 2, "ub9", 9L, ValueKind.UPDATE_BEFORE));

    CommitWindows.Result r = p.apply(Create.of(input).withCoder(INPUT_CODER)).apply(batchWindows());

    PAssert.that(r.getSortedGroups())
        .satisfies(
            groups -> {
              KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> g =
                  Iterables.getOnlyElement(groups);
              assertThat(g.getKey(), equalTo(KV.of("db.t", 0)));
              assertThat(seqsOf(g), contains(5L, 7L, 9L, 9L));
              assertThat(
                  kindsOf(g),
                  contains(
                      ValueKind.INSERT,
                      ValueKind.UPDATE_AFTER,
                      ValueKind.UPDATE_BEFORE,
                      ValueKind.UPDATE_AFTER));
              return null;
            });
    PAssert.that(r.getDeadLetterRows()).empty();
    p.run().waitUntilFinish();
  }

  // ---------------------------------------------------------------------------------------------
  // 2. Shard separation: two shards -> two groups
  // ---------------------------------------------------------------------------------------------

  @Test
  public void differentShardsProduceSeparateGroups() {
    List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> input =
        ImmutableList.of(
            element("db.t", 0, 1, "a", 1L, ValueKind.INSERT),
            element("db.t", 1, 2, "b", 2L, ValueKind.INSERT));

    CommitWindows.Result r = p.apply(Create.of(input).withCoder(INPUT_CODER)).apply(batchWindows());

    PAssert.that(r.getSortedGroups())
        .satisfies(
            groups -> {
              List<KV<String, Integer>> keys = new ArrayList<>();
              for (KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> g : groups) {
                keys.add(g.getKey());
                assertThat(Iterables.size(g.getValue()), equalTo(1));
              }
              assertThat(keys, containsInAnyOrder(KV.of("db.t", 0), KV.of("db.t", 1)));
              return null;
            });
    PAssert.that(r.getDeadLetterRows()).empty();
    p.run().waitUntilFinish();
  }

  // ---------------------------------------------------------------------------------------------
  // 4. Streaming: two event-time windows -> two groups, each sorted
  // ---------------------------------------------------------------------------------------------

  @Test
  public void streamingWindowsProduceSeparateSortedGroups() {
    Instant t0 = new Instant(0);
    TestStream<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> stream =
        TestStream.create(INPUT_CODER)
            // Window [0, 60s): two records added out of sequence order.
            .addElements(
                at(
                    t0.plus(Duration.millis(1_000)),
                    "db.t",
                    0,
                    1,
                    "w1b",
                    7L,
                    ValueKind.UPDATE_AFTER))
            .addElements(
                at(t0.plus(Duration.millis(1_500)), "db.t", 0, 1, "w1a", 5L, ValueKind.INSERT))
            .advanceWatermarkTo(t0.plus(Duration.standardSeconds(70))) // close window [0, 60s)
            // Window [60s, 120s):
            .addElements(
                at(t0.plus(Duration.millis(61_000)), "db.t", 0, 2, "w2", 9L, ValueKind.INSERT))
            .advanceWatermarkToInfinity();

    CommitWindows.Result r = p.apply(stream).apply(streamingWindows());

    PAssert.that(r.getSortedGroups())
        .satisfies(
            groups -> {
              Set<List<Long>> seqGroups = new HashSet<>();
              for (KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> g : groups) {
                assertThat(g.getKey(), equalTo(KV.of("db.t", 0)));
                seqGroups.add(seqsOf(g));
              }
              assertThat(
                  seqGroups,
                  equalTo(ImmutableSet.of(ImmutableList.of(5L, 7L), ImmutableList.of(9L))));
              return null;
            });
    PAssert.that(r.getDeadLetterRows()).empty();
    p.run().waitUntilFinish();
  }

  // ---------------------------------------------------------------------------------------------
  // 5. Late non-first pane -> replayable dead letters (+ metric); on-time group unaffected
  // ---------------------------------------------------------------------------------------------

  @Test
  public void lateNonFirstPaneDivertsToReplayableDeadLetters() {
    Instant t0 = new Instant(0);
    TestStream<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> stream =
        TestStream.create(INPUT_CODER)
            // On-time element; the watermark then closes window [0, 60s) and fires its
            // on-time pane.
            .addElements(
                at(t0.plus(Duration.millis(1_000)), "db.t", 0, 1, "first", 1L, ValueKind.INSERT))
            .advanceWatermarkTo(t0.plus(Duration.standardSeconds(70)))
            // Two elements timestamped INSIDE the already-fired window arrive late (within
            // allowed lateness): every record of a non-first late pane becomes one dead letter.
            .addElements(
                at(
                    t0.plus(Duration.millis(2_000)),
                    "db.t",
                    0,
                    2,
                    "late1",
                    2L,
                    ValueKind.UPDATE_AFTER),
                at(t0.plus(Duration.millis(3_000)), "db.t", 0, 3, "late2", 3L, ValueKind.DELETE))
            .advanceWatermarkToInfinity();

    CommitWindows.Result r = p.apply(stream).apply(streamingWindows());

    assertThat(r.getDeadLetterSchema(), equalTo(EXPECTED_DEAD_LETTER_SCHEMA));

    PAssert.that(r.getSortedGroups())
        .satisfies(
            groups -> {
              KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> g =
                  Iterables.getOnlyElement(groups);
              assertThat(g.getKey(), equalTo(KV.of("db.t", 0)));
              assertThat(seqsOf(g), contains(1L)); // only the on-time record
              return null;
            });
    // Row.equals compares schemas too, so this pins the exact dead-letter schema AND values:
    // nested data row + change type name + sequence number + destination string.
    PAssert.that(r.getDeadLetterRows())
        .containsInAnyOrder(
            Row.withSchema(EXPECTED_DEAD_LETTER_SCHEMA)
                .addValues(data(2, "late1"), "UPDATE_AFTER", 2L, "db.t")
                .build(),
            Row.withSchema(EXPECTED_DEAD_LETTER_SCHEMA)
                .addValues(data(3, "late2"), "DELETE", 3L, "db.t")
                .build());

    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(counterTotal(result, "deadLetterRecords"), equalTo(2L));
  }

  // ---------------------------------------------------------------------------------------------
  // 6. First-late pane is diverted too
  // ---------------------------------------------------------------------------------------------

  /**
   * A window whose FIRST pane is late is dead-lettered like any other late pane. The pane's timing
   * is a fact about one {@code (destination, shard, window)}; the committer's already-committed
   * skip is per {@code (destination, window)}. A shard that saw no on-time data therefore has a
   * first pane that is late even when its destination-window committed long ago, so {@code
   * isFirst()} cannot be used to let records through.
   */
  @Test
  public void firstLatePaneIsAlsoDivertedToDeadLetters() {
    Instant t0 = new Instant(0);
    TestStream<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> stream =
        TestStream.create(INPUT_CODER)
            // The watermark passes the end of window [0, 60s) with NO data for this key ...
            .advanceWatermarkTo(t0.plus(Duration.standardSeconds(70)))
            // ... then the window's ONLY records arrive, late.
            .addElements(
                at(t0.plus(Duration.millis(2_000)), "db.t", 0, 1, "jitter", 4L, ValueKind.INSERT))
            .advanceWatermarkToInfinity();

    CommitWindows.Result r = p.apply(stream).apply(streamingWindows());

    PAssert.that(r.getSortedGroups()).empty();
    PAssert.that(r.getDeadLetterRows())
        .containsInAnyOrder(
            Row.withSchema(EXPECTED_DEAD_LETTER_SCHEMA)
                .addValues(data(1, "jitter"), "INSERT", 4L, "db.t")
                .build());

    PipelineResult result = p.run();
    result.waitUntilFinish();
    assertThat(counterTotal(result, "deadLetterRecords"), equalTo(1L));
  }

  // ---------------------------------------------------------------------------------------------
  // 7. Equal sort keys: both records survive the sort
  // ---------------------------------------------------------------------------------------------

  @Test
  public void equalSortKeysBothSurviveSort() {
    // Identical (seq, kind) -> byte-identical sort keys; the sort must keep both records.
    List<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> input =
        ImmutableList.of(
            element("db.t", 0, 1, "first", 5L, ValueKind.INSERT),
            element("db.t", 0, 2, "second", 5L, ValueKind.INSERT));

    CommitWindows.Result r = p.apply(Create.of(input).withCoder(INPUT_CODER)).apply(batchWindows());

    PAssert.that(r.getSortedGroups())
        .satisfies(
            groups -> {
              KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> g =
                  Iterables.getOnlyElement(groups);
              assertThat(seqsOf(g), contains(5L, 5L));
              assertThat(namesOf(g), containsInAnyOrder("first", "second"));
              return null;
            });
    p.run().waitUntilFinish();
  }
}
