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
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.extensions.sorter.BufferedExternalSorter;
import org.apache.beam.sdk.extensions.sorter.SortValues;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;

/**
 * Windows sharded, sort-keyed records into event-time commit windows, grouped by the {@code
 * KV<destination, shard>} key. Each {@code (destination, shard, window)} becomes one commit unit
 * group and the event-time watermark is the commit barrier. Each group is sorted by the byte sort
 * key applied from {@link AssignCdcKeys}: one primary key's records contiguous, in {@code (seq,
 * kind)} order within the key.
 *
 * <p>Late panes are routed to the DLQ (via {@link SplitLateData}) before any ordering happens. This
 * is necessary because the downstream commit step skips panes if their window token is present in
 * an already committed snapshot. If late panes are let through, their contents will never get to
 * the table.
 *
 * <p>Windowing by input mode:
 *
 * <ul>
 *   <li><b>Bounded</b>: a single {@link GlobalWindows} window. One commit per destination, no late
 *       data possible.
 *   <li><b>Unbounded</b>: event-time {@link FixedWindows} of {@code triggeringFrequency}, firing on
 *       the watermark with late firings per element, discarding fired panes.
 * </ul>
 */
final class CommitWindows
    extends PTransform<
        PCollection<KV<KV<String, Integer>, KV<byte[], CdcRecord>>>, CommitWindows.Result> {

  private static final TupleTag<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>>
      ON_TIME_TAG = new TupleTag<>("onTime");
  private static final TupleTag<Row> DEAD_LETTER_TAG = new TupleTag<>("deadLetter");

  private final CdcWriteConfig config;
  private final @Nullable Duration triggeringFrequency;
  private final Duration allowedLateness;

  CommitWindows(
      CdcWriteConfig config, @Nullable Duration triggeringFrequency, Duration allowedLateness) {
    this.config = config;
    this.triggeringFrequency = triggeringFrequency;
    this.allowedLateness = allowedLateness;
  }

  @Override
  public Result expand(PCollection<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> input) {
    Schema deadLetterSchema = SplitLateData.deadLetterSchema(dataSchemaOf(input.getCoder()));

    PCollection<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> windowed = applyCommitWindow(input);

    // Exactly one group per (destination, shard, window)
    PCollection<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> grouped =
        windowed.apply("GroupByShardKey", GroupByKey.create());

    // Late-data split before sorting anything
    PCollectionTuple split =
        grouped.apply(
            "SplitLateData",
            ParDo.of(new SplitLateData(deadLetterSchema, ON_TIME_TAG, DEAD_LETTER_TAG))
                .withOutputTags(ON_TIME_TAG, TupleTagList.of(DEAD_LETTER_TAG)));
    PCollection<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> onTimeUnsorted =
        split.get(ON_TIME_TAG).setCoder(grouped.getCoder());
    PCollection<Row> deadLetter =
        split.get(DEAD_LETTER_TAG).setCoder(RowCoder.of(deadLetterSchema));

    // Sort each surviving group's records by the byte sort key. The secondary key is byte[] +
    // ByteArrayCoder, so SortValues compares the raw CdcSortKey bytes (no coder framing):
    // each primary key's records come out contiguous, in (seq, kind) order within the key.
    PCollection<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> sorted =
        onTimeUnsorted.apply(
            "SortBySeqKind",
            SortValues.create(
                BufferedExternalSorter.options().withMemoryMB(config.getSorterMemoryMB())));

    return new Result(input.getPipeline(), sorted, deadLetter, deadLetterSchema);
  }

  /** Applies the commit-window assignment for the input's boundedness; see the class Javadoc. */
  private PCollection<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> applyCommitWindow(
      PCollection<KV<KV<String, Integer>, KV<byte[], CdcRecord>>> input) {
    if (input.isBounded() == IsBounded.BOUNDED) {
      return input.apply(
          "GlobalWindows",
          Window.<KV<KV<String, Integer>, KV<byte[], CdcRecord>>>into(new GlobalWindows())
              .triggering(DefaultTrigger.of())
              .discardingFiredPanes());
    }
    return input.apply(
        "EventTimeWindows",
        Window.<KV<KV<String, Integer>, KV<byte[], CdcRecord>>>into(
                FixedWindows.of(
                    checkStateNotNull(
                        triggeringFrequency,
                        "triggeringFrequency is required for unbounded input")))
            .triggering(
                AfterWatermark.pastEndOfWindow().withLateFirings(AfterPane.elementCountAtLeast(1)))
            .withAllowedLateness(allowedLateness)
            .discardingFiredPanes());
  }

  /** Extracts the CDC data schema carried by the input's nested {@link CdcRecordCoder}. */
  private static Schema dataSchemaOf(Coder<?> inputCoder) {
    checkArgument(
        inputCoder instanceof KvCoder,
        "expected a KvCoder input element coder, got %s",
        inputCoder);
    Coder<?> valueCoder = ((KvCoder<?, ?>) inputCoder).getValueCoder();
    checkArgument(
        valueCoder instanceof KvCoder, "expected a KvCoder input value coder, got %s", valueCoder);
    Coder<?> recordCoder = ((KvCoder<?, ?>) valueCoder).getValueCoder();
    checkArgument(
        recordCoder instanceof CdcRecordCoder,
        "expected a CdcRecordCoder input record coder, got %s",
        recordCoder);
    return ((CdcRecordCoder) recordCoder).getDataSchema();
  }

  /**
   * The output of {@link CommitWindows}: the surviving sorted groups ready for the delta writer,
   * and the replayable dead-letter {@link Row}s from late panes.
   */
  public static final class Result implements POutput {

    private final Pipeline pipeline;
    private final PCollection<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>>
        sortedGroups;
    private final PCollection<Row> deadLetterRows;
    private final Schema deadLetterSchema;

    private Result(
        Pipeline pipeline,
        PCollection<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> sortedGroups,
        PCollection<Row> deadLetterRows,
        Schema deadLetterSchema) {
      this.pipeline = pipeline;
      this.sortedGroups = sortedGroups;
      this.deadLetterRows = deadLetterRows;
      this.deadLetterSchema = deadLetterSchema;
    }

    /** The surviving sorted groups: one per {@code (destination, shard, window)}. */
    public PCollection<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> getSortedGroups() {
      return sortedGroups;
    }

    /** Replayable dead-letter rows from late panes; {@link SplitLateData} describes the shape. */
    public PCollection<Row> getDeadLetterRows() {
      return deadLetterRows;
    }

    /** The schema of {@link #getDeadLetterRows()}. */
    public Schema getDeadLetterSchema() {
      return deadLetterSchema;
    }

    @Override
    public Pipeline getPipeline() {
      return pipeline;
    }

    @Override
    public Map<TupleTag<?>, PValue> expand() {
      return ImmutableMap.<TupleTag<?>, PValue>builder()
          .put(ON_TIME_TAG, sortedGroups)
          .put(DEAD_LETTER_TAG, deadLetterRows)
          .build();
    }

    @Override
    public void finishSpecifyingOutput(
        String transformName, PInput input, PTransform<?, ?> transform) {
      // no-op
    }
  }
}
