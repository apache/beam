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

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

/**
 * Splits late data in {@link CommitWindows}: every {@linkplain PaneInfo.Timing#LATE late} pane is
 * diverted to a DLQ side output; on-time and early panes pass through unchanged.
 *
 * <p>Pane history is per {@code (destination, shard, window)} while the committer's skip is per
 * {@code (destination, window)}: a late pane on a shard with no on-time data cannot prove its
 * destination-window uncommitted, and a record let through into a committed window would reach
 * neither the table nor the dead-letter output.
 *
 * <p>Each dead letter nests the untouched data row under {@value #DL_RECORD} beside {@value
 * #DL_CHANGE_TYPE}, {@value #DL_SEQ}, and {@value #DL_DEST}; to replay, unnest {@value #DL_RECORD}
 * and map {@value #DL_CHANGE_TYPE}/{@value #DL_SEQ} as the sink's control columns. Replaying is
 * only safe while no newer change for those keys has committed; a stale replay's equality delete
 * removes the newer row.
 */
final class SplitLateData
    extends DoFn<
        KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>,
        KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> {

  static final String DL_RECORD = "record";
  static final String DL_CHANGE_TYPE = "change_type";
  static final String DL_SEQ = "sequence_number";
  static final String DL_DEST = "destination";

  private final Counter deadLetterRecords =
      Metrics.counter(SplitLateData.class, "deadLetterRecords");

  private final Schema deadLetterSchema;
  private final TupleTag<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> onTimeTag;
  private final TupleTag<Row> deadLetterTag;

  SplitLateData(
      Schema deadLetterSchema,
      TupleTag<KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>>> onTimeTag,
      TupleTag<Row> deadLetterTag) {
    this.deadLetterSchema = deadLetterSchema;
    this.onTimeTag = onTimeTag;
    this.deadLetterTag = deadLetterTag;
  }

  static Schema deadLetterSchema(Schema cdcDataSchema) {
    return Schema.builder()
        .addRowField(DL_RECORD, cdcDataSchema)
        .addStringField(DL_CHANGE_TYPE)
        .addInt64Field(DL_SEQ)
        .addStringField(DL_DEST)
        .build();
  }

  @ProcessElement
  public void process(
      @Element KV<KV<String, Integer>, Iterable<KV<byte[], CdcRecord>>> group,
      PaneInfo pane,
      MultiOutputReceiver out) {
    if (pane.getTiming() == PaneInfo.Timing.LATE) {
      String dest = group.getKey().getKey();
      for (KV<byte[], CdcRecord> kv : group.getValue()) {
        CdcRecord record = kv.getValue();
        Row deadLetter =
            Row.withSchema(deadLetterSchema)
                .addValue(record.getData())
                .addValue(record.getKind().name())
                .addValue(record.getSequenceNumber())
                .addValue(dest)
                .build();
        out.get(deadLetterTag).output(deadLetter);
        deadLetterRecords.inc();
      }
    } else {
      out.get(onTimeTag).output(group);
    }
  }
}
