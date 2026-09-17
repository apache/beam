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
package org.apache.beam.sdk.io.iceberg;

import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * The output of an {@code IcebergIO} write: the snapshots each destination table committed, plus
 * the two diversion outputs the CDC sink can produce.
 *
 * <p>Only {@link #getSnapshots()} is always present. {@link #getDeadLetterRows()} (late-but-valid
 * records) and {@link #getFailedRows()} (per-record poison rows) are non-null only for results
 * built by {@link #cdc}, that is, by {@code IcebergIO.writeCdcRows}, and {@code getFailedRows()}
 * additionally only when error handling was enabled. The append-only sink ({@code
 * IcebergIO.writeRows}) leaves both null rather than exposing outputs that can never carry data.
 */
public final class IcebergWriteResult implements POutput {

  private static final TupleTag<KV<String, SnapshotInfo>> SNAPSHOTS_TAG =
      new TupleTag<KV<String, SnapshotInfo>>() {};

  private static final TupleTag<Row> DEAD_LETTER_TAG = new TupleTag<Row>() {};

  private static final TupleTag<Row> FAILED_ROWS_TAG = new TupleTag<Row>() {};

  private final Pipeline pipeline;

  private final PCollection<KV<String, SnapshotInfo>> snapshots;

  private final @Nullable PCollection<Row> deadLetterRows;

  private final @Nullable PCollection<Row> failedRows;

  /**
   * The committed snapshots, keyed by destination. A window committed in a commit fire that later
   * fails may not re-emit its {@link SnapshotInfo} on the retry (table state is unaffected).
   */
  public PCollection<KV<String, SnapshotInfo>> getSnapshots() {
    return snapshots;
  }

  /**
   * The replayable dead-letter {@link Row}s from the CDC sink: records whose grouped pane fired
   * late, i.e. after the watermark had passed their commit window's end. Schema is {@code record
   * ROW<dataSchema> + change_type STRING + sequence_number INT64 + destination STRING}. To replay,
   * unnest {@code record}, map {@code change_type}/{@code sequence_number} as the sink's control
   * columns, and route each row by {@code destination}. Replaying is only safe while no newer
   * change for those keys has committed; a stale replay's equality delete removes the newer row.
   *
   * @return the dead-letter {@code PCollection<Row>} for results produced by {@code
   *     IcebergIO.writeCdcRows}/{@link #cdc}, or {@code null} for results produced by the
   *     append-only sink ({@code IcebergIO.writeRows}), which has no dead-letter output.
   */
  public @Nullable PCollection<Row> getDeadLetterRows() {
    return deadLetterRows;
  }

  /**
   * The per-record poison rows diverted by the CDC sink when error handling is enabled (see {@code
   * WriteCdcRows.withErrorHandling}); schema is {@code failed_row ROW + error_message STRING} (see
   * {@link org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling}). Distinct from {@link
   * #getDeadLetterRows()} (which carries late-but-valid records).
   *
   * @return the failed-rows {@code PCollection<Row>}, or {@code null} when error handling was not
   *     enabled (or for the append-only sink).
   */
  public @Nullable PCollection<Row> getFailedRows() {
    return failedRows;
  }

  IcebergWriteResult(Pipeline pipeline, PCollection<KV<String, SnapshotInfo>> snapshots) {
    this(pipeline, snapshots, null, null);
  }

  private IcebergWriteResult(
      Pipeline pipeline,
      PCollection<KV<String, SnapshotInfo>> snapshots,
      @Nullable PCollection<Row> deadLetterRows,
      @Nullable PCollection<Row> failedRows) {
    this.pipeline = pipeline;
    this.snapshots = snapshots;
    this.deadLetterRows = deadLetterRows;
    this.failedRows = failedRows;
  }

  /**
   * Returns an {@link IcebergWriteResult} for the CDC sink, exposing the committed-snapshot {@code
   * snapshots}, the replayable {@code deadLetterRows} (see {@link #getDeadLetterRows()}), and the
   * optional per-record {@code failedRows} (see {@link #getFailedRows()}, {@code null} when error
   * handling is off).
   */
  @Internal
  public static IcebergWriteResult cdc(
      Pipeline pipeline,
      PCollection<KV<String, SnapshotInfo>> snapshots,
      PCollection<Row> deadLetterRows,
      @Nullable PCollection<Row> failedRows) {
    return new IcebergWriteResult(pipeline, snapshots, deadLetterRows, failedRows);
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> output = ImmutableMap.builder();
    output.put(SNAPSHOTS_TAG, snapshots);
    if (deadLetterRows != null) {
      output.put(DEAD_LETTER_TAG, deadLetterRows);
    }
    if (failedRows != null) {
      output.put(FAILED_ROWS_TAG, failedRows);
    }
    return output.build();
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
