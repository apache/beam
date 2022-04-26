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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The result of a {@link BigQueryIO.Write} transform. */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public final class WriteResult implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<TableRow> failedInsertsTag;
  private final PCollection<TableRow> failedInserts;
  private final TupleTag<BigQueryInsertError> failedInsertsWithErrTag;
  private final PCollection<BigQueryInsertError> failedInsertsWithErr;
  private final PCollection<TableRow> successfulInserts;
  private final TupleTag<TableDestination> successfulBatchInsertsTag;
  private final PCollection<TableDestination> successfulBatchInserts;

  /** Creates a {@link WriteResult} in the given {@link Pipeline}. */
  static WriteResult in(
      Pipeline pipeline,
      TupleTag<TableRow> failedInsertsTag,
      PCollection<TableRow> failedInserts,
      @Nullable PCollection<TableRow> successfulInserts,
      @Nullable TupleTag<TableDestination> successfulBatchInsertsTag,
      @Nullable PCollection<TableDestination> successfulBatchInserts) {
    return new WriteResult(
        pipeline,
        failedInsertsTag,
        failedInserts,
        null,
        null,
        successfulInserts,
        successfulBatchInsertsTag,
        successfulBatchInserts);
  }

  static WriteResult withExtendedErrors(
      Pipeline pipeline,
      TupleTag<BigQueryInsertError> failedInsertsTag,
      PCollection<BigQueryInsertError> failedInserts,
      PCollection<TableRow> successfulInserts) {
    return new WriteResult(
        pipeline, null, null, failedInsertsTag, failedInserts, successfulInserts, null, null);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> output = ImmutableMap.builder();

    if (failedInsertsTag != null) {
      output.put(failedInsertsTag, failedInserts);
    } else {
      output.put(failedInsertsWithErrTag, failedInsertsWithErr);
    }

    if (successfulBatchInserts != null) {
      output.put(successfulBatchInsertsTag, successfulBatchInserts);
    }

    return output.build();
  }

  private WriteResult(
      Pipeline pipeline,
      TupleTag<TableRow> failedInsertsTag,
      PCollection<TableRow> failedInserts,
      TupleTag<BigQueryInsertError> failedInsertsWithErrTag,
      PCollection<BigQueryInsertError> failedInsertsWithErr,
      PCollection<TableRow> successfulInserts,
      TupleTag<TableDestination> successfulInsertsTag,
      PCollection<TableDestination> successfulBatchInserts) {
    this.pipeline = pipeline;
    this.failedInsertsTag = failedInsertsTag;
    this.failedInserts = failedInserts;
    this.failedInsertsWithErrTag = failedInsertsWithErrTag;
    this.failedInsertsWithErr = failedInsertsWithErr;
    this.successfulInserts = successfulInserts;
    this.successfulBatchInsertsTag = successfulInsertsTag;
    this.successfulBatchInserts = successfulBatchInserts;
  }

  /**
   * Returns a {@link PCollection} containing the {@link TableDestination}s that were successfully
   * loaded using the batch load API.
   */
  public PCollection<TableDestination> getSuccessfulTableLoads() {
    checkArgument(
        successfulBatchInsertsTag != null,
        "Cannot use getSuccessfulTableLoads because this WriteResult was not "
            + "configured to produce them.  Note: only batch loads produce successfulTableLoads.");

    return successfulBatchInserts;
  }

  /**
   * Returns a {@link PCollection} containing the {@link TableRow}s that were written to BQ via the
   * streaming insert API.
   */
  public PCollection<TableRow> getSuccessfulInserts() {
    checkArgument(
        successfulInserts != null,
        "Retrieving successful inserts is only supported for streaming inserts.");
    return successfulInserts;
  }

  /**
   * Returns a {@link PCollection} containing the {@link TableRow}s that didn't make it to BQ.
   *
   * <p>Only use this method if you haven't enabled {@link
   * BigQueryIO.Write#withExtendedErrorInfo()}. Otherwise use {@link
   * WriteResult#getFailedInsertsWithErr()}
   */
  public PCollection<TableRow> getFailedInserts() {
    checkArgument(
        failedInsertsTag != null,
        "Cannot use getFailedInserts as this WriteResult uses extended errors"
            + " information. Use getFailedInsertsWithErr instead");
    return failedInserts;
  }

  /**
   * Returns a {@link PCollection} containing the {@link BigQueryInsertError}s with detailed error
   * information.
   *
   * <p>Only use this method if you have enabled {@link BigQueryIO.Write#withExtendedErrorInfo()}.
   * Otherwise use {@link WriteResult#getFailedInserts()}
   */
  public PCollection<BigQueryInsertError> getFailedInsertsWithErr() {
    checkArgument(
        failedInsertsWithErrTag != null,
        "Cannot use getFailedInsertsWithErr as this WriteResult does not use"
            + " extended errors. Use getFailedInserts instead");
    return failedInsertsWithErr;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
