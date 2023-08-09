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

import com.google.api.services.bigquery.model.TableRow;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.Preconditions;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/** The result of a {@link BigQueryIO.Write} transform. */
public final class WriteResult implements POutput {
  private final Pipeline pipeline;
  private final @Nullable TupleTag<TableRow> failedInsertsTag;
  private final @Nullable PCollection<TableRow> failedInserts;
  private final @Nullable TupleTag<BigQueryInsertError> failedInsertsWithErrTag;
  private final @Nullable PCollection<BigQueryInsertError> failedInsertsWithErr;
  private final @Nullable PCollection<TableRow> successfulInserts;
  private final @Nullable TupleTag<TableDestination> successfulBatchInsertsTag;
  private final @Nullable PCollection<TableDestination> successfulBatchInserts;
  private final @Nullable TupleTag<BigQueryStorageApiInsertError> failedStorageApiInsertsTag;
  private final @Nullable PCollection<BigQueryStorageApiInsertError> failedStorageApiInserts;
  private final @Nullable TupleTag<TableRow> successfulStorageApiInsertsTag;
  private final @Nullable PCollection<TableRow> successfulStorageApiInserts;

  /** Creates a {@link WriteResult} in the given {@link Pipeline}. */
  static WriteResult in(
      Pipeline pipeline,
      @Nullable TupleTag<TableRow> failedInsertsTag,
      @Nullable PCollection<TableRow> failedInserts,
      @Nullable PCollection<TableRow> successfulInserts,
      @Nullable TupleTag<TableDestination> successfulBatchInsertsTag,
      @Nullable PCollection<TableDestination> successfulBatchInserts,
      @Nullable TupleTag<BigQueryStorageApiInsertError> failedStorageApiInsertsTag,
      @Nullable PCollection<BigQueryStorageApiInsertError> failedStorageApiInserts,
      @Nullable TupleTag<TableRow> successfulStorageApiInsertsTag,
      @Nullable PCollection<TableRow> successfulStorageApiInserts) {
    return new WriteResult(
        pipeline,
        failedInsertsTag,
        failedInserts,
        null,
        null,
        successfulInserts,
        successfulBatchInsertsTag,
        successfulBatchInserts,
        failedStorageApiInsertsTag,
        failedStorageApiInserts,
        successfulStorageApiInsertsTag,
        successfulStorageApiInserts);
  }

  static WriteResult withExtendedErrors(
      Pipeline pipeline,
      TupleTag<BigQueryInsertError> failedInsertsTag,
      PCollection<BigQueryInsertError> failedInserts,
      @Nullable PCollection<TableRow> successfulInserts) {
    return new WriteResult(
        pipeline,
        null,
        null,
        failedInsertsTag,
        failedInserts,
        successfulInserts,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    ImmutableMap.Builder<TupleTag<?>, PValue> output = ImmutableMap.builder();

    if (failedInsertsTag != null) {
      output.put(failedInsertsTag, Preconditions.checkArgumentNotNull(failedInserts));
    } else if (failedInsertsWithErrTag != null) {
      output.put(failedInsertsWithErrTag, Preconditions.checkArgumentNotNull(failedInsertsWithErr));
    }
    if (failedStorageApiInsertsTag != null) {
      output.put(
          failedStorageApiInsertsTag, Preconditions.checkArgumentNotNull(failedStorageApiInserts));
    }

    if (successfulBatchInsertsTag != null) {
      output.put(
          successfulBatchInsertsTag, Preconditions.checkArgumentNotNull(successfulBatchInserts));
    }

    return output.build();
  }

  private WriteResult(
      Pipeline pipeline,
      @Nullable TupleTag<TableRow> failedInsertsTag,
      @Nullable PCollection<TableRow> failedInserts,
      @Nullable TupleTag<BigQueryInsertError> failedInsertsWithErrTag,
      @Nullable PCollection<BigQueryInsertError> failedInsertsWithErr,
      @Nullable PCollection<TableRow> successfulInserts,
      @Nullable TupleTag<TableDestination> successfulInsertsTag,
      @Nullable PCollection<TableDestination> successfulBatchInserts,
      @Nullable TupleTag<BigQueryStorageApiInsertError> failedStorageApiInsertsTag,
      @Nullable PCollection<BigQueryStorageApiInsertError> failedStorageApiInserts,
      @Nullable TupleTag<TableRow> successfulStorageApiInsertsTag,
      @Nullable PCollection<TableRow> successfulStorageApiInserts) {
    this.pipeline = pipeline;
    this.failedInsertsTag = failedInsertsTag;
    this.failedInserts = failedInserts;
    this.failedInsertsWithErrTag = failedInsertsWithErrTag;
    this.failedInsertsWithErr = failedInsertsWithErr;
    this.successfulInserts = successfulInserts;
    this.successfulBatchInsertsTag = successfulInsertsTag;
    this.successfulBatchInserts = successfulBatchInserts;
    this.failedStorageApiInsertsTag = failedStorageApiInsertsTag;
    this.failedStorageApiInserts = failedStorageApiInserts;
    this.successfulStorageApiInsertsTag = successfulStorageApiInsertsTag;
    this.successfulStorageApiInserts = successfulStorageApiInserts;
  }

  /**
   * Returns a {@link PCollection} containing the {@link TableDestination}s that were successfully
   * loaded using the batch load API.
   */
  public PCollection<TableDestination> getSuccessfulTableLoads() {
    Preconditions.checkArgumentNotNull(
        successfulBatchInsertsTag,
        "Cannot use getSuccessfulTableLoads because this WriteResult was not "
            + "configured to produce them.  Note: only batch loads produce successfulTableLoads.");
    return Preconditions.checkArgumentNotNull(
        successfulBatchInserts,
        "Cannot use getSuccessfulTableLoads because this WriteResult was not "
            + "configured to produce them.  Note: only batch loads produce successfulTableLoads.");
  }

  /**
   * Returns a {@link PCollection} containing the {@link TableRow}s that were written to BQ via the
   * streaming insert API.
   */
  public PCollection<TableRow> getSuccessfulInserts() {
    if (successfulInserts == null) {
      throw new IllegalStateException(
          "Retrieving successful inserts is only supported for streaming inserts. "
              + "Make sure withSuccessfulInsertsPropagation is correctly configured for "
              + "BigQueryIO.Write object.");
    }
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
    Preconditions.checkArgumentNotNull(
        failedInsertsTag,
        "Cannot use getFailedInserts as this WriteResult uses extended errors"
            + " information. Use getFailedInsertsWithErr or getFailedStorageApiInserts instead");
    return Preconditions.checkStateNotNull(
        failedInserts,
        "Cannot use getFailedInserts as this WriteResult uses extended errors"
            + " information. Use getFailedInsertsWithErr or getFailedStorageApiInserts instead");
  }

  /**
   * Returns a {@link PCollection} containing the {@link BigQueryInsertError}s with detailed error
   * information.
   *
   * <p>Only use this method if you have enabled {@link BigQueryIO.Write#withExtendedErrorInfo()}.
   * Otherwise use {@link WriteResult#getFailedInserts()}
   */
  public PCollection<BigQueryInsertError> getFailedInsertsWithErr() {
    Preconditions.checkArgumentNotNull(
        failedInsertsWithErrTag,
        "Cannot use getFailedInsertsWithErr as this WriteResult does not use"
            + " extended errors. Use getFailedInserts or getFailedStorageApiInserts instead");
    return Preconditions.checkArgumentNotNull(
        failedInsertsWithErr,
        "Cannot use getFailedInsertsWithErr as this WriteResult does not use"
            + " extended errors. Use getFailedInserts or getFailedStorageApiInserts instead");
  }

  /**
   * Return any rows that persistently fail to insert when using a storage-api method. For example:
   * rows with values that do not match the BigQuery schema or rows that are too large to insert.
   * This collection is in the global window.
   */
  public PCollection<BigQueryStorageApiInsertError> getFailedStorageApiInserts() {
    Preconditions.checkStateNotNull(
        failedStorageApiInsertsTag,
        "Cannot use getFailedStorageApiInserts as this insert didn't use the storage API.");
    return Preconditions.checkStateNotNull(
        failedStorageApiInserts,
        "Cannot use getFailedStorageApiInserts as this insert didn't use the storage API.");
  }

  /**
   * Return all rows successfully inserted using one of the storage-api insert methods. Rows undergo
   * a conversion process, so while these TableRow objects are logically the same as the rows in the
   * initial PCollection, they may not be physically identical. This PCollection is in the global
   * window.
   */
  public PCollection<TableRow> getSuccessfulStorageApiInserts() {
    Preconditions.checkStateNotNull(
        successfulStorageApiInsertsTag,
        "Can only getSuccessfulStorageApiInserts if using the storage API and "
            + "withPropagateSuccessfulStorageApiWrites() is set.");
    return Preconditions.checkStateNotNull(
        successfulStorageApiInserts,
        "Can only getSuccessfulStorageApiInserts if using the storage API and "
            + "withPropagateSuccessfulStorageApiWrites() is set.");
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
