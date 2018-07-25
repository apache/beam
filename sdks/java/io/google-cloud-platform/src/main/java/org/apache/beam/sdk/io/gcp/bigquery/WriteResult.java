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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/** The result of a {@link BigQueryIO.Write} transform. */
public final class WriteResult implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<TableRow> failedInsertsTag;
  private final PCollection<TableRow> failedInserts;
  private final TupleTag<BigQueryInsertError> failedInsertsWithErrTag;
  private final PCollection<BigQueryInsertError> failedInsertsWithErr;
  private final TupleTag<KV<TableDestination, BigQueryWriteResult>> loadResultsTag;
  private final PCollection<KV<TableDestination, BigQueryWriteResult>> loadResults;

  /** Creates a {@link WriteResult} in the given {@link Pipeline}. */
  static WriteResult in(
      Pipeline pipeline, TupleTag<TableRow> failedInsertsTag, PCollection<TableRow> failedInserts) {
    return new WriteResult(pipeline, failedInsertsTag, failedInserts, null, null, null, null);
  }

  static WriteResult withExtendedErrors(
      Pipeline pipeline,
      TupleTag<BigQueryInsertError> failedInsertsTag,
      PCollection<BigQueryInsertError> failedInserts) {
    return new WriteResult(pipeline, null, null, failedInsertsTag, failedInserts, null, null);
  }

  static WriteResult withLoadResults(
      Pipeline pipeline,
      TupleTag<KV<TableDestination, BigQueryWriteResult>> loadResultsTag,
      PCollection<KV<TableDestination, BigQueryWriteResult>> loadResults) {
    return new WriteResult(pipeline, null, null, null, null, loadResultsTag, loadResults);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    if (failedInsertsTag != null) {
      return ImmutableMap.of(failedInsertsTag, failedInserts);
    } else if (failedInsertsWithErrTag != null) {
      return ImmutableMap.of(failedInsertsWithErrTag, failedInsertsWithErr);
    } else {
      return ImmutableMap.of(loadResultsTag, loadResults);
    }
  }

  private WriteResult(
      Pipeline pipeline,
      TupleTag<TableRow> failedInsertsTag,
      PCollection<TableRow> failedInserts,
      TupleTag<BigQueryInsertError> failedInsertsWithErrTag,
      PCollection<BigQueryInsertError> failedInsertsWithErr,
      TupleTag<KV<TableDestination, BigQueryWriteResult>> loadResultsTag,
      PCollection<KV<TableDestination, BigQueryWriteResult>> loadResults) {
    this.pipeline = pipeline;
    this.failedInsertsTag = failedInsertsTag;
    this.failedInserts = failedInserts;
    this.failedInsertsWithErrTag = failedInsertsWithErrTag;
    this.failedInsertsWithErr = failedInsertsWithErr;
    this.loadResultsTag = loadResultsTag;
    this.loadResults = loadResults;
  }

  /**
   * Returns a {@link PCollection} containing the {@link TableRow}s that didn't made it to BQ.
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
   * <p>Only use this method if you have enabled {@link BigQueryIO.Write#withExtendedErrorInfo()}. *
   * Otherwise use {@link WriteResult#getFailedInserts()}
   */
  public PCollection<BigQueryInsertError> getFailedInsertsWithErr() {
    checkArgument(
        failedInsertsWithErrTag != null,
        "Cannot use getFailedInsertsWithErr as this WriteResult does not use"
            + " extended errors. Use getFailedInserts instead");
    return failedInsertsWithErr;
  }

  /**
   * Returns a {@link PCollection} containing pairs of {@link TableDestination} -> {@link
   * BigQueryWriteResult} indicating the result of the load job per table destination.
   */
  public PCollection<KV<TableDestination, BigQueryWriteResult>> getLoadJobResults() {
    checkArgument(
        loadResultsTag != null,
        "Cannot use getLoadJobResults as this WriteResult was not configured to represent a load jobs result");
    return loadResults;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
