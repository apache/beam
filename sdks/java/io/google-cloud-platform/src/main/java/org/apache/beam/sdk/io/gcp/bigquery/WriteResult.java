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

/** The result of a {@link BigQueryIO.Write} transform. */
public final class WriteResult implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<TableRow> failedInsertsTag;
  private final PCollection<TableRow> failedInserts;
  private final TupleTag<BigQueryInsertError> failedInsertsWithErrTag;
  private final PCollection<BigQueryInsertError> failedInsertsWithErr;

  /** Creates a {@link WriteResult} in the given {@link Pipeline}. */
  static WriteResult in(
      Pipeline pipeline, TupleTag<TableRow> failedInsertsTag, PCollection<TableRow> failedInserts) {
    return new WriteResult(pipeline, failedInsertsTag, failedInserts, null, null);
  }

  static WriteResult withExtendedErrors(
      Pipeline pipeline,
      TupleTag<BigQueryInsertError> failedInsertsTag,
      PCollection<BigQueryInsertError> failedInserts) {
    return new WriteResult(pipeline, null, null, failedInsertsTag, failedInserts);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    if (failedInsertsTag != null) {
      return ImmutableMap.of(failedInsertsTag, failedInserts);
    } else {
      return ImmutableMap.of(failedInsertsWithErrTag, failedInsertsWithErr);
    }
  }

  private WriteResult(
      Pipeline pipeline,
      TupleTag<TableRow> failedInsertsTag,
      PCollection<TableRow> failedInserts,
      TupleTag<BigQueryInsertError> failedInsertsWithErrTag,
      PCollection<BigQueryInsertError> failedInsertsWithErr) {
    this.pipeline = pipeline;
    this.failedInsertsTag = failedInsertsTag;
    this.failedInserts = failedInserts;
    this.failedInsertsWithErrTag = failedInsertsWithErrTag;
    this.failedInsertsWithErr = failedInsertsWithErr;
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

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
