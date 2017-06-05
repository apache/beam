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
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;

/**
 * The result of a {@link BigQueryIO.Write} transform.
 */
public final class WriteResult implements POutput {
  private final Pipeline pipeline;
  private final TupleTag<TableRow> failedInsertsTag;
  private final PCollection<TableRow> failedInserts;

  /** Creates a {@link WriteResult} in the given {@link Pipeline}. */
  static WriteResult in(
      Pipeline pipeline, TupleTag<TableRow> failedInsertsTag, PCollection<TableRow> failedInserts) {
    return new WriteResult(pipeline, failedInsertsTag, failedInserts);
  }

  @Override
  public Map<TupleTag<?>, PValue> expand() {
    return ImmutableMap.<TupleTag<?>, PValue>of(failedInsertsTag, failedInserts);
  }

  private WriteResult(
      Pipeline pipeline, TupleTag<TableRow> failedInsertsTag, PCollection<TableRow> failedInserts) {
    this.pipeline = pipeline;
    this.failedInsertsTag = failedInsertsTag;
    this.failedInserts = failedInserts;
  }

  public PCollection<TableRow> getFailedInserts() {
    return failedInserts;
  }

  @Override
  public Pipeline getPipeline() {
    return pipeline;
  }

  @Override
  public void finishSpecifyingOutput(
      String transformName, PInput input, PTransform<?, ?> transform) {}
}
