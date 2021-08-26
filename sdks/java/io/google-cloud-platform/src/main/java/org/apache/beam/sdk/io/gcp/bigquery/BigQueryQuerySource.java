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

import com.google.api.services.bigquery.model.TableReference;
import java.io.IOException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A {@link BigQuerySourceBase} for querying BigQuery tables. */
@VisibleForTesting
class BigQueryQuerySource<T> extends BigQuerySourceBase<T> {

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryQuerySource.class);

  static <T> BigQueryQuerySource<T> create(
      String stepUuid,
      BigQueryQuerySourceDef queryDef,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      boolean useAvroLogicalTypes) {
    return new BigQueryQuerySource<>(
        stepUuid, queryDef, bqServices, coder, parseFn, useAvroLogicalTypes);
  }

  private final BigQueryQuerySourceDef queryDef;

  private BigQueryQuerySource(
      String stepUuid,
      BigQueryQuerySourceDef queryDef,
      BigQueryServices bqServices,
      Coder<T> coder,
      SerializableFunction<SchemaAndRecord, T> parseFn,
      boolean useAvroLogicalTypes) {
    super(stepUuid, bqServices, coder, parseFn, useAvroLogicalTypes);
    this.queryDef = queryDef;
  }

  @Override
  public long getEstimatedSizeBytes(PipelineOptions options) throws Exception {
    return queryDef.getEstimatedSizeBytes(options.as(BigQueryOptions.class));
  }

  @Override
  protected TableReference getTableToExtract(BigQueryOptions bqOptions)
      throws IOException, InterruptedException {
    return queryDef.getTableReference(bqOptions, stepUuid);
  }

  @Override
  protected void cleanupTempResource(BigQueryOptions bqOptions) throws Exception {
    queryDef.cleanupTempResource(bqOptions, stepUuid);
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    builder.add(DisplayData.item("query", queryDef.getQuery()));
    builder.add(
        DisplayData.item("launchesBigQueryJobs", true)
            .withLabel("This transform launches BigQuery jobs to read/write elements."));
  }
}
