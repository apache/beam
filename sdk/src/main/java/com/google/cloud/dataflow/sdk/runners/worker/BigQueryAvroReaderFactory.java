/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getLong;
import static com.google.cloud.dataflow.sdk.util.Structs.getString;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.NativeReader;

import java.io.IOException;

import javax.annotation.Nullable;

/**
 * Creates an AvroReader specifically for the output of a BigQuery export job.
 */
public class BigQueryAvroReaderFactory implements ReaderFactory {

  public BigQueryAvroReaderFactory() {}

  @Override
  public NativeReader<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable ExecutionContext executionContext,
      @Nullable CounterSet.AddCounterMutator addCounterMutator,
      @Nullable String operationName)
      throws Exception {
    return createTyped(spec, options);
  }

  private static TableSchema parseTableSchema(String schemaStr) throws IOException {
    checkArgument(!schemaStr.isEmpty(), "Provided BigQuery schema is empty");
    return JacksonFactory.getDefaultInstance().fromString(schemaStr, TableSchema.class);
  }

  NativeReader<?> createTyped(CloudObject spec, PipelineOptions options) throws Exception {
    String filename = getString(spec, PropertyNames.FILENAME);
    Long startOffset = getLong(spec, PropertyNames.START_OFFSET, null);
    Long endOffset = getLong(spec, PropertyNames.END_OFFSET, null);
    TableSchema bqSchema = parseTableSchema(getString(spec, PropertyNames.BIGQUERY_EXPORT_SCHEMA));
    return new BigQueryAvroReader(filename, startOffset, endOffset, bqSchema, options);
  }
}
