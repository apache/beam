/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.util.Structs.getString;

import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.BigQueryOptions;
import com.google.cloud.dataflow.sdk.options.GcpOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import javax.annotation.Nullable;

/**
 * Creates a BigQueryReader from a {@link CloudObject} spec.
 */
public class BigQueryReaderFactory implements ReaderFactory {

  @Override
  public Reader<?> create(
      CloudObject spec,
      @Nullable Coder<?> coder,
      @Nullable PipelineOptions options,
      @Nullable ExecutionContext executionContext,
      @Nullable CounterSet.AddCounterMutator addCounterMutator,
      @Nullable String operationName)
          throws Exception {
    return createTyped(spec, coder, options, executionContext);
  }

  public BigQueryReader createTyped(
      CloudObject spec,
      Coder<?> coder,
      PipelineOptions options,
      ExecutionContext executionContext) throws Exception {
    String query = getString(spec, PropertyNames.BIGQUERY_QUERY, null);
    if (query != null) {
      GcpOptions gcpOptions = options.as(GcpOptions.class);
      return BigQueryReader.fromQueryWithOptions(
          query, gcpOptions.getProject(), options.as(BigQueryOptions.class));
    }

    String tableId = getString(spec, PropertyNames.BIGQUERY_TABLE, null);
    if (tableId != null) {
      return BigQueryReader.fromTableWithOptions(
          new TableReference()
              .setProjectId(getString(spec, PropertyNames.BIGQUERY_PROJECT))
              .setDatasetId(getString(spec, PropertyNames.BIGQUERY_DATASET))
              .setTableId(tableId),
          options.as(BigQueryOptions.class));
    }

    throw new IllegalArgumentException("Either a table or a query has to be specified");
  }
}
