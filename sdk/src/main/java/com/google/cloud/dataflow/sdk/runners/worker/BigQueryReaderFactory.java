/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.PropertyNames;

/**
 * Creates a BigQueryReader from a {@link CloudObject} spec.
 */
public class BigQueryReaderFactory {
  // Do not instantiate.
  private BigQueryReaderFactory() {}

  public static BigQueryReader create(PipelineOptions options, CloudObject spec, Coder<?> coder,
      ExecutionContext executionContext) throws Exception {
    return new BigQueryReader(
        options.as(BigQueryOptions.class),
        new TableReference()
            .setProjectId(getString(spec, PropertyNames.BIGQUERY_PROJECT))
            .setDatasetId(getString(spec, PropertyNames.BIGQUERY_DATASET))
            .setTableId(getString(spec, PropertyNames.BIGQUERY_TABLE)));
  }
}
