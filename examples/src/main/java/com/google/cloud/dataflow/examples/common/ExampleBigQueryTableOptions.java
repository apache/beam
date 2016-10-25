/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.examples.common;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

/**
 * Options that can be used to configure BigQuery tables in Dataflow examples.
 * The project defaults to the project being used to run the example.
 */
public interface ExampleBigQueryTableOptions extends DataflowPipelineOptions {
  @Description("BigQuery dataset name")
  @Default.String("dataflow_examples")
  String getBigQueryDataset();
  void setBigQueryDataset(String dataset);

  @Description("BigQuery table name")
  @Default.InstanceFactory(BigQueryTableFactory.class)
  String getBigQueryTable();
  void setBigQueryTable(String table);

  @Description("BigQuery table schema")
  TableSchema getBigQuerySchema();
  void setBigQuerySchema(TableSchema schema);

  /**
   * Returns the job name as the default BigQuery table name.
   */
  class BigQueryTableFactory implements DefaultValueFactory<String> {
    @Override
    public String create(PipelineOptions options) {
      return options.as(DataflowPipelineOptions.class).getJobName()
          .replace('-', '_');
    }
  }
}
