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

package com.google.cloud.dataflow.sdk.options;

/**
 * Properties needed when using BigQuery with the Dataflow SDK.
 */
@Description("Options that are used to configure BigQuery. See "
    + "https://cloud.google.com/bigquery/what-is-bigquery for details on BigQuery.")
public interface BigQueryOptions extends ApplicationNameOptions, GcpOptions,
    PipelineOptions, StreamingOptions {
  @Description("Temporary dataset for BigQuery table operations. "
      + "Supported values are \"bigquery.googleapis.com/{dataset}\"")
  @Default.String("bigquery.googleapis.com/cloud_dataflow")
  String getTempDatasetId();
  void setTempDatasetId(String value);
}
