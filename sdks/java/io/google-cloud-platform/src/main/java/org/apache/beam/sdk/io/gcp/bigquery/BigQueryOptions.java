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

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

/** Properties needed when using Google BigQuery with the Apache Beam SDK. */
@Description(
    "Options that are used to configure Google BigQuery. See "
        + "https://cloud.google.com/bigquery/what-is-bigquery for details on BigQuery.")
public interface BigQueryOptions
    extends ApplicationNameOptions, GcpOptions, PipelineOptions, StreamingOptions {
  @Description(
      "Temporary dataset for BigQuery table operations. "
          + "Supported values are \"bigquery.googleapis.com/{dataset}\"")
  @Default.String("bigquery.googleapis.com/cloud_dataflow")
  String getTempDatasetId();

  void setTempDatasetId(String value);

  @Description(
      "If specified, the given write timeout will be set to HTTP requests created to "
          + "communicate with BigQuery service.")
  @Default.Integer(0)
  Integer getHTTPWriteTimeout();

  void setHTTPWriteTimeout(Integer timeout);

  @Description(
      "If specified, the given number of maximum concurrent threads will be used to insert "
          + "rows from one bundle to BigQuery service with streaming insert API.")
  @Default.Integer(3)
  Integer getInsertBundleParallelism();

  void setInsertBundleParallelism(Integer parallelism);

  @Description("The number of keys used per table when doing streaming inserts to BigQuery.")
  @Default.Integer(50)
  Integer getNumStreamingKeys();

  void setNumStreamingKeys(Integer value);

  @Description("The maximum number of rows to batch in a single streaming insert to BigQuery.")
  @Default.Long(500)
  Long getMaxStreamingRowsToBatch();

  void setMaxStreamingRowsToBatch(Long value);

  @Description("The maximum byte size of a single streaming insert to BigQuery.")
  @Default.Long(64L * 1024L)
  Long getMaxStreamingBatchSize();

  void setMaxStreamingBatchSize(Long value);

  @Description(
      "The minimum duration in milliseconds between percentile latencies logging. The interval "
          + "might be longer than the specified value due to each bundle processing time.")
  @Default.Long(30000)
  Long getLatencyLoggingFrequency();

  void setLatencyLoggingFrequency(Long value);
}
