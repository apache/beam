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
      "Timeout for HTTP requests to BigQuery service in milliseconds. Set to 0 to disable.")
  @Default.Integer(900 * 1000)
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
      "The minimum duration in seconds between streaming API statistics logging. "
          + "The interval might be longer than the specified value due to each bundle processing time.")
  @Default.Integer(180)
  Integer getBqStreamingApiLoggingFrequencySec();

  void setBqStreamingApiLoggingFrequencySec(Integer value);

  @Description("If set, then BigQueryIO.Write will default to using the Storage Write API.")
  @Default.Boolean(false)
  Boolean getUseStorageWriteApi();

  void setUseStorageWriteApi(Boolean value);

  @Description(
      "If set, then BigQueryIO.Write will default to using the approximate Storage Write API.")
  @Default.Boolean(false)
  Boolean getUseStorageWriteApiAtLeastOnce();

  void setUseStorageWriteApiAtLeastOnce(Boolean value);

  @Description(
      "If set, then BigQueryIO.Write will default to using this number of Storage Write API streams. ")
  @Default.Integer(0)
  Integer getNumStorageWriteApiStreams();

  void setNumStorageWriteApiStreams(Integer value);

  @Description(
      "When using the {@link org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method#STORAGE_API_AT_LEAST_ONCE} write method, "
          + "this option sets the number of stream append clients that will be allocated at a per worker and destination basis. "
          + "A large value can cause a large pipeline to go over the BigQuery connection quota quickly on a job with "
          + "enough number of workers. On the case of low-mid volume pipelines using the default configuration should be sufficient.")
  @Default.Integer(1)
  Integer getNumStorageWriteApiStreamAppendClients();

  void setNumStorageWriteApiStreamAppendClients(Integer value);

  @Description("The max number of messages inflight that we expect each connection will retain.")
  @Default.Long(1000)
  Long getStorageWriteMaxInflightRequests();

  void setStorageWriteMaxInflightRequests(Long value);

  @Description(
      "The max size in bytes for inflight messages that we expect each connection will retain.")
  @Default.Long(104857600)
  Long getStorageWriteMaxInflightBytes();

  void setStorageWriteMaxInflightBytes(Long value);

  @Default.Boolean(false)
  Boolean getUseStorageApiConnectionPool();

  void setUseStorageApiConnectionPool(Boolean value);

  @Description(
      "If set, then BigQueryIO.Write will default to triggering the Storage Write API writes this often.")
  Integer getStorageWriteApiTriggeringFrequencySec();

  void setStorageWriteApiTriggeringFrequencySec(Integer value);

  @Description(
      "When auto-sharding is used, the maximum duration in milliseconds the input records are"
          + " allowed to be buffered before being written to BigQuery.")
  @Default.Integer(0)
  Integer getMaxBufferingDurationMilliSec();

  void setMaxBufferingDurationMilliSec(Integer value);

  @Description("If specified, it will override the default (GcpOptions#getProject()) project id.")
  String getBigQueryProject();

  void setBigQueryProject(String value);

  @Description("Maximum (best effort) size of a single append to the storage API.")
  @Default.Integer(2 * 1024 * 1024)
  Integer getStorageApiAppendThresholdBytes();

  void setStorageApiAppendThresholdBytes(Integer value);

  @Description("Maximum (best effort) record count of a single append to the storage API.")
  @Default.Integer(150000)
  Integer getStorageApiAppendThresholdRecordCount();

  void setStorageApiAppendThresholdRecordCount(Integer value);

  @Description("Maximum request size allowed by the storage write API. ")
  @Default.Long(10 * 1000 * 1000)
  Long getStorageWriteApiMaxRequestSize();

  void setStorageWriteApiMaxRequestSize(Long value);

  @Description(
      "If set, BigQueryIO.Read will use the StreamBundle based"
          + "implementation of the Read API Source")
  @Default.Boolean(false)
  Boolean getEnableBundling();

  void setEnableBundling(Boolean value);
}
