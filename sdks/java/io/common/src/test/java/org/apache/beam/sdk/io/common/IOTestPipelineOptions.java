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
package org.apache.beam.sdk.io.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Pipeline options common for IO integration tests. */
public interface IOTestPipelineOptions extends TestPipelineOptions {

  /* Used by most IOIT */
  @Description("Number records that will be written and read by the test")
  @Default.Integer(100000)
  Integer getNumberOfRecords();

  void setNumberOfRecords(Integer count);

  @Description("BigQuery dataset to publish results to.")
  @Nullable
  String getBigQueryDataset();

  void setBigQueryDataset(@Nullable String dataset);

  @Description("BigQuery table to publish results to.")
  @Nullable
  String getBigQueryTable();

  void setBigQueryTable(@Nullable String tableName);

  @Description("InfluxDB measurement to publish results to.")
  String getInfluxMeasurement();

  void setInfluxMeasurement(@Nullable String measurement);

  @Description("InfluxDB host.")
  String getInfluxHost();

  void setInfluxHost(@Nullable String host);

  @Description("InfluxDB database.")
  String getInfluxDatabase();

  void setInfluxDatabase(@Nullable String database);
}
