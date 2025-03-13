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
package org.apache.beam.sdk.tpcds;

import java.util.Map;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Options used to configure TPC-DS test. */
public interface TpcdsOptions extends ApplicationNameOptions, GcpOptions, BeamSqlPipelineOptions {
  @Description(
      "The size of TPC-DS data to run query on, user input should contain the unit, such as '1G', '10G'")
  @Validation.Required
  String getDataSize();

  void setDataSize(String dataSize);

  // Set the return type to be String since reading from the command line (user input will be like
  // "1,2,55" which represent TPC-DS query1, query3, query55)
  @Description("The queries numbers, read user input as string, numbers separated by commas")
  String getQueries();

  void setQueries(String queries);

  @Description("The number of queries to run in parallel")
  @Default.Integer(1)
  Integer getTpcParallel();

  void setTpcParallel(Integer parallelism);

  @Description("The path to input data directory")
  @Validation.Required
  String getDataDirectory();

  void setDataDirectory(String path);

  @Description("The path to directory with results")
  @Validation.Required
  String getResultsDirectory();

  void setResultsDirectory(String path);

  @Description("Where the data comes from.")
  @Default.Enum("CSV")
  TpcdsUtils.SourceType getSourceType();

  void setSourceType(TpcdsUtils.SourceType sourceType);

  @Description("How to derive names of resources.")
  @Default.Enum("QUERY_AND_SALT")
  TpcdsUtils.ResourceNameMode getResourceNameMode();

  void setResourceNameMode(TpcdsUtils.ResourceNameMode mode);

  @Description("Shall we export the summary to BigQuery.")
  @Default.Boolean(false)
  Boolean getExportSummaryToBigQuery();

  void setExportSummaryToBigQuery(Boolean exportSummaryToBigQuery);

  @Description("Base name of BigQuery table name if using BigQuery output.")
  @Default.String("nexmark")
  @Nullable
  String getBigQueryTable();

  void setBigQueryTable(String bigQueryTable);

  @Description("BigQuery dataset")
  @Default.String("nexmark")
  String getBigQueryDataset();

  void setBigQueryDataset(String bigQueryDataset);

  @Description("InfluxDB host.")
  @Nullable
  String getInfluxHost();

  void setInfluxHost(@Nullable String host);

  @Description("InfluxDB database.")
  @Nullable
  String getInfluxDatabase();

  void setInfluxDatabase(@Nullable String database);

  @Description("Shall we export the summary to InfluxDB.")
  @Default.Boolean(false)
  boolean getExportSummaryToInfluxDB();

  void setExportSummaryToInfluxDB(boolean exportSummaryToInfluxDB);

  @Description("Base name of measurement name if using InfluxDB output.")
  @Default.String("tpcds")
  @Nullable
  String getBaseInfluxMeasurement();

  void setBaseInfluxMeasurement(String influxDBMeasurement);

  @Description("Name of retention policy for Influx data.")
  @Nullable
  String getInfluxRetentionPolicy();

  void setInfluxRetentionPolicy(String influxRetentionPolicy);

  @Description("Additional tags for Influx data")
  @Nullable
  Map<String, String> getInfluxTags();

  void setInfluxTags(Map<String, String> influxTags);
}
