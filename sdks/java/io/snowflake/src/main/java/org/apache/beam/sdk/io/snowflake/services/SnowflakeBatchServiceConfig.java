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
package org.apache.beam.sdk.io.snowflake.services;

import java.util.List;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.data.SnowflakeTableSchema;
import org.apache.beam.sdk.io.snowflake.enums.CreateDisposition;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.transforms.SerializableFunction;

/** Class for preparing configuration for batch write and read. */
public class SnowflakeBatchServiceConfig extends ServiceConfig {
  private final SerializableFunction<Void, DataSource> dataSourceProviderFn;

  private final String database;
  private final String schema;
  private final String table;
  private final String query;
  private final String storageIntegrationName;
  private List<String> filesList;
  private WriteDisposition writeDisposition;
  private CreateDisposition createDisposition;
  private SnowflakeTableSchema tableSchema;
  private final String stagingBucketDir;
  private final String quotationMark;

  /** Creating a batch configuration for reading. */
  public SnowflakeBatchServiceConfig(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String database,
      String schema,
      String table,
      String query,
      String storageIntegrationName,
      String stagingBucketDir,
      String quotationMark) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.query = query;
    this.storageIntegrationName = storageIntegrationName;
    this.stagingBucketDir = stagingBucketDir;
    this.quotationMark = quotationMark;
  }

  /** Creating a batch configuration for writing. */
  public SnowflakeBatchServiceConfig(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      List<String> filesList,
      String database,
      String schema,
      String table,
      String query,
      SnowflakeTableSchema tableSchema,
      CreateDisposition createDisposition,
      WriteDisposition writeDisposition,
      String storageIntegrationName,
      String stagingBucketDir,
      String quotationMark) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.filesList = filesList;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.query = query;
    this.writeDisposition = writeDisposition;
    this.createDisposition = createDisposition;
    this.tableSchema = tableSchema;
    this.storageIntegrationName = storageIntegrationName;
    this.stagingBucketDir = stagingBucketDir;
    this.quotationMark = quotationMark;
  }

  /** Getting a DataSource provider function for connection credentials. */
  public SerializableFunction<Void, DataSource> getDataSourceProviderFn() {
    return dataSourceProviderFn;
  }

  /** Getting a table as a source of reading or destination to write. */
  public String getTable() {
    return table;
  }

  /** Getting a query which can be source for reading. */
  public String getQuery() {
    return query;
  }

  /** Getting Snowflake integration which is used in COPY statement. */
  public String getStorageIntegrationName() {
    return storageIntegrationName;
  }

  /** Getting directory where files are staged. */
  public String getStagingBucketDir() {
    return stagingBucketDir;
  }

  /** Getting list of names of staged files. */
  public List<String> getFilesList() {
    return filesList;
  }

  /** Getting disposition how write data to table, see: {@link WriteDisposition}. */
  public WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }

  /** Getting a character that will surround {@code String} in staged CSV files. */
  public String getQuotationMark() {
    return quotationMark;
  }

  /** Getting a Snowflake database. */
  public String getDatabase() {
    return database;
  }

  /** Getting a schema of a Snowflake table. */
  public String getSchema() {
    return schema;
  }

  public CreateDisposition getCreateDisposition() {
    return createDisposition;
  }

  public SnowflakeTableSchema getTableSchema() {
    return tableSchema;
  }
}
