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

public class SnowflakeBatchServiceConfig extends ServiceConfig {
  private SerializableFunction<Void, DataSource> dataSourceProviderFn;

  private String database;
  private String schema;
  private String table;
  private String query;
  private String storageIntegrationName;
  private List<String> filesList;
  private WriteDisposition writeDisposition;
  private CreateDisposition createDisposition;
  private SnowflakeTableSchema tableSchema;
  private String stagingBucketDir;

  private String quotationMark;

  public SnowflakeBatchServiceConfig(
      SerializableFunction<Void, DataSource> dataSourceProviderFn,
      String database,
      String schema,
      String table,
      String query,
      String storageIntegrationName,
      String stagingBucketDir) {
    this.dataSourceProviderFn = dataSourceProviderFn;
    this.database = database;
    this.schema = schema;
    this.table = table;
    this.query = query;
    this.storageIntegrationName = storageIntegrationName;
    this.stagingBucketDir = stagingBucketDir;
  }

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
      String stagingBucketDir) {
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
  }

  public SerializableFunction<Void, DataSource> getDataSourceProviderFn() {
    return dataSourceProviderFn;
  }

  public String getTable() {
    return table;
  }

  public String getQuery() {
    return query;
  }

  public String getStorageIntegrationName() {
    return storageIntegrationName;
  }

  public String getStagingBucketDir() {
    return stagingBucketDir;
  }

  public List<String> getFilesList() {
    return filesList;
  }

  public WriteDisposition getWriteDisposition() {
    return writeDisposition;
  }

  public String getDatabase() {
    return database;
  }

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
