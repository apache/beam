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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.snowflake.enums.CloudProvider;
import org.apache.beam.sdk.io.snowflake.enums.WriteDisposition;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implemenation of {@link SnowflakeService} used in production. */
public class SnowflakeServiceImpl implements SnowflakeService<SnowflakeServiceConfig> {
  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeServiceImpl.class);
  private static final String SNOWFLAKE_GCS_PREFIX = "gcs://";

  @Override
  public void write(SnowflakeServiceConfig config) throws Exception {
    copyToTable(config);
  }

  @Override
  public String read(SnowflakeServiceConfig config) throws Exception {
    return copyIntoStage(config);
  }

  public String copyIntoStage(SnowflakeServiceConfig config) throws SQLException {
    SerializableFunction<Void, DataSource> dataSourceProviderFn = config.getDataSourceProviderFn();
    String table = config.getTable();
    String query = config.getQuery();
    String storageIntegrationName = config.getstorageIntegrationName();
    String stagingBucketDir = config.getStagingBucketDir();

    String source;
    if (query != null) {
      // Query must be surrounded with brackets
      source = String.format("(%s)", query);
    } else {
      source = table;
    }

    String copyQuery =
        String.format(
            "COPY INTO '%s' FROM %s STORAGE_INTEGRATION=%s FILE_FORMAT=(TYPE=CSV COMPRESSION=GZIP FIELD_OPTIONALLY_ENCLOSED_BY='%s');",
            getProperBucketDir(stagingBucketDir),
            source,
            storageIntegrationName,
            CSV_QUOTE_CHAR_FOR_COPY);

    runStatement(copyQuery, getConnection(dataSourceProviderFn), null);

    return stagingBucketDir.concat("*");
  }

  public void copyToTable(SnowflakeServiceConfig config) throws SQLException {

    SerializableFunction<Void, DataSource> dataSourceProviderFn = config.getDataSourceProviderFn();
    List<String> filesList = config.getFilesList();
    String table = config.getTable();
    String query = config.getQuery();
    WriteDisposition writeDisposition = config.getWriteDisposition();
    String storageIntegrationName = config.getstorageIntegrationName();
    String stagingBucketDir = config.getStagingBucketDir();

    String source;
    if (query != null) {
      // Query must be surrounded with brackets
      source = String.format("(%s)", query);
    } else {
      source = String.format("'%s'", stagingBucketDir);
    }

    filesList = filesList.stream().map(e -> String.format("'%s'", e)).collect(Collectors.toList());
    String files = String.join(", ", filesList);
    files = files.replaceAll(stagingBucketDir, "");
    DataSource dataSource = dataSourceProviderFn.apply(null);

    prepareTableAccordingWriteDisposition(dataSource, table, writeDisposition);

    if (!storageIntegrationName.isEmpty()) {
      query =
          String.format(
              "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s' COMPRESSION=GZIP) STORAGE_INTEGRATION=%s;",
              table,
              getProperBucketDir(source),
              files,
              CSV_QUOTE_CHAR_FOR_COPY,
              storageIntegrationName);
    } else {
      query =
          String.format(
              "COPY INTO %s FROM %s FILES=(%s) FILE_FORMAT=(TYPE=CSV FIELD_OPTIONALLY_ENCLOSED_BY='%s' COMPRESSION=GZIP);",
              table, source, files, CSV_QUOTE_CHAR_FOR_COPY);
    }

    runStatement(query, dataSource.getConnection(), null);
  }

  private void truncateTable(DataSource dataSource, String table) throws SQLException {
    String query = String.format("TRUNCATE %s;", table);
    runConnectionWithStatement(dataSource, query, null);
  }

  private static void checkIfTableIsEmpty(DataSource dataSource, String table) throws SQLException {
    String selectQuery = String.format("SELECT count(*) FROM %s LIMIT 1;", table);
    runConnectionWithStatement(
        dataSource,
        selectQuery,
        resultSet -> {
          assert resultSet != null;
          checkIfTableIsEmpty((ResultSet) resultSet);
        });
  }

  private static void checkIfTableIsEmpty(ResultSet resultSet) {
    int columnId = 1;
    try {
      if (!resultSet.next() || !checkIfTableIsEmpty(resultSet, columnId)) {
        throw new RuntimeException("Table is not empty. Aborting COPY with disposition EMPTY");
      }
    } catch (SQLException e) {
      throw new RuntimeException("Unable run pipeline with EMPTY disposition.", e);
    }
  }

  private static boolean checkIfTableIsEmpty(ResultSet resultSet, int columnId)
      throws SQLException {
    int rowCount = resultSet.getInt(columnId);
    if (rowCount >= 1) {
      return false;
    }
    return true;
  }

  private void prepareTableAccordingWriteDisposition(
      DataSource dataSource, String table, WriteDisposition writeDisposition) throws SQLException {
    switch (writeDisposition) {
      case TRUNCATE:
        truncateTable(dataSource, table);
        break;
      case EMPTY:
        checkIfTableIsEmpty(dataSource, table);
        break;
      case APPEND:
      default:
        break;
    }
  }

  private static void runConnectionWithStatement(
      DataSource dataSource, String query, Consumer resultSetMethod) throws SQLException {
    Connection connection = dataSource.getConnection();
    runStatement(query, connection, resultSetMethod);
    connection.close();
  }

  private static void runStatement(String query, Connection connection, Consumer resultSetMethod)
      throws SQLException {
    PreparedStatement statement = connection.prepareStatement(query);
    try {
      if (resultSetMethod != null) {
        ResultSet resultSet = statement.executeQuery();
        resultSetMethod.accept(resultSet);
      } else {
        statement.execute();
      }
    } finally {
      statement.close();
      connection.close();
    }
  }

  private Connection getConnection(SerializableFunction<Void, DataSource> dataSourceProviderFn)
      throws SQLException {
    DataSource dataSource = dataSourceProviderFn.apply(null);
    return dataSource.getConnection();
  }

  // Snowflake is expecting "gcs://" prefix for GCS and Beam "gs://"
  private String getProperBucketDir(String bucketDir) {
    if (bucketDir.contains(CloudProvider.GCS.getPrefix())) {
      return bucketDir.replace(CloudProvider.GCS.getPrefix(), SNOWFLAKE_GCS_PREFIX);
    }
    return bucketDir;
  }
}
