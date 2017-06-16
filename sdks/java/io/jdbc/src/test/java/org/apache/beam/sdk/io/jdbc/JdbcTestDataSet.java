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
package org.apache.beam.sdk.io.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.common.IOTestPipelineOptions;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manipulates test data used by the {@link org.apache.beam.sdk.io.jdbc.JdbcIO} tests.
 *
 * <p>This is independent from the tests so that for read tests it can be run separately after data
 * store creation rather than every time (which can be more fragile.)
 */
public class JdbcTestDataSet {
  private static final Logger LOG = LoggerFactory.getLogger(JdbcTestDataSet.class);
  public static final String[] SCIENTISTS = {"Einstein", "Darwin", "Copernicus", "Pasteur", "Curie",
      "Faraday", "McClintock", "Herschel", "Hopper", "Lovelace"};
  public static final long EXPECTED_ROW_COUNT = 1000L;
  public static final String EXPECTED_HASH_CODE = "7d94d63a41164be058a9680002914358";

  public static PGSimpleDataSource getDataSource(IOTestPipelineOptions options)
      throws SQLException {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();

    // Tests must receive parameters for connections from PipelineOptions
    // Parameters should be generic to all tests that use a particular datasource, not
    // the particular test.
    dataSource.setDatabaseName(options.getPostgresDatabaseName());
    dataSource.setServerName(options.getPostgresServerName());
    dataSource.setPortNumber(options.getPostgresPort());
    dataSource.setUser(options.getPostgresUsername());
    dataSource.setPassword(options.getPostgresPassword());
    dataSource.setSsl(options.getPostgresSsl());

    return dataSource;
  }

  public static final String READ_TABLE_NAME = "BEAM_TEST_READ";

  public static void createReadDataTableAndAddInitialData(DataSource dataSource)
      throws SQLException {
    createDataTable(dataSource, READ_TABLE_NAME);
    addInitialData(dataSource, READ_TABLE_NAME);
  }

  public static String createWriteDataTableAndAddInitialData(DataSource dataSource)
      throws SQLException {
    String tableName = getWriteTableName();
    createDataTable(dataSource, tableName);
    addInitialData(dataSource, tableName);
    return tableName;
  }

  public static String getWriteTableName() {
    return "BEAMTEST" + org.joda.time.Instant.now().getMillis();
  }

  private static void addInitialData(
      DataSource dataSource, String tableName)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try (PreparedStatement preparedStatement =
               connection.prepareStatement(
                   String.format("insert into %s values (?,?)", tableName))) {
        for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
          int index = i % SCIENTISTS.length;
          preparedStatement.clearParameters();
          preparedStatement.setInt(1, i);
          preparedStatement.setString(2, SCIENTISTS[index]);
          preparedStatement.executeUpdate();
        }
      }
      connection.commit();
    }
  }

  public static void createDataTable(
      DataSource dataSource, String tableName)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format("create table %s (id INT, name VARCHAR(500))", tableName));
      }
    }

    LOG.info("Created table {}", tableName);
  }

  public static void cleanUpDataTable(DataSource dataSource, String tableName)
      throws SQLException {
    if (tableName != null) {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("drop table %s", tableName));
      }
    }
  }

}
