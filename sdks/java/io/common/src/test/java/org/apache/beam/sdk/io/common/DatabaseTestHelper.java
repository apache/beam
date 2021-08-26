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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import javax.sql.DataSource;
import org.postgresql.ds.PGSimpleDataSource;

/** This class contains helper methods to ease database usage in tests. */
public class DatabaseTestHelper {

  public static PGSimpleDataSource getPostgresDataSource(PostgresIOTestPipelineOptions options) {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setDatabaseName(options.getPostgresDatabaseName());
    dataSource.setServerName(options.getPostgresServerName());
    dataSource.setPortNumber(options.getPostgresPort());
    dataSource.setUser(options.getPostgresUsername());
    dataSource.setPassword(options.getPostgresPassword());
    dataSource.setSsl(options.getPostgresSsl());
    return dataSource;
  }

  public static void createTable(DataSource dataSource, String tableName) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(String.format("create table %s (id INT, name VARCHAR(500))", tableName));
      }
    }
  }

  public static void createTableForRowWithSchema(DataSource dataSource, String tableName)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(String.format("create table %s (name VARCHAR(500), id INT)", tableName));
      }
    }
  }

  public static void deleteTable(DataSource dataSource, String tableName) throws SQLException {
    if (tableName != null) {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeUpdate(String.format("drop table %s", tableName));
      }
    }
  }

  public static String getTestTableName(String testIdentifier) {
    SimpleDateFormat formatter = new SimpleDateFormat();
    formatter.applyPattern("yyyy_MM_dd_HH_mm_ss_S");
    return String.format("BEAMTEST_%s_%s", testIdentifier, formatter.format(new Date()));
  }

  public static String getPostgresDBUrl(PostgresIOTestPipelineOptions options) {
    return String.format(
        "jdbc:postgresql://%s:%s/%s",
        options.getPostgresServerName(),
        options.getPostgresPort(),
        options.getPostgresDatabaseName());
  }

  public static Optional<Long> getPostgresTableSize(DataSource dataSource, String tableName) {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        ResultSet resultSet =
            statement.executeQuery(String.format("select pg_relation_size('%s')", tableName));
        if (resultSet.next()) {
          return Optional.of(resultSet.getLong(1));
        }
      }
    } catch (SQLException e) {
      return Optional.empty();
    }
    return Optional.empty();
  }

  public static void createTableWithStatement(DataSource dataSource, String stmt)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(stmt);
      }
    }
  }
}
