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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.JdbcDatabaseContainer;

/** This class contains helper methods to ease database usage in tests. */
@SuppressFBWarnings(
    value = {"OBL_UNSATISFIED_OBLIGATION", "OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE"},
    justification = "https://github.com/spotbugs/spotbugs/issues/493")
public class DatabaseTestHelper {
  private static Map<String, DataSource> hikariSources = new HashMap<>();

  public static DataSource getDataSourceForContainer(JdbcDatabaseContainer<?> container) {
    String jdbcUrl = checkNotNull(container.getJdbcUrl());
    DataSource existingSource = hikariSources.get(jdbcUrl);
    if (existingSource != null) {
      return existingSource;
    }
    HikariConfig hikariConfig = new HikariConfig();
    // Keeping a small connection pool to a testContainer to avoid overwhelming it.
    hikariConfig.setMaximumPoolSize(2);
    hikariConfig.setJdbcUrl(jdbcUrl);
    hikariConfig.setUsername(container.getUsername());
    hikariConfig.setPassword(container.getPassword());
    hikariConfig.setDriverClassName(container.getDriverClassName());
    existingSource = new HikariDataSource(hikariConfig);
    hikariSources.put(jdbcUrl, existingSource);
    return existingSource;
  }

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

  public static void createTable(
      DataSource dataSource, String tableName, List<KV<String, String>> fieldsAndTypes)
      throws SQLException {
    String fieldsList =
        fieldsAndTypes.stream()
            .map(kv -> kv.getKey() + " " + kv.getValue())
            .collect(Collectors.joining(", "));
    SQLException exception = null;
    Sleeper sleeper = Sleeper.DEFAULT;
    BackOff backoff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.standardSeconds(1))
            .withMaxCumulativeBackoff(Duration.standardMinutes(5))
            .withMaxRetries(4)
            .backoff();
    while (true) {
      // This is not implemented as try-with-resources because it appears that try-with-resources is
      // not correctly catching the PSQLException thrown by dataSource.getConnection()
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute(String.format("create table %s (%s)", tableName, fieldsList));
        return;

      } catch (SQLException e) {
        exception = e;
      }
      boolean hasNext;
      try {
        hasNext = BackOffUtils.next(sleeper, backoff);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      if (!hasNext) {
        // we tried the max number of times
        throw exception;
      }
    }
  }

  public static void createTable(DataSource dataSource, String tableName) throws SQLException {
    createTable(
        dataSource,
        tableName,
        Lists.newArrayList(KV.of("id", "INT"), KV.of("name", "VARCHAR(500)")));
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
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(stmt);
    }
  }

  public static ArrayList<KV<Integer, String>> getTestDataToWrite(long rowsToAdd) {
    ArrayList<KV<Integer, String>> data = new ArrayList<>();
    for (int i = 0; i < rowsToAdd; i++) {
      KV<Integer, String> kv = KV.of(i, TestRow.getNameForSeed(i));
      data.add(kv);
    }
    return data;
  }

  public static void assertRowCount(DataSource dataSource, String tableName, int expectedRowCount)
      throws SQLException {
    int count;
    try (Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName)) {
      resultSet.next();
      count = resultSet.getInt(1);
    }
    // check state outside the try close to enable auto-close resources
    checkState(
        expectedRowCount == count, "Expected count: %d, actual: %d", expectedRowCount, count);
  }
}
