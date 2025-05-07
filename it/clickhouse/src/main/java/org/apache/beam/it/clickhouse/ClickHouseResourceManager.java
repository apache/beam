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
package org.apache.beam.it.clickhouse;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.it.testcontainers.TestContainerResourceManager;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

/**
 * Client for managing ClickHouse resources.
 *
 * <p>The class is thread-safe.
 */
public class ClickHouseResourceManager extends TestContainerResourceManager<GenericContainer<?>>
    implements ResourceManager {

  private static final Logger LOG = LoggerFactory.getLogger(ClickHouseResourceManager.class);

  private final Connection connection;

  // 8123 is the default port that ClickHouse is configured to listen on
  private static final int CLICKHOUSE_INTERNAL_PORT = 8123;

  private static final String DEFAULT_CLICKHOUSE_CONTAINER_NAME = "clickhouse/clickhouse-server";

  private static final String DEFAULT_CLICKHOUSE_CONTAINER_TAG = "23.8";

  private final String jdbcConnectionString;

  final List<String> managedTableNames = new ArrayList<>();

  ClickHouseResourceManager(Builder builder) throws SQLException {
    this(/* clickHouseConnection*/ null, buildContainer(builder), builder);
  }

  private static ClickHouseContainer buildContainer(Builder builder) {
    ClickHouseContainer container =
        new ClickHouseContainer(
                DockerImageName.parse(builder.containerImageName)
                    .withTag(builder.containerImageTag))
            .withStartupAttempts(10);

    Duration startupTimeout = Duration.ofMinutes(2);
    container.setWaitStrategy(new LogMessageWaitStrategy().withStartupTimeout(startupTimeout));

    return container;
  }

  //    @VisibleForTesting
  @SuppressWarnings("nullness")
  ClickHouseResourceManager(
      @Nullable Connection clickHosueConnection, ClickHouseContainer container, Builder builder)
      throws SQLException {
    super(container, builder);

    this.jdbcConnectionString =
        "jdbc:clickhouse://"
            + this.getHost()
            + ":"
            + this.getPort(CLICKHOUSE_INTERNAL_PORT)
            + "/default";

    this.connection =
        clickHosueConnection != null ? clickHosueConnection : container.createConnection("");
  }

  /** Builder for {@link ClickHouseResourceManager}. */
  public static final class Builder
      extends TestContainerResourceManager.Builder<ClickHouseResourceManager> {

    Builder(String testId) {
      super(testId, DEFAULT_CLICKHOUSE_CONTAINER_NAME, DEFAULT_CLICKHOUSE_CONTAINER_TAG);
    }

    @Override
    public ClickHouseResourceManager build() {
      try {
        return new ClickHouseResourceManager(this);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static Builder builder(String testId) {
    return new Builder(testId);
  }

  /** Returns the JDBC connection string to the ClickHouse service. */
  public synchronized String getJdbcConnectionString() {
    return jdbcConnectionString;
  }

  synchronized boolean tableExists(String tableName) throws SQLException {

    ClickHouseUtils.checkValidTableName(tableName);
    String query = String.format("EXISTS TABLE %s", tableName);

    try (Statement statement = connection.createStatement();
        ResultSet res = statement.executeQuery(query)) {

      if (res.next()) {
        return res.getInt(1) == 1; // ClickHouse returns 1 if the table exists, 0 otherwise
      }

      return false;
    }
  }

  /**
   * Creates a table on ClickHouse.
   *
   * @param tableName Name of the table to create.
   * @return A boolean indicating whether the resource was created.
   * @throws ClickHouseResourceManagerException if there is an error creating the table in
   *     ClickHouse.
   */
  public synchronized boolean createTable(
      String tableName, Map<String, String> columns, String engine, List<String> orderBy)
      throws ClickHouseResourceManagerException {

    LOG.info("Creating table '{}' in ClickHouse.", tableName);

    try {

      ClickHouseUtils.checkValidTableName(tableName);

      if (tableExists(tableName)) {
        LOG.warn("Table '{}' already exists.", tableName);
        return false;
      }

      if (columns == null || columns.isEmpty()) {
        throw new ClickHouseResourceManagerException(
            "Cannot create table with no columns.",
            new Exception("Cannot create table with no columns."));
      }

      if (engine == null || engine.isEmpty()) {
        engine = "MergeTree()";
      }

      if (orderBy == null || orderBy.isEmpty()) {
        orderBy = Collections.singletonList(columns.keySet().iterator().next()); // First column
      }

      String columnDefinitions =
          columns.entrySet().stream()
              .map(entry -> entry.getKey() + " " + entry.getValue())
              .collect(Collectors.joining(", "));

      // Construct CREATE TABLE query
      String createTableQuery =
          String.format(
              "CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = %s ORDER BY (%s)",
              tableName, columnDefinitions, engine, String.join(", ", orderBy));

      LOG.info("Executing query: {}", createTableQuery);

      try (Statement statement = connection.createStatement()) {
        statement.execute(createTableQuery);
        return tableExists(tableName);
      }

    } catch (Exception e) {
      throw new ClickHouseResourceManagerException("Error creating table: " + tableName, e);
    }
  }

  /**
   * Inserts the given rows into a ClickHouse table.
   *
   * <p>Note: Implementations may create the table if it does not already exist.
   *
   * @param tableName The name of the table to insert the rows into.
   * @param rows A list of rows, where each row is a map of (column name -> value).
   * @return A boolean indicating whether the rows were inserted successfully.
   * @throws ClickHouseResourceManagerException if there is an error inserting the rows.
   */
  public synchronized boolean insertRows(String tableName, List<Map<String, Object>> rows)
      throws ClickHouseResourceManagerException {
    LOG.info("Attempting to write {} rows to {}.", rows.size(), tableName);

    if (rows.isEmpty()) {
      LOG.warn("No rows provided for insertion.");
      return false;
    }

    try {
      // Extract column names from the first row
      List<String> columns = new ArrayList<>(rows.get(0).keySet());

      String columnNames = String.join(", ", columns);
      String placeholders = String.join(", ", Collections.nCopies(columns.size(), "?"));
      String insertQuery =
          String.format("INSERT INTO %s (%s) VALUES (%s)", tableName, columnNames, placeholders);

      try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery)) {
        // Add batch inserts
        for (Map<String, Object> row : rows) {
          int index = 1;
          for (String column : columns) {
            preparedStatement.setObject(index++, row.get(column));
          }
          preparedStatement.addBatch();
        }

        // Execute batch insert
        int[] result = preparedStatement.executeBatch();

        boolean allSuccessful =
            Arrays.stream(result)
                .allMatch(resRows -> resRows > 0 || resRows == Statement.SUCCESS_NO_INFO);
        if (!allSuccessful) {
          LOG.warn("Some rows might have failed to insert into {}", tableName);
          return false;
        }

        int affectedRows = Arrays.stream(result).sum();
        LOG.info("Successfully wrote {} rows to {}", affectedRows, tableName);
        return true;
      }
    } catch (Exception e) {
      throw new ClickHouseResourceManagerException("Error inserting rows into ClickHouse. ", e);
    }
  }

  /**
   * Reads all the rows in a table.
   *
   * @param tableName The name of the table to read from.
   * @return An iterable of all the Rows in the collection.
   * @throws ClickHouseResourceManagerException if there is an error reading the data.
   */
  public synchronized List<Map<String, Object>> fetchAll(String tableName)
      throws ClickHouseResourceManagerException {
    LOG.info("Reading all rows from {}.", tableName);

    String query = String.format("SELECT * FROM %s", tableName);

    try (Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(query)) {

      List<Map<String, Object>> rows = new ArrayList<>();
      ResultSetMetaData metaData = resultSet.getMetaData();
      int columnCount = metaData.getColumnCount();

      while (resultSet.next()) {
        Map<String, Object> row = new HashMap<>();
        for (int i = 1; i <= columnCount; i++) {
          Object value = resultSet.getObject(i);
          row.put(metaData.getColumnName(i), value != null ? value : "NULL");
        }
        rows.add(row);
      }

      return rows;

    } catch (SQLException e) {
      LOG.error("Error during reading table {}.", tableName, e);
      throw new ClickHouseResourceManagerException(
          "Error during reading table " + tableName + ".", e);
    }
  }

  /**
   * Gets the count of rows in a table.
   *
   * @param tableName The name of the table to read from.
   * @return The number of rows for the given table
   * @throws ClickHouseResourceManagerException if there is an error reading the data.
   */
  public long count(String tableName) throws ClickHouseResourceManagerException {
    LOG.info("Fetching count from {}.", tableName);

    String query = String.format("SELECT count(*) FROM %s", tableName);

    try (Statement statement = connection.createStatement();
        ResultSet countRes = statement.executeQuery(query)) {
      if (countRes.next()) {
        return countRes.getLong(1);
      } else {
        throw new ClickHouseResourceManagerException(
            "No result returned for count query.", new Error());
      }

    } catch (Exception e) {
      throw new ClickHouseResourceManagerException(
          "Error fetching count from " + tableName + ".", e);
    }
  }

  /**
   * Deletes all created resources and cleans up the ClickHouse client, making the manager object
   * unusable.
   *
   * @throws ClickHouseResourceManagerException if there is an error deleting the ClickHouse
   *     resources.
   */
  @Override
  public synchronized void cleanupAll() throws ClickHouseResourceManagerException {
    LOG.info("Attempting to clean up ClickHouse manager.");

    if (!managedTableNames.isEmpty()) {
      try (Statement statement = connection.createStatement()) {
        for (String tableName : managedTableNames) {
          String dropTableQuery = String.format("DROP TABLE IF EXISTS %s", tableName);
          statement.execute(dropTableQuery);
          LOG.info("Dropped table: {}", tableName);
        }
        managedTableNames.clear();
      } catch (Exception e) {
        LOG.error("Failed to delete ClickHouse tables: {}", managedTableNames, e);
        throw new ClickHouseResourceManagerException("Failed deleting ClickHouse tables", e);
      }
    }

    super.cleanupAll();

    LOG.info("ClickHouse manager successfully cleaned up.");
  }
}
