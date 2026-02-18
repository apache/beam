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
package org.apache.beam.sdk.io.clickhouse;

import static org.testcontainers.containers.ClickHouseContainer.HTTP_PORT;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.GenericRecord;
import com.clickhouse.client.api.query.Records;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/** Base setup for ClickHouse containers. */
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked",
})
public class BaseClickHouseTest {

  public static ClickHouseContainer clickHouse;
  public static String clickHouseUrl;
  public static String database;
  public static Network network;
  public static GenericContainer zookeeper;
  static final int CLIENT_TIMEOUT = 30;

  private static final Logger LOG = LoggerFactory.getLogger(BaseClickHouseTest.class);

  private Client client;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    System.setProperty("api.version", "1.44");
    network = Network.newNetwork();

    zookeeper =
        new GenericContainer<>("zookeeper:3.8.4")
            .withStartupAttempts(10)
            .withExposedPorts(2181)
            .withNetwork(network)
            .withNetworkAliases("zookeeper");

    // so far zookeeper container always starts successfully, so no extra retries
    zookeeper.start();

    clickHouse =
        new ClickHouseContainer("clickhouse/clickhouse-server:23.8")
            .withStartupAttempts(10)
            .withNetwork(network)
            .withClasspathResourceMapping(
                "config.d/zookeeper_default.xml",
                "/etc/clickhouse-server/config.d/zookeeper_default.xml",
                BindMode.READ_ONLY);
    ;
    clickHouse.start();
    LOG.info("Start Clickhouse");
    clickHouseUrl = "http://" + clickHouse.getHost() + ":" + clickHouse.getMappedPort(HTTP_PORT);
    database = "default";
  }

  @AfterClass
  public static void tearDown() {
    if (clickHouse != null) {
      clickHouse.close();
    }
    if (zookeeper != null) {
      zookeeper.close();
    }
  }

  @Before
  public void setUp() throws Exception {
    // Create ClickHouse Java Client
    Client.Builder clientBuilder =
        new Client.Builder()
            .addEndpoint(clickHouseUrl)
            .setUsername(clickHouse.getUsername())
            .setPassword(clickHouse.getPassword())
            .setDefaultDatabase(database);

    client = clientBuilder.build();
  }

  @After
  public void after() {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        LOG.warn("Failed to close ClickHouse client", e);
      } finally {
        client = null;
      }
    }
  }

  /**
   * Executes a SQL statement (DDL, DML, etc.).
   *
   * @param sql SQL statement to execute
   * @return true if execution was successful
   * @throws Exception if execution fails
   */
  boolean executeSql(String sql) throws Exception {
    try {
      client.query(sql).get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
      return true;
    } catch (Exception e) {
      LOG.error("Failed to execute SQL: {}", sql, e);
      throw e;
    }
  }

  /**
   * Executes a query and returns the results.
   *
   * @param sql SQL query to execute
   * @return Records containing query results
   * @throws Exception if query fails
   */
  Records executeQuery(String sql) throws Exception {
    try {
      return client.queryRecords(sql).get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
    } catch (Exception e) {
      LOG.error("Failed to execute query: {}", sql, e);
      throw e;
    }
  }

  /**
   * Executes a query and returns the first column of the first row as a long. Useful for COUNT
   * queries or other single-value results.
   *
   * @param sql SQL query to execute
   * @return long value from first column of first row
   * @throws Exception if query fails or result is empty
   */
  long executeQueryAsLong(String sql) throws Exception {
    try (Records records = executeQuery(sql)) {
      for (GenericRecord record : records) {
        // Get the first column value - assuming it's numeric
        return record.getLong(1); // Column index is 1-based
      }
      throw new IllegalStateException("Query returned no results: " + sql);
    } catch (Exception e) {
      LOG.error("Failed to execute query as long: {}", sql, e);
      throw e;
    }
  }

  /**
   * Executes a query and returns the first column of the first row as a String.
   *
   * @param sql SQL query to execute
   * @return String value from first column of first row
   * @throws Exception if query fails or result is empty
   */
  String executeQueryAsString(String sql) throws Exception {
    try (Records records = executeQuery(sql)) {
      for (GenericRecord record : records) {
        return record.getString(1); // Column index is 1-based
      }
      throw new IllegalStateException("Query returned no results: " + sql);
    } catch (Exception e) {
      LOG.error("Failed to execute query as string: {}", sql, e);
      throw e;
    }
  }

  /**
   * Checks if the ClickHouse server is alive and responsive.
   *
   * @return true if server responds to ping
   */
  boolean isServerAlive() {
    try {
      return client != null && client.ping();
    } catch (Exception e) {
      LOG.warn("Server ping failed", e);
      return false;
    }
  }
}
