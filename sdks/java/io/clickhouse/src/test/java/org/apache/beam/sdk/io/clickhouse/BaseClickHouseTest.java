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

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
  public static Network network;
  public static GenericContainer zookeeper;
  private static final Logger LOG = LoggerFactory.getLogger(BaseClickHouseTest.class);

  private Connection connection;

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
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
  }

  @AfterClass
  public static void tearDown() {
    clickHouse.close();
    zookeeper.close();
  }

  @Before
  public void setUp() throws SQLException {
    connection = clickHouse.createConnection("");
  }

  @After
  public void after() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // failed to close connection, ignore
      } finally {
        connection = null;
      }
    }
  }

  boolean executeSql(String sql) throws SQLException {
    Statement statement = connection.createStatement();
    return statement.execute(sql);
  }

  ResultSet executeQuery(String sql) throws SQLException {
    Statement statement = connection.createStatement();
    return statement.executeQuery(sql);
  }

  long executeQueryAsLong(String sql) throws SQLException {
    ResultSet rs = executeQuery(sql);
    rs.next();
    return rs.getLong(1);
  }
}
