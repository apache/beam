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

import com.github.dockerjava.api.command.CreateContainerCmd;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;

/** Base setup for ClickHouse containers. */
@SuppressWarnings("unchecked")
public class BaseClickHouseTest {

  public static Network network;
  public static GenericContainer zookeeper;
  public static ClickHouseContainer clickHouse;

  @BeforeClass
  public static void setup() {
    // network sharing doesn't work with ClassRule
    network = Network.newNetwork();

    zookeeper =
        new GenericContainer<>("zookeeper:3.4.13")
            .withExposedPorts(2181)
            .withNetwork(network)
            .withNetworkAliases("zookeeper");
    zookeeper.start();

    clickHouse =
        (ClickHouseContainer)
            new ClickHouseContainer()
                .withCreateContainerCmdModifier(
                    // type inference for `(CreateContainerCmd) -> cmd.` doesn't work
                    cmd ->
                        ((CreateContainerCmd) cmd)
                            .withMemory(256 * 1024 * 1024L)
                            .withMemorySwap(4L * 1024 * 1024 * 1024L))
                .withNetwork(network)
                .withClasspathResourceMapping(
                    "config.d/zookeeper_default.xml",
                    "/etc/clickhouse-server/config.d/zookeeper_default.xml",
                    BindMode.READ_ONLY);
    clickHouse.start();
  }

  @AfterClass
  public static void tearDown() {
    clickHouse.close();
    zookeeper.close();

    try {
      network.close();
    } catch (Exception e) {
      // ignore
    }
  }

  boolean executeSql(String sql) throws SQLException {
    try (Connection connection = clickHouse.createConnection("");
        Statement statement = connection.createStatement()) {
      return statement.execute(sql);
    }
  }

  ResultSet executeQuery(String sql) throws SQLException {
    try (Connection connection = clickHouse.createConnection("");
        Statement statement = connection.createStatement(); ) {
      return statement.executeQuery(sql);
    }
  }

  long executeQueryAsLong(String sql) throws SQLException {
    ResultSet rs = executeQuery(sql);
    rs.next();
    return rs.getLong(1);
  }
}
