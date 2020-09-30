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
import com.github.dockerjava.api.model.Image;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.apache.beam.sdk.util.BackOff;
import org.apache.beam.sdk.util.BackOffUtils;
import org.apache.beam.sdk.util.FluentBackoff;
import org.apache.beam.sdk.util.Sleeper;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.joda.time.Duration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ClickHouseContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

/** Base setup for ClickHouse containers. */
@SuppressWarnings("unchecked")
public class BaseClickHouseTest {

  public static Network network;
  public static GenericContainer zookeeper;
  public static ClickHouseContainer clickHouse;

  // yandex/clickhouse-server:19.1.6
  // use SHA256 not to pull docker hub for tag if image already exists locally
  private static final String CLICKHOUSE_IMAGE =
      "yandex/clickhouse-server@"
          + "sha256:c75f66f3619ca70a9f7215966505eaed2fc0ca0ee7d6a7b5407d1b14df8ddefc";

  private static final Logger LOG = LoggerFactory.getLogger(BaseClickHouseTest.class);

  @BeforeClass
  public static void setup() throws IOException, InterruptedException {
    // network sharing doesn't work with ClassRule
    network = Network.newNetwork();

    zookeeper =
        new GenericContainer<>(DockerImageName.parse("zookeeper:3.4.13"))
            .withStartupAttempts(10)
            .withExposedPorts(2181)
            .withNetwork(network)
            .withNetworkAliases("zookeeper");

    // so far zookeeper container always starts successfully, so no extra retries
    zookeeper.start();

    clickHouse =
        (ClickHouseContainer)
            new ClickHouseContainer(CLICKHOUSE_IMAGE)
                .withStartupAttempts(10)
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

    BackOff backOff =
        FluentBackoff.DEFAULT
            .withMaxRetries(3)
            .withInitialBackoff(Duration.standardSeconds(15))
            .backoff();

    // try to start clickhouse-server a couple of times, see BEAM-6639
    while (true) {
      try {
        Unreliables.retryUntilSuccess(
            10,
            () -> {
              DockerClientFactory.instance()
                  .checkAndPullImage(DockerClientFactory.instance().client(), CLICKHOUSE_IMAGE);

              return null;
            });

        clickHouse.start();
        break;
      } catch (Exception e) {
        if (!BackOffUtils.next(Sleeper.DEFAULT, backOff)) {
          throw e;
        } else {
          List<Image> images =
              DockerClientFactory.instance().client().listImagesCmd().withShowAll(true).exec();
          String listImagesOutput = "listImagesCmd:\n" + Joiner.on('\n').join(images) + "\n";

          LOG.warn("failed to start clickhouse-server\n\n" + listImagesOutput, e);
        }
      }
    }
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
