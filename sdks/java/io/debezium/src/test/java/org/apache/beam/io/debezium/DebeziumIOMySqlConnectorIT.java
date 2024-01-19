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
package org.apache.beam.io.debezium;

import static org.apache.beam.io.debezium.DebeziumIOPostgresSqlConnectorIT.TABLE_SCHEMA;
import static org.apache.beam.sdk.testing.SerializableMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.connector.mysql.MySqlConnector;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import javax.sql.DataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class DebeziumIOMySqlConnectorIT {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumIOMySqlConnectorIT.class);
  /**
   * Debezium - MySqlContainer
   *
   * <p>Creates a docker container using the image used by the debezium tutorial.
   */
  @ClassRule
  public static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>(
              DockerImageName.parse("debezium/example-mysql:1.4")
                  .asCompatibleSubstituteFor("mysql"))
          .withPassword("debezium")
          .withUsername("mysqluser")
          .withExposedPorts(3306)
          .waitingFor(
              new HttpWaitStrategy()
                  .forPort(3306)
                  .forStatusCodeMatching(response -> response == 200)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  public static DataSource getMysqlDatasource(Void unused) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(MY_SQL_CONTAINER.getJdbcUrl());
    hikariConfig.setUsername(MY_SQL_CONTAINER.getUsername());
    hikariConfig.setPassword(MY_SQL_CONTAINER.getPassword());
    hikariConfig.setDriverClassName(MY_SQL_CONTAINER.getDriverClassName());
    return new HikariDataSource(hikariConfig);
  }

  private void monitorEssentialMetrics() {
    DataSource ds = getMysqlDatasource(null);
    try {
      Connection conn = ds.getConnection();
      Statement st = conn.createStatement();
      while (true) {
        ResultSet rs = st.executeQuery("SHOW STATUS WHERE `variable_name` = 'Threads_connected'");
        if (rs.next()) {
          LOG.info("Open connections: {}", rs.getLong(2));
          rs.close();
          Thread.sleep(4000);
        } else {
          throw new IllegalArgumentException("OIOI");
        }
      }
    } catch (InterruptedException | SQLException ex) {
      throw new IllegalArgumentException("Oi", ex);
    }
  }

  @Test
  public void testDebeziumSchemaTransformMysqlRead() throws InterruptedException {
    long writeSize = 500L;
    long testTime = writeSize * 200L;
    MY_SQL_CONTAINER.start();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline writePipeline = Pipeline.create(options);
    writePipeline
        .apply(
            GenerateSequence.from(0)
                .to(writeSize)
                .withRate(10, org.joda.time.Duration.standardSeconds(1)))
        .apply(
            MapElements.into(TypeDescriptors.rows())
                .via(
                    num ->
                        Row.withSchema(TABLE_SCHEMA)
                            .withFieldValue(
                                "id",
                                // We need this tricky conversion because the original "customers"
                                // table already
                                // contains rows 1001, 1002, 1003, 1004.
                                num <= 1000
                                    ? Long.valueOf(num).intValue()
                                    : Long.valueOf(num).intValue() + 4)
                            .withFieldValue("first_name", Long.toString(num))
                            .withFieldValue("last_name", Long.toString(writeSize - num))
                            .withFieldValue("email", Long.toString(num) + "@beamail.com")
                            // TODO(pabloem): Add other data types
                            .build()))
        .setRowSchema(TABLE_SCHEMA)
        .apply(
            JdbcIO.<Row>write()
                .withTable("inventory.customers")
                .withDataSourceProviderFn(DebeziumIOMySqlConnectorIT::getMysqlDatasource));

    Pipeline readPipeline = Pipeline.create(options);
    PCollection<Row> result =
        PCollectionRowTuple.empty(readPipeline)
            .apply(
                new DebeziumReadSchemaTransformProvider(
                        true, Long.valueOf(writeSize).intValue() + 4, testTime)
                    .from(
                        DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration
                            .builder()
                            .setDatabase("MYSQL")
                            .setPassword("dbz")
                            .setUsername("debezium")
                            .setHost("localhost")
                            .setTable("inventory.customers")
                            .setPort(MY_SQL_CONTAINER.getMappedPort(3306))
                            .build()))
            .get("output");

    PAssert.that(result)
        .satisfies(
            rows -> {
              assertThat(
                  Lists.newArrayList(rows).size(), equalTo(Long.valueOf(writeSize + 4).intValue()));
              return null;
            });
    Thread writeThread = new Thread(() -> writePipeline.run().waitUntilFinish());
    Thread monitorThread = new Thread(this::monitorEssentialMetrics);
    monitorThread.start();
    writeThread.start();
    readPipeline.run().waitUntilFinish();
    writeThread.join();
    monitorThread.interrupt();
    monitorThread.join();
  }

  /**
   * Debezium - MySQL connector Test.
   *
   * <p>Tests that connector can actually connect to the database
   */
  @Test
  public void testDebeziumIOMySql() {
    MY_SQL_CONTAINER.start();

    String host = MY_SQL_CONTAINER.getContainerIpAddress();
    String port = MY_SQL_CONTAINER.getMappedPort(3306).toString();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> results =
        p.apply(
            DebeziumIO.<String>read()
                .withConnectorConfiguration(
                    DebeziumIO.ConnectorConfiguration.create()
                        .withUsername("debezium")
                        .withPassword("dbz")
                        .withConnectorClass(MySqlConnector.class)
                        .withHostName(host)
                        .withPort(port)
                        .withConnectionProperty("database.server.id", "184054")
                        .withConnectionProperty("database.server.name", "dbserver1")
                        .withConnectionProperty("database.include.list", "inventory")
                        .withConnectionProperty("include.schema.changes", "false"))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withMaxNumberOfRecords(30)
                .withCoder(StringUtf8Coder.of()));
    String expected =
        "{\"metadata\":{\"connector\":\"mysql\",\"version\":\"1.3.1.Final\",\"name\":\"dbserver1\","
            + "\"database\":\"inventory\",\"schema\":\"mysql-bin.000003\",\"table\":\"addresses\"},\"before\":null,"
            + "\"after\":{\"fields\":{\"zip\":\"76036\",\"city\":\"Euless\","
            + "\"street\":\"3183 Moore Avenue\",\"id\":10,\"state\":\"Texas\",\"customer_id\":1001,"
            + "\"type\":\"SHIPPING\"}}}";

    PAssert.that(results)
        .satisfies(
            (Iterable<String> res) -> {
              assertThat(res, hasItem(expected));
              return null;
            });

    p.run().waitUntilFinish();
    MY_SQL_CONTAINER.stop();
  }
}
