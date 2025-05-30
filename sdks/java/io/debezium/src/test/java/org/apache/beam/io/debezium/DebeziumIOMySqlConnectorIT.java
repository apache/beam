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
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class DebeziumIOMySqlConnectorIT {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumIOMySqlConnectorIT.class);

  private static final DockerImageName KAFKA_IMAGE =
      DockerImageName.parse("confluentinc/cp-kafka:7.6.0");

  /**
   * Debezium - MySqlContainer
   *
   * <p>Creates a docker container using the image used by the debezium tutorial.
   */
  @ClassRule
  public static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>(
              DockerImageName.parse("debezium/example-mysql:3.0.0.Final") // Ensure this image version is compatible with Debezium 3.1.1
                  .asCompatibleSubstituteFor("mysql"))
          .withDatabaseName("inventory") // Ensures the 'inventory' database is created.
          .withUsername("root") // Connect as root initially to grant privileges
          .withPassword("debezium") // Root password for this specific Debezium image
          .withExposedPorts(3306)
          // Add a command to increase the server's connection timeout to prevent dropped connections.
          .withCommand("--wait_timeout=28800", "--max_allowed_packet=64M") // Added max_allowed_packet
          // Add a robust wait strategy to ensure the DB is fully ready before tests run.
          .waitingFor(Wait.forLogMessage(".*ready for connections.*\\s", 2) // Wait for 2 occurrences
                         .withStartupTimeout(Duration.ofMinutes(3))); // Increase startup timeout

  @ClassRule public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(KAFKA_IMAGE);

  @BeforeClass
  public static void startContainersAndGrantPrivileges() throws Exception {
    Startables.deepStart(Stream.of(MY_SQL_CONTAINER, KAFKA_CONTAINER)).join();
    try (Connection conn =
            DriverManager.getConnection(
                MY_SQL_CONTAINER.getJdbcUrl(),
                "root",
                MY_SQL_CONTAINER.getPassword());
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES, FLUSH TABLES, PROCESS ON *.* TO 'mysqluser'@'%'");
      LOG.info(
          "Granted necessary privileges to 'mysqluser'@'%' in MySQL.");
    } catch (SQLException e) {
      LOG.error("Failed to grant privileges to 'mysqluser' in MySQL container", e);
      throw e;
    }
  }

  @AfterClass
  public static void stopContainers() {
    if (KAFKA_CONTAINER != null && KAFKA_CONTAINER.isRunning()) {
      KAFKA_CONTAINER.stop();
    }
    if (MY_SQL_CONTAINER != null && MY_SQL_CONTAINER.isRunning()) {
      MY_SQL_CONTAINER.stop();
    }
  }

  public static DataSource getMysqlDatasource(Void unused) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(MY_SQL_CONTAINER.getJdbcUrl());
    hikariConfig.setUsername("mysqluser");
    hikariConfig.setPassword("debezium");
    hikariConfig.setDriverClassName(MY_SQL_CONTAINER.getDriverClassName());
    hikariConfig.setConnectionTimeout(30000); // 30 seconds
    hikariConfig.setIdleTimeout(60000); // 1 minute
    hikariConfig.setMaxLifetime(180000); // 3 minutes
    return new HikariDataSource(hikariConfig);
  }

  private void monitorEssentialMetrics() {
    DataSource ds = getMysqlDatasource(null);
    LOG.info("Monitoring thread started for MySQL connections.");
    try (Connection conn = ds.getConnection();
        Statement st = conn.createStatement()) {
      while (!Thread.currentThread().isInterrupted()) {
        try (ResultSet rs =
            st.executeQuery("SHOW STATUS WHERE `variable_name` = 'Threads_connected'")) {
          if (rs.next()) {
            LOG.info("MySQL Open connections: {}", rs.getLong(2));
          } else {
            LOG.warn("Could not retrieve 'Threads_connected' status from MySQL.");
            // Consider if breaking the loop is appropriate here or just log and continue
            break;
          }
        }
        // Make sleep interruptible
        Thread.sleep(4000);
      }
    } catch (InterruptedException e) {
      // This is an expected part of shutting down the monitor thread.
      LOG.info("MySQL connection monitoring thread interrupted normally.");
      // Restore the interrupted status
      Thread.currentThread().interrupt();
    } catch (SQLException ex) {
      // Log SQL exceptions but don't let them kill the test with an unrelated IllegalArgumentException
      LOG.error("SQLException in MySQL connection monitoring thread, exiting monitor.", ex);
    }
    LOG.info("Monitoring thread finished for MySQL connections.");
  }

  @Test
  public void testDebeziumSchemaTransformMysqlRead() throws InterruptedException {
    long writeSize = 500L;
    // Significantly increased minimum test time to 180 seconds (3 minutes) to allow more time for Debezium snapshot.
    long testTime = Math.max(writeSize * 300L, 180000L); 

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline writePipeline = Pipeline.create(options);
    writePipeline
        .apply(
            GenerateSequence.from(0)
                .to(writeSize)
                .withRate(
                    10, org.joda.time.Duration.standardSeconds(1)))
        .apply(
            MapElements.into(TypeDescriptors.rows())
                .via(
                    num ->
                        Row.withSchema(TABLE_SCHEMA)
                            .withFieldValue(
                                "id",
                                num <= 1000
                                    ? Long.valueOf(num).intValue()
                                    : Long.valueOf(num).intValue() + 4)
                            .withFieldValue("first_name", "FN-" + num)
                            .withFieldValue("last_name", "LN-" + (writeSize - num))
                            .withFieldValue("email", num + "@beamail.com")
                            .build()))
        .setRowSchema(TABLE_SCHEMA)
        .apply(
            JdbcIO.<Row>write()
                .withTable("inventory.customers")
                .withDataSourceProviderFn(
                    DebeziumIOMySqlConnectorIT
                        ::getMysqlDatasource));

    Pipeline readPipeline = Pipeline.create(options);
    PCollection<Row> result =
        PCollectionRowTuple.empty(readPipeline)
            .apply(
                "ReadFromMySQLDebeziumSchemaTransform", // Ensure unique name
                new DebeziumReadSchemaTransformProvider(
                        true, // isStreaming
                        Long.valueOf(writeSize + 4)
                            .intValue(), // numMessagesToAssert (initial 4 in DB + writes)
                        testTime) // maxSecondsToFinish (milliseconds for the provider)
                    .from(
                        DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration
                            .builder()
                            .setDatabase("MYSQL")
                            .setPassword(
                                "debezium")
                            .setUsername(
                                "mysqluser")
                            .setHost(
                                MY_SQL_CONTAINER
                                    .getHost())
                            .setTable("inventory.customers") // Table for the ReadSchemaTransform to focus on
                            .setPort(MY_SQL_CONTAINER.getMappedPort(3306))
                            .setDebeziumConnectionProperties(
                                Lists.newArrayList(
                                    "schema.history.internal.kafka.bootstrap.servers="
                                        + KAFKA_CONTAINER.getBootstrapServers(),
                                    "schema.history.internal.kafka.topic=schema-history-mysql-transform-"
                                        + System.nanoTime(),
                                    "schema.history.internal=io.debezium.storage.kafka.history.KafkaSchemaHistory",
                                    "database.server.id=" + (6000 + (int) (Math.random() * 500)), // Wider range for server ID
                                    "database.server.name=mysql-transform-server-" + System.nanoTime(),
                                    "database.include.list=inventory",
                                    "table.include.list=inventory.customers", // Ensure this aligns with setTable()
                                    "snapshot.mode=initial", // Explicitly 'initial'
                                    "snapshot.locking.mode=none",
                                    "snapshot.delay.ms=5000", 
                                    "snapshot.fetch.size=2048", // Default is 2048, explicit
                                    "connect.timeout.ms=90000", // Debezium's own connection timeout to DB (90s)
                                    "heartbeat.interval.ms=10000", // Add heartbeat
                                    "max.queue.size=8192", // Default
                                    "max.batch.size=2048", // Default
                                    "database.history.ddl.filter=DROP TABLE.*"
                                    ))
                            .build()))
            .get("output");

    PAssert.that(result)
        .satisfies(
            rows -> {
              assertThat(
                  "Number of rows received",
                  Lists.newArrayList(rows).size(),
                  equalTo(
                      Long.valueOf(writeSize + 4)
                          .intValue())); // Existing 4 + new inserts
              return null;
            });

    Thread writeThread =
        new Thread(
            () -> {
              LOG.info("Starting write pipeline for SchemaTransform test...");
              writePipeline.run().waitUntilFinish();
              LOG.info("Write pipeline for SchemaTransform test finished.");
            });
    Thread monitorThread = new Thread(this::monitorEssentialMetrics);

    monitorThread.start();
    writeThread.start();

    LOG.info("Starting read pipeline for SchemaTransform test...");
    readPipeline.run().waitUntilFinish();
    LOG.info("Read pipeline for SchemaTransform test finished.");

    writeThread.join();
    LOG.info("Write thread for SchemaTransform test joined.");
    monitorThread.interrupt();
    monitorThread.join();
    LOG.info("Monitor thread for SchemaTransform test joined.");
  }

  @Test
  public void testDebeziumIOMySql() {
    String host = MY_SQL_CONTAINER.getHost();
    String port = MY_SQL_CONTAINER.getMappedPort(3306).toString();
    String kafkaBootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
    String schemaHistoryTopic = "mysql-schema-history-io-" + System.nanoTime();
    String username = "mysqluser";
    String password = "debezium";

    Properties dbzConnectorProps = new Properties();
    dbzConnectorProps.setProperty("database.hostname", host);
    dbzConnectorProps.setProperty("database.port", port);
    dbzConnectorProps.setProperty("database.user", username);
    dbzConnectorProps.setProperty("database.password", password);
    dbzConnectorProps.setProperty(
        "database.server.id",
        String.valueOf(190000 + (int) (Math.random() * 2000))); // Wider range for server ID
    dbzConnectorProps.setProperty("database.server.name", "dbserver1-io-" + System.nanoTime());
    dbzConnectorProps.setProperty(
        "database.include.list", "inventory");
    dbzConnectorProps.setProperty(
        "table.include.list",
        "inventory.addresses,inventory.customers"); // Check both for this direct IO test
    dbzConnectorProps.setProperty("snapshot.mode", "initial"); 
    dbzConnectorProps.setProperty("snapshot.locking.mode", "none");
    dbzConnectorProps.setProperty("snapshot.delay.ms", "5000"); 
    dbzConnectorProps.setProperty("snapshot.fetch.size", "2048");
    dbzConnectorProps.setProperty("connect.timeout.ms", "90000"); 
    dbzConnectorProps.setProperty("heartbeat.interval.ms", "10000");
    dbzConnectorProps.setProperty("max.queue.size", "8192"); 
    dbzConnectorProps.setProperty("max.batch.size", "2048"); 
    dbzConnectorProps.setProperty("database.history.ddl.filter", "DROP TABLE.*"); 

    dbzConnectorProps.setProperty(
        "schema.history.internal.kafka.bootstrap.servers", kafkaBootstrapServers);
    dbzConnectorProps.setProperty("schema.history.internal.kafka.topic", schemaHistoryTopic);
    dbzConnectorProps.setProperty(
        "schema.history.internal", "io.debezium.storage.kafka.history.KafkaSchemaHistory");

    Map<String, String> connectorPropsMap =
        dbzConnectorProps.entrySet().stream()
            .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> results =
        p.apply(
            "ReadFromMySQLDebeziumDirectIO", // Ensure unique name
            DebeziumIO.<String>read()
                .withConnectorConfiguration(
                    DebeziumIO.ConnectorConfiguration.create()
                        .withConnectorClass(MySqlConnector.class)
                        .withConnectionProperties(connectorPropsMap))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withMaxNumberOfRecords(
                    4 + 4) // Expect 4 from addresses, 4 from customers from initial dataset
                .withMaxTimeToRun(5 * 60 * 1000L) // 5 minutes in milliseconds. Keep this long.
                .withCoder(StringUtf8Coder.of()));

    String expectedAddressRecordPart =
        "\"table\":\"addresses\""
            + ".*\"zip\":\"76036\""
            + ".*\"customer_id\":1001"; 

    PAssert.that(results)
        .satisfies(
            (Iterable<String> res) -> {
              boolean foundExpectedAddress = false;
              int recordCount = 0;
              for (String recordJson : res) {
                recordCount++;
                LOG.debug("DebeziumIO MySQL Received: {}", recordJson);
                if (recordJson.matches("(?s).*" + expectedAddressRecordPart + ".*")) {
                  foundExpectedAddress = true;
                  LOG.info("Found matching address record: {}", recordJson);
                }
              }
              assertThat("Should have found the specific address record", foundExpectedAddress, equalTo(true));
              assertThat("Expected initial records from snapshot", recordCount, equalTo(4+4));
              return null;
            });

    LOG.info("Starting DebeziumIO MySQL direct read pipeline...");
    p.run().waitUntilFinish();
    LOG.info("DebeziumIO MySQL direct read pipeline finished.");
  }
}
