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
              DockerImageName.parse("debezium/example-mysql:3.0.0.Final")
                  .asCompatibleSubstituteFor("mysql"))
          .withPassword("debezium")
          .withUsername("mysqluser")
          .withExposedPorts(3306)
          .withCommand("--wait_timeout=28800")
          // .waitingFor(
          //     new HttpWaitStrategy()
          //         .forPort(3306)
          //         .forStatusCodeMatching(response -> response == 200)
          //         .withStartupTimeout(Duration.ofMinutes(2)))
          .waitingFor(Wait.forLogMessage(".*ready for connections.*\\s", 1));;

  // Added Kafka Testcontainer for schema history
  @ClassRule public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(KAFKA_IMAGE);

  @BeforeClass
  public static void startContainersAndGrantPrivileges() throws Exception {
    // Start containers. Testcontainers will manage parallel startup.
    Startables.deepStart(Stream.of(MY_SQL_CONTAINER, KAFKA_CONTAINER)).join();

    try (Connection conn =
            DriverManager.getConnection(
                MY_SQL_CONTAINER.getJdbcUrl(),
                "root", // Connect as root to grant privileges
                MY_SQL_CONTAINER.getPassword()); // Root password
        Statement stmt = conn.createStatement()) {
      stmt.execute("GRANT REPLICATION CLIENT ON *.* TO 'mysqluser'@'%'");
      stmt.execute(
          "GRANT SELECT, RELOAD, FLUSH_TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO"
              + " 'mysqluser'@'%'");
      stmt.execute("FLUSH PRIVILEGES");
      LOG.info(
          "Granted necessary privileges (SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE,"
              + " REPLICATION CLIENT, LOCK TABLES, FLUSH_TABLES, PROCESS) to 'mysqluser'@'%' in"
              + " MySQL.");
    } catch (SQLException e) {
      LOG.error("Failed to grant privileges to 'mysqluser' in MySQL container", e);
      throw e; // Rethrow to fail fast if setup fails
    }
  }

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
      // Log SQL exceptions but don't let them kill the test with an unrelated
      // IllegalArgumentException
      LOG.error("SQLException in MySQL connection monitoring thread, exiting monitor.", ex);
    }
    LOG.info("Monitoring thread finished for MySQL connections.");
  }

  @Test
  public void testDebeziumSchemaTransformMysqlRead() throws InterruptedException {
    long writeSize = 500L;
    long testTime = Math.max(writeSize * 200L, 120000L);
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
                            .setDebeziumConnectionProperties(
                                Lists.newArrayList(
                                    "schema.history.internal.kafka.bootstrap.servers="
                                        + KAFKA_CONTAINER.getBootstrapServers(),
                                    "schema.history.internal.kafka.topic=schema-history-mysql-transform-"
                                        + System.nanoTime(),
                                    "schema.history.internal=io.debezium.storage.kafka.history.KafkaSchemaHistory",
                                    "database.server.id=" + (5400 + (int) (Math.random() * 100)),
                                    "database.server.name=mysql-transform-server-"
                                        + System.nanoTime(),
                                    "database.include.list=inventory",
                                    "table.include.list=inventory.customers",
                                    "snapshot.mode=initial",
                                    "snapshot.locking.mode=none",
                                    "snapshot.delay.ms=5000", // Increased snapshot delay
                                    "connect.timeout.ms=60000",
                                    "database.history.ddl.filter=DROP TABLE.*"))
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

    // MY_SQL_CONTAINER.start(); // Container is started by @ClassRule
    // KAFKA_CONTAINER.start(); // Container is started by @ClassRule

    String host = MY_SQL_CONTAINER.getHost();
    String port = MY_SQL_CONTAINER.getMappedPort(3306).toString();
    String kafkaBootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
    String schemaHistoryTopic = "mysql-schema-history-io-" + System.nanoTime(); // Unique topic
    String username = MY_SQL_CONTAINER.getUsername();
    String password = MY_SQL_CONTAINER.getPassword();
    // --- Create Debezium Connector Properties ---
    Properties dbzConnectorProps = new Properties();
    // MySQL connection properties
    dbzConnectorProps.setProperty("database.hostname", host);
    dbzConnectorProps.setProperty("database.port", port);
    dbzConnectorProps.setProperty("database.user", MY_SQL_CONTAINER.getUsername());
    dbzConnectorProps.setProperty("database.password", MY_SQL_CONTAINER.getPassword());
    dbzConnectorProps.setProperty("database.server.id", "184054");
    dbzConnectorProps.setProperty("database.server.name", "dbserver1");
    dbzConnectorProps.setProperty(
        "database.include.list", "inventory"); // Comma-separated list of databases
    // dbzConnectorProps.setProperty(
    //     "table.include.list",
    //     "inventory.addresses"); // Comma-separated list of tables (fully qualified)
    // dbzConnectorProps.setProperty("include.schema.changes", "false"); // Default is true, usually
    // needed for schema history.
    // Consider if you really want this false. For robust schema history, true is better.

    dbzConnectorProps.setProperty(
        "table.include.list",
        "inventory.addresses,inventory.customers"); // Check both for this direct IO test
    dbzConnectorProps.setProperty("snapshot.mode", "initial"); // Explicitly 'initial'
    dbzConnectorProps.setProperty("snapshot.locking.mode", "none");
    dbzConnectorProps.setProperty("snapshot.delay.ms", "5000"); // Increased snapshot delay
    dbzConnectorProps.setProperty(
        "connect.timeout.ms", "60000"); // Debezium's own connection timeout to DB
    dbzConnectorProps.setProperty("database.history.ddl.filter", "DROP TABLE.*"); // Keep DDL filter
    // --- Kafka Schema History Properties ---
    dbzConnectorProps.setProperty(
        "schema.history.internal.kafka.bootstrap.servers", kafkaBootstrapServers);
    dbzConnectorProps.setProperty("schema.history.internal.kafka.topic", schemaHistoryTopic);
    // Explicitly set KafkaSchemaHistory (though often default if above are set)
    dbzConnectorProps.setProperty(
        "schema.history.internal", "io.debezium.storage.kafka.history.KafkaSchemaHistory");

    // Convert Properties to Map<String, String>
    Map<String, String> connectorPropsMap =
        dbzConnectorProps.entrySet().stream()
            .collect(Collectors.toMap(e -> (String) e.getKey(), e -> (String) e.getValue()));

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> results =
        p.apply(
            DebeziumIO.<String>read()
                .withConnectorConfiguration(
                    DebeziumIO.ConnectorConfiguration.create()
                        .withConnectorClass(MySqlConnector.class)
                        .withHostName(host)
                        .withPort(port)
                        .withUsername(username)
                        .withPassword(password)
                        .withConnectionProperties(connectorPropsMap))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withMaxNumberOfRecords(30)
                .withMaxTimeToRun(Duration.ofMinutes(3).toMillis())
                .withCoder(StringUtf8Coder.of()));
    String expected =
        "{\"metadata\":{\"connector\":\"mysql\",\"version\":\"1.9.8.Final\",\"name\":\"dbserver1\","
            + "\"database\":\"inventory\",\"schema\":\"mysql-bin.000003\",\"table\":\"addresses\"},\"before\":null,"
            + "\"after\":{\"fields\":{\"zip\":\"76036\",\"city\":\"Euless\",\"street\":\"3183 Moore"
            + " Avenue\",\"id\":10,\"state\":\"Texas\",\"customer_id\":1001,"
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
