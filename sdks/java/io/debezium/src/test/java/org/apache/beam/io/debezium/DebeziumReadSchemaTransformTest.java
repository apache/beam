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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

@RunWith(Parameterized.class)
public class DebeziumReadSchemaTransformTest {
  private static final Logger LOG = LoggerFactory.getLogger(DebeziumReadSchemaTransformTest.class);

  private static final DockerImageName KAFKA_IMAGE =
      DockerImageName.parse("confluentinc/cp-kafka:7.6.0");

  @ClassRule
  public static final PostgreSQLContainer<?> POSTGRES_SQL_CONTAINER =
      new PostgreSQLContainer<>(
              DockerImageName.parse("quay.io/debezium/example-postgres:latest")
                  .asCompatibleSubstituteFor("postgres"))
          .withPassword("dbz")
          .withUsername("debezium")
          .withExposedPorts(5432)
          .withDatabaseName("inventory");

  @ClassRule
  public static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>(
              DockerImageName.parse("debezium/example-mysql:3.0.0.Final")
                  .asCompatibleSubstituteFor("mysql"))
          .withUsername("mysqluser")
          .withPassword("debezium")
          .withExposedPorts(3306)
          .withCommand("--character-set-server=utf8mb4", "--collation-server=utf8mb4_unicode_ci");

  @ClassRule public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(KAFKA_IMAGE);

  @BeforeClass
  public static void startContainers() throws Exception {
    Startables.deepStart(Stream.of(POSTGRES_SQL_CONTAINER, MY_SQL_CONTAINER, KAFKA_CONTAINER))
        .join();

    try (Connection conn =
            DriverManager.getConnection(
                MY_SQL_CONTAINER.getJdbcUrl(), "root", MY_SQL_CONTAINER.getPassword());
        Statement stmt = conn.createStatement()) {
      stmt.execute("GRANT REPLICATION CLIENT ON *.* TO 'mysqluser'@'%'");
      stmt.execute("GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'mysqluser'@'%'");
      stmt.execute("FLUSH PRIVILEGES");
      LOG.info("Granted privileges to mysqluser for MySQL.");
    } catch (Exception e) {
      LOG.error("Failed to grant privileges to mysqluser in MySQL container", e);
    }
  }

  @AfterClass
  public static void stopContainers() {
    if (KAFKA_CONTAINER != null && KAFKA_CONTAINER.isRunning()) KAFKA_CONTAINER.stop();
    if (MY_SQL_CONTAINER != null && MY_SQL_CONTAINER.isRunning()) MY_SQL_CONTAINER.stop();
    if (POSTGRES_SQL_CONTAINER != null && POSTGRES_SQL_CONTAINER.isRunning())
      POSTGRES_SQL_CONTAINER.stop();
  }

  @Parameterized.Parameters(name = "{index}: database={3}, user={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {
            POSTGRES_SQL_CONTAINER,
            "debezium",
            "dbz",
            "POSTGRES",
            5432,
            "inventory.customers" // schema.table for PostgreSQL
          },
          {
            MY_SQL_CONTAINER,
            "mysqluser",
            "debezium",
            "MYSQL",
            3306,
            "inventory.customers" // database.table for MySQL
          }
        });
  }

  @Parameterized.Parameter(0)
  public JdbcDatabaseContainer<?> databaseContainer;

  @Parameterized.Parameter(1)
  public String userName;

  @Parameterized.Parameter(2)
  public String password;

  @Parameterized.Parameter(3)
  public String database;

  @Parameterized.Parameter(4)
  public Integer port;

  @Parameterized.Parameter(5)
  public String tableName;

  private PTransform<PCollectionRowTuple, PCollectionRowTuple> makePtransform(
      String user,
      String password,
      String databaseType,
      Integer mappedPort,
      String host,
      String tableToRead) {

    String kafkaBootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
    String uniqueSuffix = UUID.randomUUID().toString().substring(0, 8);

    Properties connectorProps = new Properties();
    connectorProps.setProperty(
        "schema.history.internal.kafka.bootstrap.servers", kafkaBootstrapServers);
    connectorProps.setProperty(
        "schema.history.internal.kafka.topic",
        "schema-history-" + databaseType.toLowerCase() + "-" + uniqueSuffix);
    connectorProps.setProperty(
        "schema.history.internal", "io.debezium.storage.kafka.history.KafkaSchemaHistory");
    connectorProps.setProperty("database.connectionTimeout.ms", "30000");
    connectorProps.setProperty("connect.timeout.ms", "30000");

    if ("MYSQL".equals(databaseType)) {
      String mySqlServerId = String.valueOf(ThreadLocalRandom.current().nextInt(50000, 1000000));
      connectorProps.setProperty("database.server.id", mySqlServerId);
      connectorProps.setProperty("database.server.name", "test-mysql-server-" + uniqueSuffix);
      String dbName =
          tableToRead.contains(".")
              ? tableToRead.substring(0, tableToRead.indexOf('.'))
              : "inventory";
      connectorProps.setProperty("database.include.list", dbName);
      connectorProps.setProperty("snapshot.locking.mode", "none");
      connectorProps.setProperty("table.include.list", tableToRead);
    } else if ("POSTGRES".equals(databaseType)) {
      connectorProps.setProperty("plugin.name", "pgoutput");
      connectorProps.setProperty("database.server.name", "test-pg-server-" + uniqueSuffix);
      connectorProps.setProperty("database.dbname", "inventory");
      String slotName = "beam_pg_" + uniqueSuffix.replaceAll("[^a-zA-Z0-9_]", "");
      connectorProps.setProperty(
          "slot.name", slotName.substring(0, Math.min(slotName.length(), 63)));
      String[] parts = tableToRead.split("\\.", 2);
      if (parts.length == 2) {
        connectorProps.setProperty("schema.include.list", parts[0]);
        connectorProps.setProperty("table.include.list", tableToRead);
      } else {
        connectorProps.setProperty("schema.include.list", "inventory");
        connectorProps.setProperty("table.include.list", "inventory." + tableToRead);
        LOG.warn(
            "PostgreSQL table name '{}' was not fully qualified (schema.table). Assuming schema 'inventory'.",
            tableToRead);
      }
      connectorProps.setProperty("publication.autocreate.mode", "filtered");
    }

    List<String> debeziumConnectionPropertiesList = new ArrayList<>();
    for (Map.Entry<Object, Object> entry : connectorProps.entrySet()) {
      debeziumConnectionPropertiesList.add(entry.getKey() + "=" + entry.getValue());
    }

    DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration.Builder
        configBuilder =
            DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration.builder()
                .setDatabase(databaseType)
                .setPassword(password)
                .setUsername(user)
                .setHost(host)
                .setTable(tableToRead)
                .setPort(mappedPort);

    if (!debeziumConnectionPropertiesList.isEmpty()) {
      configBuilder.setDebeziumConnectionProperties(debeziumConnectionPropertiesList);
    }

    return new DebeziumReadSchemaTransformProvider(true, 10, 60000L).from(configBuilder.build());
  }

  @Test
  public void testNoProblem() {
    LOG.info(
        "Running testNoProblem for {} with user {} on port {} for table {}",
        database,
        userName,
        databaseContainer.getMappedPort(port),
        tableName);
    Pipeline readPipeline = Pipeline.create();
    PCollection<Row> result = null;
    try {
      result =
          PCollectionRowTuple.empty(readPipeline)
              .apply(
                  "ReadDebeziumSchema_"
                      + database
                      + "_"
                      + UUID.randomUUID().toString().substring(0, 4),
                  makePtransform(
                      userName,
                      password,
                      database,
                      databaseContainer.getMappedPort(port),
                      databaseContainer.getHost(),
                      tableName))
              .get("output");
    } catch (Exception e) {
      LOG.error("Error applying/expanding transform in testNoProblem for {}: ", database, e);
      throw new RuntimeException(
          "Error applying/expanding transform in testNoProblem for " + database, e);
    }

    assertThat(
        "Schema fields for " + database,
        result.getSchema().getFields().stream()
            .map(Schema.Field::getName)
            .collect(Collectors.toList()),
        Matchers.containsInAnyOrder(
            "before", "after", "source", "op", "ts_ms", "ts_us", "ts_ns", "transaction"));
    try {
      readPipeline.run().waitUntilFinish();
    } catch (Exception e) {
      LOG.error("Pipeline run failed in testNoProblem for database: {}", database, e);
      throw new RuntimeException(
          "Pipeline run failed in testNoProblem for database: " + database, e);
    }
  }

  private boolean checkExceptionCauseChainForKeywords(
      Throwable throwable, String testNameInfo, List<String> orKeywords, String... andKeywords) {
    Throwable current = throwable;
    StringBuilder fullCauseChainForLog = new StringBuilder();
    int level = 0;

    while (current != null) {
      String message = current.getMessage() != null ? current.getMessage().toLowerCase() : "";
      String causeEntry = "  ".repeat(level) + current.getClass().getName() + ": " + message + "\n";
      fullCauseChainForLog.append(causeEntry);
      LOG.debug("{} - Checking cause: {}", testNameInfo, causeEntry.trim());

      boolean orConditionMet = orKeywords == null || orKeywords.isEmpty();
      if (orKeywords != null && !orKeywords.isEmpty()) {
        for (String orKeyword : orKeywords) {
          if (message.contains(orKeyword.toLowerCase())) {
            orConditionMet = true;
            break;
          }
        }
      }

      boolean andConditionMet = true;
      if (andKeywords != null && andKeywords.length > 0) {
        for (String andKeyword : andKeywords) {
          if (!message.contains(andKeyword.toLowerCase())) {
            andConditionMet = false;
            break;
          }
        }
      }

      if (orConditionMet && andConditionMet) {
        LOG.info(
            "{} - Found required keywords {} and OR keywords {} in message: {}",
            testNameInfo,
            Arrays.toString(andKeywords),
            orKeywords,
            message);
        return true;
      }

      if (current == current.getCause()) {
        break;
      }
      current = current.getCause();
      level++;
    }
    LOG.warn(
        "{}: Expected OR keywords '{}' and AND keywords '{}' not found in any single cause message of the exception chain. Full chain:\n{}",
        testNameInfo,
        orKeywords,
        Arrays.toString(andKeywords),
        fullCauseChainForLog.toString());
    return false;
  }

  @Test
  public void testWrongUser() {
    String testInfo = "testWrongUser (" + database + ")";
    LOG.info("Running {} on port {}", testInfo, databaseContainer.getMappedPort(port));

    final Pipeline readPipeline = Pipeline.create();
    final PTransform<PCollectionRowTuple, PCollectionRowTuple> transform =
        makePtransform(
            "wrongUser",
            password,
            database,
            databaseContainer.getMappedPort(port),
            databaseContainer.getHost(),
            tableName);

    Exception ex =
        assertThrows(
            "Expected pipeline to fail due to wrong user for " + database,
            Exception.class,
            () -> {
              // This .apply() call is the single "action" statement expected to throw
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      "ReadWithWrongUser_"
                          + database
                          + "_"
                          + UUID.randomUUID().toString().substring(0, 4),
                      transform);
            });

    assertTrue(
        String.format(
            "%s: Expected an authentication-related error for 'wrongUser'. Got: %s",
            testInfo, ex.toString()),
        checkExceptionCauseChainForKeywords(
            ex,
            testInfo,
            Arrays.asList("authentication failed", "access denied for user"),
            "wronguser"));
  }

  @Test
  public void testWrongPassword() {
    String testInfo = "testWrongPassword (" + database + ")";
    LOG.info("Running {} on port {}", testInfo, databaseContainer.getMappedPort(port));

    final Pipeline readPipeline = Pipeline.create();
    final PTransform<PCollectionRowTuple, PCollectionRowTuple> transform =
        makePtransform(
            userName,
            "wrongPassword",
            database,
            databaseContainer.getMappedPort(port),
            databaseContainer.getHost(),
            tableName);

    Exception ex =
        assertThrows(
            "Expected pipeline to fail due to wrong password for " + database,
            Exception.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      "ReadWithWrongPassword_"
                          + database
                          + "_"
                          + UUID.randomUUID().toString().substring(0, 4),
                      transform);
            });

    assertTrue(
        String.format(
            "%s: Expected an authentication-related error for user %s. Got: %s",
            testInfo, userName, ex.toString()),
        checkExceptionCauseChainForKeywords(
            ex,
            testInfo,
            Arrays.asList("authentication failed", "access denied for user"),
            userName));
  }

  @Test
  public void testWrongPort() {
    String testInfo = "testWrongPort (" + database + ")";
    LOG.info("Running {} ", testInfo);

    final Pipeline readPipeline = Pipeline.create();
    final PTransform<PCollectionRowTuple, PCollectionRowTuple> transform =
        makePtransform(
            userName,
            password,
            database,
            12345, // Incorrect port
            databaseContainer.getHost(),
            tableName);

    Exception ex =
        assertThrows(
            "Expected pipeline to fail due to wrong port for " + database,
            Exception.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      "ReadWithWrongPort_"
                          + database
                          + "_"
                          + UUID.randomUUID().toString().substring(0, 4),
                      transform);
            });

    assertTrue(
        String.format(
            "%s: Expected connection refused or timeout error. Got: %s", testInfo, ex.toString()),
        checkExceptionCauseChainForKeywords(
            ex,
            testInfo,
            Arrays.asList("connection refused", "connect timed out", "connection timed out")));
  }

  @Test
  public void testWrongHost() {
    String testInfo = "testWrongHost (" + database + ")";
    LOG.info("Running {} ", testInfo);

    final Pipeline readPipeline = Pipeline.create();
    final PTransform<PCollectionRowTuple, PCollectionRowTuple> transform =
        makePtransform(
            userName,
            password,
            database,
            databaseContainer.getMappedPort(port),
            "172.168.254.253", // Non-existent, non-routable IP
            tableName);

    Exception ex =
        assertThrows(
            "Expected pipeline to fail due to wrong host for " + database,
            Exception.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      "ReadWithWrongHost_"
                          + database
                          + "_"
                          + UUID.randomUUID().toString().substring(0, 4),
                      transform);
            });

    assertTrue(
        String.format("%s: Expected connection/host error. Got: %s", testInfo, ex.toString()),
        checkExceptionCauseChainForKeywords(
            ex,
            testInfo,
            Arrays.asList(
                "timed out",
                "timeout",
                "unknownhostexception",
                "could not connect",
                "unreachable",
                "connection refused")));
  }
}
