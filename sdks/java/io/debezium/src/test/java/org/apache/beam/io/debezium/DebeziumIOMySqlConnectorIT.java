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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.debezium.connector.mysql.MySqlConnector;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.JsonElement;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.JsonObject;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.gson.JsonParser;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
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
  private static final DockerImageName KAFKA_IMAGE =
      DockerImageName.parse("confluentinc/cp-kafka:7.6.0");

  @ClassRule public static Network network = Network.newNetwork();

  @ClassRule
  public static final MySQLContainer<?> MY_SQL_CONTAINER =
      new MySQLContainer<>(
              DockerImageName.parse("quay.io/debezium/example-mysql:3.5.2.Final")
                  .asCompatibleSubstituteFor("mysql"))
          .withPassword("debezium")
          .withUsername("mysqluser")
          .withNetwork(network)
          .withNetworkAliases("mysql")
          .withExposedPorts(3306)
          .waitingFor(
              new HttpWaitStrategy()
                  .forPort(3306)
                  .forStatusCodeMatching(response -> response == 200)
                  .withStartupTimeout(Duration.ofMinutes(2)));

  @ClassRule
  public static final KafkaContainer KAFKA_CONTAINER =
      new KafkaContainer(KAFKA_IMAGE)
          .withNetwork(network)
          .withNetworkAliases("kafka")
          .dependsOn(MY_SQL_CONTAINER);

  public static DataSource getMysqlDatasource(Void unused) {
    HikariConfig hikariConfig = new HikariConfig();

    String jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%d/inventory?allowPublicKeyRetrieval=true",
            MY_SQL_CONTAINER.getHost(), MY_SQL_CONTAINER.getMappedPort(3306));
    LOG.info("Hikari DataSource JDBC URL for mysqluser: {}", jdbcUrl);

    hikariConfig.setJdbcUrl(jdbcUrl);
    hikariConfig.setUsername(MY_SQL_CONTAINER.getUsername());
    hikariConfig.setPassword(MY_SQL_CONTAINER.getPassword());
    hikariConfig.addDataSourceProperty("allowPublicKeyRetrieval", "true");
    hikariConfig.setDriverClassName(MY_SQL_CONTAINER.getDriverClassName());
    return new HikariDataSource(hikariConfig);
  }

  private void monitorEssentialMetrics() throws SQLException {
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
          throw new IllegalArgumentException(
              "Illegal Argument Exception in monitorEssentialMetrics.");
        }
      }
    } catch (SQLException ex) {
      LOG.error("SQL error in monitoring thread. Shutting down.", ex);
      throw ex;
    } catch (InterruptedException ex) {
      LOG.info("Monitoring thread interrupted. Shutting down.");
      Thread.currentThread().interrupt();
    }
  }

  @Test
  public void testDebeziumSchemaTransformMysqlRead() throws InterruptedException {
    long writeSize = 500L;
    long testTime = writeSize * 200L;

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
                                    "database.server.id=1849055",
                                    "schema.history.internal.kafka.bootstrap.servers="
                                        + KAFKA_CONTAINER.getBootstrapServers(),
                                    "schema.history.internal.kafka.topic=schema-history-mysql-transform-"
                                        + System.nanoTime(),
                                    "schema.history.internal=io.debezium.storage.kafka.history.KafkaSchemaHistory",
                                    "schema.history.internal.store.only.captured.tables.ddl=false",
                                    "table.include.list=inventory.customers",
                                    "snapshot.mode=initial_only"))
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
    Thread monitorThread =
        new Thread(
            () -> {
              try {
                monitorEssentialMetrics();
              } catch (SQLException e) {
                e.printStackTrace();
                fail("Failed because of SQLException in monitorEssentialMetrics!");
              }
            });
    monitorThread.start();
    writeThread.start();

    writeThread.join();
    LOG.info("Write thread for SchemaTransform test joined.");

    LOG.info("Starting read pipeline for SchemaTransform test...");
    readPipeline.run().waitUntilFinish();
    LOG.info("Read pipeline for SchemaTransform test finished.");

    monitorThread.interrupt();
    monitorThread.join();
    LOG.info("Monitor thread for SchemaTransform test joined.");
  }

  /**
   * Debezium - MySQL connector Test.
   *
   * <p>Tests that connector can actually connect to the database
   */
  @Test
  public void testDebeziumIOMySql() {

    String kafkaBootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
    String schemaHistoryTopic = "mysql-schema-history-io-" + System.nanoTime();

    String host = MY_SQL_CONTAINER.getHost();
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
                        .withConnectionProperty("database.include.list", "inventory")
                        .withConnectionProperty("include.schema.changes", "false")
                        .withConnectionProperty(
                            "schema.history.internal.kafka.bootstrap.servers",
                            kafkaBootstrapServers)
                        .withConnectionProperty(
                            "schema.history.internal.kafka.topic", schemaHistoryTopic)
                        .withConnectionProperty(
                            "schema.history.internal",
                            "io.debezium.storage.kafka.history.KafkaSchemaHistory")
                        .withConnectionProperty(
                            "schema.history.internal.store.only.captured.tables.ddl", "true"))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withMaxNumberOfRecords(30)
                .withCoder(StringUtf8Coder.of()));
    String expected =
        "{\"metadata\":{\"connector\":\"mysql\",\"version\":\"3.5.2.Final\",\"name\":\"beam-debezium-connector\","
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
  }

  /**
   * Debezium - MySQL connector Test with {@link DebeziumIO.Read#withFullMetadata(boolean)}.
   *
   * <p>Tests that the JSON metadata carries {@code op} and the MySQL source fields, flattened.
   */
  @Test
  public void testDebeziumIOMySqlFullMetadata() {

    String kafkaBootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
    String schemaHistoryTopic = "mysql-schema-history-full-metadata-" + System.nanoTime();

    String host = MY_SQL_CONTAINER.getHost();
    String port = MY_SQL_CONTAINER.getMappedPort(3306).toString();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> results =
        p.apply(
            DebeziumIO.readAsJson()
                .withConnectorConfiguration(
                    DebeziumIO.ConnectorConfiguration.create()
                        .withUsername("debezium")
                        .withPassword("dbz")
                        .withConnectorClass(MySqlConnector.class)
                        .withHostName(host)
                        .withPort(port)
                        .withConnectionProperty("database.server.id", "184055")
                        .withConnectionProperty("database.include.list", "inventory")
                        .withConnectionProperty("include.schema.changes", "false")
                        .withConnectionProperty(
                            "schema.history.internal.kafka.bootstrap.servers",
                            kafkaBootstrapServers)
                        .withConnectionProperty(
                            "schema.history.internal.kafka.topic", schemaHistoryTopic)
                        .withConnectionProperty(
                            "schema.history.internal",
                            "io.debezium.storage.kafka.history.KafkaSchemaHistory")
                        .withConnectionProperty(
                            "schema.history.internal.store.only.captured.tables.ddl", "true"))
                .withFullMetadata(true)
                .withMaxNumberOfRecords(10));

    PAssert.that(results)
        .satisfies(
            (Iterable<String> res) -> {
              List<String> records = Lists.newArrayList(res);
              assertThat(records, not(empty()));
              for (String record : records) {
                JsonObject metadata =
                    JsonParser.parseString(record).getAsJsonObject().getAsJsonObject("metadata");
                // Snapshot rows are reads.
                assertThat(metadata.get("op").getAsString(), equalTo("r"));
                assertTrue(metadata.get("pos").getAsJsonPrimitive().isNumber());
                assertTrue(metadata.get("row").getAsJsonPrimitive().isNumber());
                assertTrue(metadata.get("ts_ms").getAsJsonPrimitive().isNumber());
                // MySQL keeps exposing the binlog file under "schema" for compatibility.
                assertThat(
                    metadata.get("file").getAsString(),
                    equalTo(metadata.get("schema").getAsString()));
                assertFalse(metadata.has("source"));
              }
              return null;
            });

    p.run().waitUntilFinish();
  }

  /**
   * Debezium - MySQL connector Test with {@link DebeziumIO.Read#withSchemaChangeEvents(boolean)}.
   *
   * <p>Tests the schema change events of a schema-only snapshot and of a live {@code ALTER TABLE}
   * read from the binlog. The snapshot's {@code CREATE} event for {@code inventory.customers}
   * carries the table's columns, each with every field of Debezium's column schema. The {@code
   * ALTER} runs on a table of its own, so {@code inventory.customers} is left untouched.
   */
  @Test
  public void testDebeziumIOMySqlSchemaChangeEvents() throws Exception {

    String kafkaBootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
    String schemaHistoryTopic = "mysql-schema-history-schema-change-" + System.nanoTime();

    String host = MY_SQL_CONTAINER.getHost();
    String port = MY_SQL_CONTAINER.getMappedPort(3306).toString();

    String alteredTable = "schema_change_events";
    String alterDdl = "ALTER TABLE " + alteredTable + " ADD COLUMN note VARCHAR(255)";
    executeSql("CREATE TABLE " + alteredTable + " (id INT PRIMARY KEY)");

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> results =
        p.apply(
            DebeziumIO.readAsJson()
                .withConnectorConfiguration(
                    DebeziumIO.ConnectorConfiguration.create()
                        .withUsername("debezium")
                        .withPassword("dbz")
                        .withConnectorClass(MySqlConnector.class)
                        .withHostName(host)
                        .withPort(port)
                        .withConnectionProperty("database.server.id", "184056")
                        .withConnectionProperty("database.include.list", "inventory")
                        .withConnectionProperty(
                            "table.include.list", "inventory.customers,inventory." + alteredTable)
                        // Snapshot the schema only: a consistent snapshot read of a table that is
                        // altered meanwhile would fail.
                        .withConnectionProperty("snapshot.mode", "no_data")
                        .withConnectionProperty("include.schema.changes", "true")
                        .withConnectionProperty(
                            "schema.history.internal.kafka.bootstrap.servers",
                            kafkaBootstrapServers)
                        .withConnectionProperty(
                            "schema.history.internal.kafka.topic", schemaHistoryTopic)
                        .withConnectionProperty(
                            "schema.history.internal",
                            "io.debezium.storage.kafka.history.KafkaSchemaHistory")
                        .withConnectionProperty(
                            "schema.history.internal.store.only.captured.tables.ddl", "true"))
                .withFullMetadata(true)
                .withSchemaChangeEvents(true)
                // The snapshot emits 8 schema change events for the 2 captured tables: SET
                // character_set_server, DROP TABLE IF EXISTS for each table, DROP DATABASE,
                // CREATE DATABASE, USE and CREATE TABLE for each table. The ALTER is the 9th.
                .withMaxNumberOfRecords(9)
                .withMaxTimeToRun(Duration.ofMinutes(2).toMillis()));

    // Every field of Debezium's column schema, in schema order, nulls included.
    String expectedFirstNameColumn =
        "{\"name\":\"first_name\",\"jdbcType\":12,\"nativeType\":null,\"typeName\":\"VARCHAR\","
            + "\"typeExpression\":\"VARCHAR\",\"charsetName\":\"utf8mb4\",\"length\":255,"
            + "\"scale\":null,\"position\":2,\"optional\":false,\"autoIncremented\":false,"
            + "\"generated\":false,\"comment\":null,\"defaultValueExpression\":null,"
            + "\"enumValues\":null}";
    String customersId = "\"inventory\".\"customers\"";
    String alteredId = "\"inventory\".\"" + alteredTable + "\"";

    PAssert.that(results)
        .satisfies(
            (Iterable<String> res) -> {
              JsonObject customersEvent = null;
              JsonObject customersTable = null;
              JsonObject alterEvent = null;
              for (String record : res) {
                JsonObject event = JsonParser.parseString(record).getAsJsonObject();
                if (!event.has("tableChanges") || event.get("tableChanges").isJsonNull()) {
                  continue; // Not a schema change event.
                }
                for (JsonElement change : event.getAsJsonArray("tableChanges")) {
                  JsonObject tableChange = change.getAsJsonObject();
                  String type = tableChange.get("type").getAsString();
                  String id = tableChange.get("id").getAsString();
                  if (type.equals("CREATE") && id.equals(customersId)) {
                    customersEvent = event;
                    customersTable = tableChange.getAsJsonObject("table");
                  } else if (type.equals("ALTER") && id.equals(alteredId)) {
                    alterEvent = event;
                  }
                }
              }

              // The snapshot's CREATE event for the customers table.
              assertNotNull("No snapshot CREATE event for " + customersId, customersEvent);
              assertNotNull("No table in the CREATE event for " + customersId, customersTable);
              assertThat(
                  customersEvent.getAsJsonObject("metadata").get("snapshot").getAsString(),
                  equalTo("true"));
              assertThat(
                  customersTable.get("primaryKeyColumnNames").toString(), equalTo("[\"id\"]"));
              // Each column's JSON, keyed by column name, in column order.
              Map<String, String> columns = new LinkedHashMap<>();
              for (JsonElement column : customersTable.getAsJsonArray("columns")) {
                columns.put(column.getAsJsonObject().get("name").getAsString(), column.toString());
              }
              assertThat(columns.keySet(), contains("id", "first_name", "last_name", "email"));
              assertThat(columns.get("first_name"), equalTo(expectedFirstNameColumn));

              // The live ALTER, read from the binlog. It keeps metadata, before and after, so the
              // full metadata's snapshot field tells it apart from the snapshot's events.
              assertNotNull("No ALTER event for " + alteredId, alterEvent);
              assertThat(alterEvent.get("ddl").getAsString(), equalTo(alterDdl));
              assertThat(alterEvent.get("databaseName").getAsString(), equalTo("inventory"));
              JsonObject alterMetadata = alterEvent.getAsJsonObject("metadata");
              assertThat(alterMetadata.get("connector").getAsString(), equalTo("mysql"));
              assertThat(alterMetadata.get("table").getAsString(), equalTo(alteredTable));
              assertThat(alterMetadata.get("snapshot").getAsString(), equalTo("false"));
              assertTrue(alterEvent.get("before").isJsonNull());
              assertTrue(alterEvent.get("after").isJsonNull());
              List<String> alteredColumns = Lists.newArrayList();
              for (JsonElement column :
                  alterEvent
                      .getAsJsonArray("tableChanges")
                      .get(0)
                      .getAsJsonObject()
                      .getAsJsonObject("table")
                      .getAsJsonArray("columns")) {
                alteredColumns.add(column.getAsJsonObject().get("name").getAsString());
              }
              assertThat(alteredColumns, contains("id", "note"));
              return null;
            });

    // Alter the table once Debezium has snapshotted the schema, so the ALTER is read from the
    // binlog rather than folded into the snapshot's CREATE.
    CompletableFuture<Void> alter =
        CompletableFuture.runAsync(
            () -> {
              try {
                awaitSchemaHistory(kafkaBootstrapServers, schemaHistoryTopic);
                executeSql(alterDdl);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });

    p.run().waitUntilFinish();
    alter.get(1, TimeUnit.MINUTES);
  }

  /** Runs a SQL statement against the {@code inventory} database. */
  private static void executeSql(String sql) throws SQLException {
    try (HikariDataSource dataSource = (HikariDataSource) getMysqlDatasource(null);
        Connection connection = dataSource.getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  /**
   * Waits until Debezium has written the snapshot's schema to the schema history topic. The
   * snapshot reads its binlog position before that, so any later DDL is read from the binlog.
   */
  private static void awaitSchemaHistory(String bootstrapServers, String topic)
      throws InterruptedException {
    Map<String, Object> config = new HashMap<>();
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    // Leave creating the topic to Debezium, which configures it for schema history.
    config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
    TopicPartition partition = new TopicPartition(topic, 0);
    Instant deadline = Instant.now().plus(Duration.ofMinutes(1));
    try (KafkaConsumer<byte[], byte[]> consumer =
        new KafkaConsumer<>(config, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
      while (!consumer.listTopics().containsKey(topic)
          || consumer.endOffsets(Collections.singleton(partition)).get(partition) == 0L) {
        if (Instant.now().isAfter(deadline)) {
          throw new IllegalStateException("No schema history written to " + topic);
        }
        Thread.sleep(500);
      }
    }
  }
}
