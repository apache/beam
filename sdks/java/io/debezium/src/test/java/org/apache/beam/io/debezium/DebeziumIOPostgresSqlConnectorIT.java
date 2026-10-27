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
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

import io.debezium.connector.postgresql.PostgresConnector;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(JUnit4.class)
public class DebeziumIOPostgresSqlConnectorIT {
  /**
   * Debezium - PostgresSqlContainer
   *
   * <p>Creates a docker container using the image used by the debezium tutorial.
   */
  @ClassRule
  public static final PostgreSQLContainer<?> POSTGRES_SQL_CONTAINER =
      new PostgreSQLContainer<>(
              DockerImageName.parse("quay.io/debezium/example-postgres:3.5.2.Final")
                  .asCompatibleSubstituteFor("postgres"))
          .withPassword("dbz")
          .withUsername("debezium")
          .withExposedPorts(5432)
          .withDatabaseName("inventory");

  static final Schema TABLE_SCHEMA =
      Schema.builder()
          .addInt32Field("id")
          .addStringField("first_name")
          .addStringField("last_name")
          .addStringField("email")
          .build();

  static DataSource getPostgresDatasource() {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setDatabaseName("inventory");
    String[] serverNames = new String[] {POSTGRES_SQL_CONTAINER.getHost()};
    dataSource.setServerNames(serverNames);
    int[] ports = new int[] {POSTGRES_SQL_CONTAINER.getMappedPort(5432)};
    dataSource.setPortNumbers(ports);
    dataSource.setUser("debezium");
    dataSource.setPassword("dbz");
    return dataSource;
  }

  @Test
  public void testDebeziumSchemaTransformPostgresRead() throws InterruptedException {
    long writeSize = 500L;
    long testTime = writeSize * 200L;
    POSTGRES_SQL_CONTAINER.start();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline writePipeline = Pipeline.create(options);
    writePipeline
        .apply(GenerateSequence.from(0).to(writeSize).withRate(10, Duration.standardSeconds(1)))
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
                .withTable("inventory.inventory.customers")
                .withDataSourceConfiguration(
                    JdbcIO.DataSourceConfiguration.create(getPostgresDatasource())));

    Pipeline readPipeline = Pipeline.create(options);
    PCollection<Row> result =
        PCollectionRowTuple.empty(readPipeline)
            .apply(
                new DebeziumReadSchemaTransformProvider(
                        true, Long.valueOf(writeSize).intValue() + 4, testTime)
                    .from(
                        DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration
                            .builder()
                            .setDatabase("POSTGRES")
                            .setPassword("dbz")
                            .setUsername("debezium")
                            .setHost("localhost")
                            .setTable("inventory.customers")
                            .setPort(POSTGRES_SQL_CONTAINER.getMappedPort(5432))
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
    writeThread.start();
    readPipeline.run().waitUntilFinish();
    writeThread.join();
  }

  /**
   * Debezium - PostgresSql connector Test.
   *
   * <p>Tests that connector can actually connect to the database
   */
  @Test
  public void testDebeziumIOPostgresSql() {
    POSTGRES_SQL_CONTAINER.start();

    String host = POSTGRES_SQL_CONTAINER.getHost();
    String port = POSTGRES_SQL_CONTAINER.getMappedPort(5432).toString();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> results =
        p.apply(
            DebeziumIO.<String>read()
                .withConnectorConfiguration(
                    DebeziumIO.ConnectorConfiguration.create()
                        .withUsername("debezium")
                        .withPassword("dbz")
                        .withConnectorClass(PostgresConnector.class)
                        .withHostName(host)
                        .withPort(port)
                        .withConnectionProperty("database.dbname", "inventory")
                        .withConnectionProperty("database.server.name", "dbserver1")
                        .withConnectionProperty("include.schema.changes", "false"))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withMaxNumberOfRecords(30)
                .withCoder(StringUtf8Coder.of()));
    String expected =
        "{\"metadata\":{\"connector\":\"postgresql\",\"version\":\"3.5.2.Final\",\"name\":\"beam-debezium-connector\","
            + "\"database\":\"inventory\",\"schema\":\"inventory\",\"table\":\"customers\"},\"before\":null,"
            + "\"after\":{\"fields\":{\"last_name\":\"Thomas\",\"id\":1001,\"first_name\":\"Sally\","
            + "\"email\":\"sally.thomas@acme.com\"}}}";

    PAssert.that(results)
        .satisfies(
            (Iterable<String> res) -> {
              assertThat(res, hasItem(expected));
              return null;
            });

    p.run().waitUntilFinish();
    POSTGRES_SQL_CONTAINER.stop();
  }

  /**
   * Debezium - PostgresSql connector Test with transaction metadata.
   *
   * <p>Tests that the transaction BEGIN/END events emitted with {@code
   * provide.transaction.metadata=true}, which have no {@code source} block, are mapped to JSON
   * alongside the data change events of the transaction, and that with full metadata each change
   * event carries its {@code transaction} block.
   */
  @Test
  public void testShouldProcessTransactionMetadata() throws SQLException {
    POSTGRES_SQL_CONTAINER.start();

    try (Connection connection = getPostgresDatasource().getConnection();
        Statement statement = connection.createStatement()) {
      // Create the slot first so it captures the commit below (no_data skips the snapshot).
      statement.execute(
          "SELECT pg_create_logical_replication_slot('beam_transaction_metadata', 'decoderbufs')");
      connection.setAutoCommit(false);
      statement.executeUpdate(
          "INSERT INTO inventory.customers (id, first_name, last_name, email) "
              + "VALUES (5001, 'Ann', 'One', 'ann.one@example.com')");
      statement.executeUpdate(
          "INSERT INTO inventory.customers (id, first_name, last_name, email) "
              + "VALUES (5002, 'Bob', 'Two', 'bob.two@example.com')");
      statement.executeUpdate("UPDATE inventory.customers SET last_name = 'Uno' WHERE id = 5001");
      connection.commit();
    }

    String host = POSTGRES_SQL_CONTAINER.getHost();
    String port = POSTGRES_SQL_CONTAINER.getMappedPort(5432).toString();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);
    PCollection<String> results =
        p.apply(
            DebeziumIO.readAsJson()
                .withConnectorConfiguration(
                    DebeziumIO.ConnectorConfiguration.create()
                        .withUsername("debezium")
                        .withPassword("dbz")
                        .withConnectorClass(PostgresConnector.class)
                        .withHostName(host)
                        .withPort(port)
                        .withConnectionProperty("database.dbname", "inventory")
                        .withConnectionProperty("table.include.list", "inventory.customers")
                        .withConnectionProperty("snapshot.mode", "no_data")
                        .withConnectionProperty("provide.transaction.metadata", "true")
                        .withConnectionProperty("slot.name", "beam_transaction_metadata"))
                .withFullMetadata(true)
                .withMaxNumberOfRecords(5)
                .withMaxTimeToRun(120_000L));

    PAssert.that(results)
        .satisfies(
            (Iterable<String> res) -> {
              assertThat(Lists.newArrayList(res).size(), equalTo(5));
              assertThat(res, hasItem(containsString("{\"status\":\"BEGIN\",")));
              assertThat(res, hasItem(containsString("\"email\":\"ann.one@example.com\"")));
              assertThat(res, hasItem(containsString("\"email\":\"bob.two@example.com\"")));
              assertThat(
                  res,
                  hasItem(
                      allOf(
                          containsString("\"last_name\":\"Uno\""),
                          containsString("\"transaction\":{\"id\":\""),
                          containsString("\"total_order\":3,\"data_collection_order\":3}"))));
              assertThat(
                  res,
                  hasItem(
                      containsString(
                          "\"event_count\":3,\"data_collections\":"
                              + "[{\"data_collection\":\"inventory.customers\",\"event_count\":3}]")));
              return null;
            });

    p.run().waitUntilFinish();
    POSTGRES_SQL_CONTAINER.stop();
  }
}
