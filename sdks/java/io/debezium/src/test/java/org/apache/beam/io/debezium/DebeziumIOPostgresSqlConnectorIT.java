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

import static org.apache.beam.sdk.testing.SerializableMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;

import io.debezium.connector.postgresql.PostgresConnector;
import javax.sql.DataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
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
              DockerImageName.parse("debezium/example-postgres:latest")
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
    dataSource.setServerName(POSTGRES_SQL_CONTAINER.getContainerIpAddress());
    dataSource.setPortNumber(POSTGRES_SQL_CONTAINER.getMappedPort(5432));
    dataSource.setUser("debezium");
    dataSource.setPassword("dbz");
    return dataSource;
  }

  @Test
  public void testDebeziumSchemaTransformPostgresRead() {
    long WRITE_SIZE = 1000L;
    POSTGRES_SQL_CONTAINER.start();

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline writePipeline = Pipeline.create(options);
    writePipeline
        .apply(GenerateSequence.from(0).to(WRITE_SIZE).withRate(10, Duration.standardSeconds(1)))
        .apply(
            MapElements.into(TypeDescriptors.rows())
                .via(
                    num ->
                        Row.withSchema(TABLE_SCHEMA)
                            .withFieldValue("id", Long.valueOf(num).intValue())
                            .withFieldValue("first_name", Long.toString(num))
                            .withFieldValue("last_name", Long.toString(WRITE_SIZE - num))
                            .withFieldValue("email", Long.toString(num) + "@beamail.com")
                            //                            .withFieldValue("name",
                            // Long.toString(num))
                            //                            .withFieldValue("age", num % 100)
                            //                            .withFieldValue("temperature", num /
                            // 100.0)
                            //                            .withFieldValue("distance", num * 1000.0)
                            //                            .withFieldValue("birthYear", num)
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
                new DebeziumReadSchemaTransformProvider(true, 1004)
                    .from(
                        DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration
                            .builder()
                            .setDatabase("POSTGRES")
                            .setPassword("dbz")
                            .setUsername("debezium")
                            .setHost("localhost")
                            .setTable("inventory.customers")
                            .setPort(POSTGRES_SQL_CONTAINER.getMappedPort(5432))
                            .build())
                    .buildTransform())
            .get("output");

    PAssert.that(result)
        .satisfies(rows -> {
          assert Lists.newArrayList(rows).size() == 1004;
          return null;
        });

    PipelineResult writeResult = writePipeline.run();
    readPipeline.run().waitUntilFinish();
    writeResult.waitUntilFinish();
  }

  /**
   * Debezium - PostgresSql connector Test.
   *
   * <p>Tests that connector can actually connect to the database
   */
  @Test
  public void testDebeziumIOPostgresSql() {
    POSTGRES_SQL_CONTAINER.start();

    String host = POSTGRES_SQL_CONTAINER.getContainerIpAddress();
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
                        .withConnectionProperty("database.include.list", "inventory")
                        .withConnectionProperty("include.schema.changes", "false"))
                .withFormatFunction(new SourceRecordJson.SourceRecordJsonMapper())
                .withMaxNumberOfRecords(30)
                .withCoder(StringUtf8Coder.of()));
    String expected =
        "{\"metadata\":{\"connector\":\"postgresql\",\"version\":\"1.3.1.Final\",\"name\":\"dbserver1\","
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
}
