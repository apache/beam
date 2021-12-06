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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.values.PCollection;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
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
