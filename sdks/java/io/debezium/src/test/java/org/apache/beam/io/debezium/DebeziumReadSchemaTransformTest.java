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

import io.debezium.DebeziumException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@RunWith(Parameterized.class)
public class DebeziumReadSchemaTransformTest {

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
          .withUsername("mysqluser") // User 'mysqluser'
          .withPassword("debezium") // Password for 'mysqluser'
          .withExposedPorts(3306);

  @ClassRule public static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(KAFKA_IMAGE);

  @Parameterized.Parameters(name = "{index}: database={3}, user={1}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          // Corrected MySQL credentials to match container definition
          {
            POSTGRES_SQL_CONTAINER,
            "debezium",
            "dbz",
            "POSTGRES",
            5432,
            "inventory.inventory.customers"
          }, // PG table name
          {
            MY_SQL_CONTAINER, "mysqluser", "debezium", "MYSQL", 3306, "inventory.customers"
          } // MySQL table name
        });
  }

  @Parameterized.Parameter(0)
  public Container<?> databaseContainer;

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
    String schemaHistoryTopic =
        "schema-history-" + databaseType.toLowerCase() + "-" + System.nanoTime();

    Properties connectorProps = new Properties();
    connectorProps.setProperty(
        "schema.history.internal.kafka.bootstrap.servers", kafkaBootstrapServers);
    connectorProps.setProperty("schema.history.internal.kafka.topic", schemaHistoryTopic);
    connectorProps.setProperty(
        "schema.history.internal", "io.debezium.storage.kafka.history.KafkaSchemaHistory");

    
    if ("MYSQL".equals(databaseType)) {
      connectorProps.setProperty("database.server.id", "19001");
      connectorProps.setProperty("database.server.name", "test-mysql-server-transform");
    } else if ("POSTGRES".equals(databaseType)) {
      connectorProps.setProperty("plugin.name", "pgoutput");
      connectorProps.setProperty("database.server.name", "test-pg-server-transform");
      connectorProps.setProperty("database.dbname", "inventory");
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

    return new DebeziumReadSchemaTransformProvider(true, 10, 100L).from(configBuilder.build());
  }

  @Test
  public void testNoProblem() {
    Pipeline readPipeline = Pipeline.create();
    PCollection<Row> result =
        PCollectionRowTuple.empty(readPipeline)
            .apply(
                "ReadDebeziumSchema_" + database,
                makePtransform(
                    userName,
                    password,
                    database,
                    databaseContainer.getMappedPort(port),
                    databaseContainer.getHost(),
                    tableName))
            .get("output");
    assertThat(
        result.getSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toList()),
        Matchers.containsInAnyOrder(
            "before", "after", "source", "op", "ts_ms", "ts_us", "ts_ns", "transaction"));
  }

  @Test
  public void testWrongUser() {
    Pipeline readPipeline = Pipeline.create();
    DebeziumException ex =
        assertThrows(
            DebeziumException.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      makePtransform(
                          "wrongUser",
                          password,
                          database,
                          databaseContainer.getMappedPort(port),
                          databaseContainer.getHost(),
                          tableName))
                  .get("output");
            });
    assertThat(ex.getCause().getMessage(), Matchers.containsString("password"));
    assertThat(ex.getCause().getMessage(), Matchers.containsString("wrongUser"));
  }

  @Test
  public void testWrongPassword() {
    Pipeline readPipeline = Pipeline.create();
    DebeziumException ex =
        assertThrows(
            DebeziumException.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      "ReadWithWrongPassword_" + database,
                      makePtransform(
                          userName,
                          "wrongPassword",
                          database,
                          databaseContainer.getMappedPort(port),
                          databaseContainer.getHost(),
                          tableName))
                  .get("output");
            });
    assertThat(ex.getCause().getMessage(), Matchers.containsString("password"));
    assertThat(ex.getCause().getMessage(), Matchers.containsString(userName));
  }

  @Test
  public void testWrongPort() {
    Pipeline readPipeline = Pipeline.create();
    DebeziumException ex =
        assertThrows(
            DebeziumException.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      "ReadWithWrongPort_" + database,
                      makePtransform(
                          userName,
                          password,
                          database,
                          12345, // Incorrect port
                          databaseContainer.getHost(),
                          tableName))
                  .get("output");
            });
    Throwable lowestCause = ex.getCause();
    while (lowestCause.getCause() != null) {
      lowestCause = lowestCause.getCause();
    }
    assertThat(lowestCause.getMessage(), Matchers.containsString("Connection refused"));
  }

  @Test
  public void testWrongHost() {
    Pipeline readPipeline = Pipeline.create();
    assertThrows(
        Exception.class,
        () ->
            PCollectionRowTuple.empty(readPipeline)
                .apply(
                    "ReadWithWrongHost_" + database,
                    makePtransform(
                        userName,
                        password,
                        database,
                        databaseContainer.getMappedPort(port),
                        "169.254.254.254",
                        tableName))
                .get("output"));
  }
}
