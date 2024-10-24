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

import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.kafka.connect.errors.ConnectException;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

@RunWith(Parameterized.class)
public class DebeziumReadSchemaTransformTest {

  @ClassRule
  public static final PostgreSQLContainer<?> POSTGRES_SQL_CONTAINER =
      // TODO(https://github.com/apache/beam/issues/32937): use latest tag once
	    // a container exists again
      new PostgreSQLContainer<>(
              DockerImageName.parse("debezium/example-postgres:3.0.0.final")
                  .asCompatibleSubstituteFor("postgres"))
          .withPassword("dbz")
          .withUsername("debezium")
          .withExposedPorts(5432)
          .withDatabaseName("inventory");

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

  @Parameterized.Parameters
  public static Iterable<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {POSTGRES_SQL_CONTAINER, "debezium", "dbz", "POSTGRES", 5432},
          {MY_SQL_CONTAINER, "debezium", "dbz", "MYSQL", 3306}
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

  private PTransform<PCollectionRowTuple, PCollectionRowTuple> makePtransform(
      String user, String password, String database, Integer port, String host) {
    return new DebeziumReadSchemaTransformProvider(true, 10, 100L)
        .from(
            DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration.builder()
                .setDatabase(database)
                .setPassword(password)
                .setUsername(user)
                .setHost(host)
                // In postgres, this field is "schema.table", while in MySQL it
                // is "database.table".
                .setTable("inventory.customers")
                .setPort(port)
                .build());
  }

  @Test
  public void testNoProblem() {
    Pipeline readPipeline = Pipeline.create();
    PCollection<Row> result =
        PCollectionRowTuple.empty(readPipeline)
            .apply(
                makePtransform(
                    userName,
                    password,
                    database,
                    databaseContainer.getMappedPort(port),
                    "localhost"))
            .get("output");
    assertThat(
        result.getSchema().getFields().stream()
            .map(field -> field.getName())
            .collect(Collectors.toList()),
        Matchers.containsInAnyOrder("before", "after", "source", "op", "ts_ms", "transaction"));
  }

  @Test
  public void testWrongUser() {
    Pipeline readPipeline = Pipeline.create();
    ConnectException ex =
        assertThrows(
            ConnectException.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      makePtransform(
                          "wrongUser",
                          password,
                          database,
                          databaseContainer.getMappedPort(port),
                          "localhost"))
                  .get("output");
            });
    assertThat(ex.getCause().getMessage(), Matchers.containsString("password"));
    assertThat(ex.getCause().getMessage(), Matchers.containsString("wrongUser"));
  }

  @Test
  public void testWrongPassword() {
    Pipeline readPipeline = Pipeline.create();
    ConnectException ex =
        assertThrows(
            ConnectException.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      makePtransform(
                          userName,
                          "wrongPassword",
                          database,
                          databaseContainer.getMappedPort(port),
                          "localhost"))
                  .get("output");
            });
    assertThat(ex.getCause().getMessage(), Matchers.containsString("password"));
    assertThat(ex.getCause().getMessage(), Matchers.containsString(userName));
  }

  @Test
  public void testWrongPort() {
    Pipeline readPipeline = Pipeline.create();
    ConnectException ex =
        assertThrows(
            ConnectException.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(makePtransform(userName, password, database, 12345, "localhost"))
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
                    makePtransform(
                        userName,
                        password,
                        database,
                        databaseContainer.getMappedPort(port),
                        "169.254.254.254"))
                .get("output"));
  }
}
