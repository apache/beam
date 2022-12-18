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

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;

@RunWith(Parameterized.class)
public class DebeziumReadSchemaTransformTest {

  @ClassRule
  public static final PostgreSQLContainer<?> POSTGRES_SQL_CONTAINER =
      new PostgreSQLContainer<>(
              DockerImageName.parse("debezium/example-postgres:latest")
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

  static DataSource getPostgresDatasource(Void unused) {
    PGSimpleDataSource dataSource = new PGSimpleDataSource();
    dataSource.setDatabaseName("inventory");
    dataSource.setServerName(POSTGRES_SQL_CONTAINER.getContainerIpAddress());
    dataSource.setPortNumber(POSTGRES_SQL_CONTAINER.getMappedPort(5432));
    dataSource.setUser("debezium");
    dataSource.setPassword("dbz");
    return dataSource;
  }

  public static DataSource getMysqlDatasource(Void unused) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setJdbcUrl(MY_SQL_CONTAINER.getJdbcUrl());
    hikariConfig.setUsername(MY_SQL_CONTAINER.getUsername());
    hikariConfig.setPassword(MY_SQL_CONTAINER.getPassword());
    hikariConfig.setDriverClassName(MY_SQL_CONTAINER.getDriverClassName());
    return new HikariDataSource(hikariConfig);
  }

  private PTransform<PCollectionRowTuple, PCollectionRowTuple> makePtransform(
          String user, String password, String database, Integer port, String host, String table) {
    return new DebeziumReadSchemaTransformProvider(true, 10, 100L)
            .from(
                    DebeziumReadSchemaTransformProvider.DebeziumReadSchemaTransformConfiguration.builder()
                            .setDatabase(database)
                            .setPassword(password)
                            .setUsername(user)
                            .setHost(host)
                            // In postgres, this field is "schema.table", while in MySQL it
                            // is "database.table".
                            .setTable(table)
                            .setPort(port)
                            .build())
            .buildTransform();
  }

  private PTransform<PCollectionRowTuple, PCollectionRowTuple> makePtransform(
      String user, String password, String database, Integer port, String host) {
    return makePtransform(user, password, database, port, host, "inventory.customers");
  }

  @Test
  public void testNoProblemButNoData() throws SQLException {
    DataSource ds = database.equals("MYSQL") ? getMysqlDatasource(null) : getPostgresDatasource(null);
    Connection conn = ds.getConnection();
    conn.createStatement().execute("DELETE FROM inventory.orders");
    conn.close();
    Pipeline readPipeline = Pipeline.create();
    PCollection<Row> result =
            PCollectionRowTuple.empty(readPipeline)
                    .apply(
                            makePtransform(
                                    userName,
                                    password,
                                    database,
                                    databaseContainer.getMappedPort(port),
                                    "localhost",
                                    "inventory.orders"))
                    .get("output");
    assertThat(
            result.getSchema().getFields().stream()
                    .map(field -> field.getName())
                    .collect(Collectors.toList()),
            Matchers.containsInAnyOrder("before", "after", "source", "op", "ts_ms", "transaction"));
    assertThat(
            result.getSchema().getField("after").getType().getRowSchema().getFields().stream()
                    .map(field -> field.getName())
                    .collect(Collectors.toList()),
            Matchers.containsInAnyOrder("before", "after", "source", "op", "ts_ms", "transaction"));
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
  public void testWrongHost() throws Exception {
    Pipeline readPipeline = Pipeline.create();
    Exception ex =
        assertThrows(
            Exception.class,
            () -> {
              PCollectionRowTuple.empty(readPipeline)
                  .apply(
                      makePtransform(
                          userName,
                          password,
                          database,
                          databaseContainer.getMappedPort(port),
                          "23.128.129.130"))
                  .get("output");
            });
    Throwable lowestCause = ex.getCause();
    while (lowestCause.getCause() != null) {
      lowestCause = lowestCause.getCause();
    }
    assertThat(lowestCause.getMessage(), Matchers.containsString("Connection refused"));
  }
}
