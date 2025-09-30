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
package org.apache.beam.sdk.io.jdbc;

import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.PostgresIOTestPipelineOptions;
import org.apache.beam.sdk.io.jdbc.providers.ReadFromPostgresSchemaTransformProvider;
import org.apache.beam.sdk.io.jdbc.providers.WriteToPostgresSchemaTransformProvider;
import org.apache.beam.sdk.managed.Managed;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * A test of {@link org.apache.beam.sdk.io.jdbc.JdbcIO} on an independent Postgres instance.
 *
 * <p>Similar to JdbcIOIT, this test requires a running instance of Postgres. Pass in connection
 * information using PipelineOptions:
 *
 * <pre>
 *  ./gradlew integrationTest -p sdks/java/io/jdbc -DintegrationTestPipelineOptions='[
 *  "--postgresServerName=1.2.3.4",
 *  "--postgresUsername=postgres",
 *  "--postgresDatabaseName=myfancydb",
 *  "--postgresPassword=mypass",
 *  "--postgresSsl=false" ]'
 *  --tests org.apache.beam.sdk.io.jdbc.JdbcIOPostgresIT
 *  -DintegrationTestRunner=direct
 * </pre>
 */
@RunWith(JUnit4.class)
public class JdbcIOPostgresIT {
  private static final Schema INPUT_SCHEMA =
      Schema.of(
          Schema.Field.of("id", Schema.FieldType.INT32),
          Schema.Field.of("name", Schema.FieldType.STRING));

  private static final List<Row> ROWS =
      Arrays.asList(
          Row.withSchema(INPUT_SCHEMA)
              .withFieldValue("id", 1)
              .withFieldValue("name", "foo")
              .build(),
          Row.withSchema(INPUT_SCHEMA)
              .withFieldValue("id", 2)
              .withFieldValue("name", "bar")
              .build(),
          Row.withSchema(INPUT_SCHEMA)
              .withFieldValue("id", 3)
              .withFieldValue("name", "baz")
              .build());

  private static PGSimpleDataSource dataSource;
  private static String jdbcUrl;

  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() {
    PostgresIOTestPipelineOptions options;
    try {
      options = readIOTestPipelineOptions(PostgresIOTestPipelineOptions.class);
    } catch (IllegalArgumentException e) {
      options = null;
    }
    org.junit.Assume.assumeNotNull(options);
    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    jdbcUrl = DatabaseTestHelper.getPostgresDBUrl(options);
  }

  @Test
  public void testWriteThenRead() throws SQLException {
    String tableName = DatabaseTestHelper.getTestTableName("JdbcIOPostgresIT");
    DatabaseTestHelper.createTable(dataSource, tableName);

    JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration writeConfig =
        JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
            .setJdbcUrl(jdbcUrl)
            .setUsername(dataSource.getUser())
            .setPassword(dataSource.getPassword())
            .setLocation(tableName)
            .build();

    JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration readConfig =
        JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
            .setJdbcUrl(jdbcUrl)
            .setUsername(dataSource.getUser())
            .setPassword(dataSource.getPassword())
            .setLocation(tableName)
            .build();

    try {
      PCollection<Row> input = writePipeline.apply(Create.of(ROWS)).setRowSchema(INPUT_SCHEMA);
      PCollectionRowTuple inputTuple = PCollectionRowTuple.of("input", input);
      inputTuple.apply(
          new WriteToPostgresSchemaTransformProvider.PostgresWriteSchemaTransform(writeConfig));
      writePipeline.run().waitUntilFinish();

      PCollectionRowTuple pbeginTuple = PCollectionRowTuple.empty(readPipeline);
      PCollectionRowTuple outputTuple =
          pbeginTuple.apply(
              new ReadFromPostgresSchemaTransformProvider.PostgresReadSchemaTransform(readConfig));
      PCollection<Row> output = outputTuple.get("output");
      PAssert.that(output).containsInAnyOrder(ROWS);
      readPipeline.run().waitUntilFinish();
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }

  @Test
  public void testManagedWriteThenManagedRead() throws SQLException {
    String tableName = DatabaseTestHelper.getTestTableName("ManagedJdbcIOPostgresIT");
    DatabaseTestHelper.createTable(dataSource, tableName);

    Map<String, Object> writeConfig =
        ImmutableMap.<String, Object>builder()
            .put("jdbc_url", jdbcUrl)
            .put("username", dataSource.getUser())
            .put("password", dataSource.getPassword())
            .put("location", tableName)
            .build();

    Map<String, Object> readConfig =
        ImmutableMap.<String, Object>builder()
            .put("jdbc_url", jdbcUrl)
            .put("username", dataSource.getUser())
            .put("password", dataSource.getPassword())
            .put("location", tableName)
            .build();

    try {
      PCollection<Row> input = writePipeline.apply(Create.of(ROWS)).setRowSchema(INPUT_SCHEMA);
      input.apply(Managed.write(Managed.POSTGRES).withConfig(writeConfig));
      writePipeline.run().waitUntilFinish();

      PCollectionRowTuple output =
          readPipeline.apply(Managed.read(Managed.POSTGRES).withConfig(readConfig));
      PAssert.that(output.get("output")).containsInAnyOrder(ROWS);
      readPipeline.run().waitUntilFinish();
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }
}
