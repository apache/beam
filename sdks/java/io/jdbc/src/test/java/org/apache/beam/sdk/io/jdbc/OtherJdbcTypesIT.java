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

import static org.apache.beam.sdk.io.common.IOITHelper.executeWithRetry;
import static org.apache.beam.sdk.io.common.IOITHelper.readIOTestPipelineOptions;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.PostgresIOTestPipelineOptions;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.postgresql.ds.PGSimpleDataSource;

@RunWith(JUnit4.class)
public class OtherJdbcTypesIT {

  private static final int ID_VALUE = 1;
  private static final UUID UUID_VALUE = UUID.randomUUID();
  private static final String JSON_VALUE = "{\"id\": \"value\"}";

  private static PGSimpleDataSource dataSource;
  private static String tableName;
  @Rule public TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void setup() throws Exception {
    PostgresIOTestPipelineOptions options;
    try {
      options = readIOTestPipelineOptions(PostgresIOTestPipelineOptions.class);
    } catch (IllegalArgumentException e) {
      options = null;
    }
    org.junit.Assume.assumeNotNull(options);

    dataSource = DatabaseTestHelper.getPostgresDataSource(options);
    tableName = DatabaseTestHelper.getTestTableName("IT");
    executeWithRetry(OtherJdbcTypesIT::createTable);
  }

  private static void createTable() throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(
            String.format(
                "create table %s (id INT, uuid_field uuid, json_field json, jsonb_field jsonb)",
                tableName));
      }
    }
  }

  private static void insertTestData() throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (PreparedStatement statement =
          connection.prepareStatement(
              String.format("insert into %s values (?, ?, ?, ?)", tableName))) {
        statement.setInt(1, ID_VALUE);
        statement.setObject(2, UUID_VALUE);
        statement.setObject(3, JSON_VALUE, java.sql.Types.OTHER);
        statement.setObject(4, JSON_VALUE, java.sql.Types.OTHER);
        statement.execute();
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    executeWithRetry(OtherJdbcTypesIT::deleteTable);
  }

  private static void deleteTable() throws SQLException {
    DatabaseTestHelper.deleteTable(dataSource, tableName);
  }

  /** Tests reading and writing data with other jdbc types for a postgres database. */
  @Test
  public void testReadAndWrite() throws SQLException {
    insertTestData();
    PCollection<Row> rows = readRows("Read rows");

    rows.apply(
            "Change ID",
            MapElements.into(TypeDescriptors.rows())
                .via((Row row) -> Row.fromRow(row).withFieldValue("id", ID_VALUE + 1).build()))
        .setRowSchema(rows.getSchema())
        .apply(
            "Write",
            JdbcIO.<Row>write()
                .withDataSourceProviderFn(ignored -> dataSource)
                .withTable(tableName));

    pipeline.run().waitUntilFinish();

    PCollection<Row> resultRows = readRows("Read result rows");

    List<Row> expectedRows =
        ImmutableList.of(
            Row.withSchema(rows.getSchema())
                .addValues(ID_VALUE + 1, UUID_VALUE, JSON_VALUE, JSON_VALUE)
                .build(),
            Row.withSchema(rows.getSchema())
                .addValues(ID_VALUE, UUID_VALUE, JSON_VALUE, JSON_VALUE)
                .build());
    PCollection<Row> output =
        resultRows.apply(Select.fieldNames("id", "uuid_field", "json_field", "jsonb_field"));
    PAssert.that(output).containsInAnyOrder(expectedRows);

    pipeline.run();
  }

  private PCollection<Row> readRows(String transformationName) {
    return pipeline.apply(
        transformationName,
        JdbcIO.readRows()
            .withDataSourceProviderFn(ignored -> dataSource)
            .withQuery(
                String.format(
                    "select id, uuid_field, json_field, jsonb_field from %s", tableName)));
  }
}
