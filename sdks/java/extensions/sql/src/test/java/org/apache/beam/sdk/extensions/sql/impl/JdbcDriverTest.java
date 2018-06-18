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
package org.apache.beam.sdk.extensions.sql.impl;

import static org.apache.beam.sdk.values.Row.toRow;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.impl.parser.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.mock.MockedBoundedTable;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.calcite.jdbc.CalciteConnection;
import org.junit.Before;
import org.junit.Test;

/** Test for {@link JdbcDriver}. */
public class JdbcDriverTest {

  private static final Schema BASIC_SCHEMA =
      Schema.builder()
          .addNullableField("id", Schema.FieldType.INT64)
          .addNullableField("name", Schema.FieldType.STRING)
          .build();

  private static final Schema COMPLEX_SCHEMA =
      Schema.builder()
          .addNullableField("description", Schema.FieldType.STRING)
          .addNullableField("nestedRow", Schema.FieldType.row(BASIC_SCHEMA))
          .build();

  @Before
  public void before() throws Exception {
    Class.forName("org.apache.beam.sdk.extensions.sql.impl.JdbcDriver");
  }

  @Test
  public void testDriverManager_getDriver() throws Exception {
    Driver driver = DriverManager.getDriver(JdbcDriver.CONNECT_STRING_PREFIX);
    assertTrue(driver instanceof JdbcDriver);
  }

  @Test
  public void testDriverManager_simple() throws Exception {
    Connection connection = DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX);
    Statement statement = connection.createStatement();
    // SELECT 1 is a special case and does not reach the parser
    assertTrue(statement.execute("SELECT 1"));
  }

  @Test
  public void testDriverManager_parse() throws Exception {
    Connection connection = DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX);
    Statement statement = connection.createStatement();
    assertTrue(statement.execute("SELECT 'beam'"));
  }

  @Test
  public void testDriverManager_ddl() throws Exception {
    Connection connection = DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX);

    // Ensure no tables
    final DatabaseMetaData metadata = connection.getMetaData();
    ResultSet resultSet = metadata.getTables(null, null, null, new String[] {"TABLE"});
    assertFalse(resultSet.next());

    // Create tables
    Statement statement = connection.createStatement();
    assertEquals(0, statement.executeUpdate("CREATE TABLE test (id INTEGER) TYPE 'text'"));

    // Ensure table test
    resultSet = metadata.getTables(null, null, null, new String[] {"TABLE"});
    assertTrue(resultSet.next());
    assertEquals("test", resultSet.getString("TABLE_NAME"));
    assertFalse(resultSet.next());

    // Create tables
    assertEquals(0, statement.executeUpdate("DROP TABLE test"));

    // Ensure no tables
    resultSet = metadata.getTables(null, null, null, new String[] {"TABLE"});
    assertFalse(resultSet.next());
  }

  @Test
  public void testSelectsFromExistingTable() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider);

    connection
        .createStatement()
        .executeUpdate("CREATE TABLE person (id BIGINT, name VARCHAR) TYPE 'test'");

    tableProvider.addRows("person", row(1L, "aaa"), row(2L, "bbb"));

    ResultSet selectResult =
        connection.createStatement().executeQuery("SELECT id, name FROM person");

    List<Row> resultRows =
        readResultSet(selectResult)
            .stream()
            .map(values -> values.stream().collect(toRow(BASIC_SCHEMA)))
            .collect(Collectors.toList());

    assertThat(resultRows, containsInAnyOrder(row(1L, "aaa"), row(2L, "bbb")));
  }

  @Test
  public void testSelectsFromExistingComplexTable() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider);

    connection
        .createStatement()
        .executeUpdate(
            "CREATE TABLE person ( \n"
                + "description VARCHAR, \n"
                + "nestedRow ROW< \n"
                + "              id BIGINT, \n"
                + "              name VARCHAR> \n"
                + ") \n"
                + "TYPE 'test'");

    tableProvider.addRows(
        "person",
        row(COMPLEX_SCHEMA, "description1", row(1L, "aaa")),
        row(COMPLEX_SCHEMA, "description2", row(2L, "bbb")));

    ResultSet selectResult =
        connection
            .createStatement()
            .executeQuery("SELECT person.nestedRow.id, person.nestedRow.name FROM person");

    List<Row> resultRows =
        readResultSet(selectResult)
            .stream()
            .map(values -> values.stream().collect(toRow(BASIC_SCHEMA)))
            .collect(Collectors.toList());

    assertThat(resultRows, containsInAnyOrder(row(1L, "aaa"), row(2L, "bbb")));
  }

  @Test
  public void testInsertIntoCreatedTable() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider);

    connection
        .createStatement()
        .executeUpdate("CREATE TABLE person (id BIGINT, name VARCHAR) TYPE 'test'");

    connection
        .createStatement()
        .executeUpdate("CREATE TABLE person_src (id BIGINT, name VARCHAR) TYPE 'test'");
    tableProvider.addRows("person_src", row(1L, "aaa"), row(2L, "bbb"));

    connection.createStatement().execute("INSERT INTO person SELECT id, name FROM person_src");

    ResultSet selectResult =
        connection.createStatement().executeQuery("SELECT id, name FROM person");

    List<Row> resultRows =
        readResultSet(selectResult)
            .stream()
            .map(resultValues -> resultValues.stream().collect(toRow(BASIC_SCHEMA)))
            .collect(Collectors.toList());

    assertThat(resultRows, containsInAnyOrder(row(1L, "aaa"), row(2L, "bbb")));
  }

  @Test
  public void testInternalConnect_boundedTable() throws Exception {
    ReadOnlyTableProvider tableProvider =
        new ReadOnlyTableProvider(
            "test",
            ImmutableMap.of(
                "test",
                MockedBoundedTable.of(
                        Schema.FieldType.INT32, "id",
                        Schema.FieldType.STRING, "name")
                    .addRows(1, "first")));
    CalciteConnection connection = JdbcDriver.connect(tableProvider);
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM test");
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getInt("id"));
    assertEquals("first", resultSet.getString("name"));
    assertFalse(resultSet.next());
  }

  private List<List<Object>> readResultSet(ResultSet result) throws Exception {
    List<List<Object>> results = new ArrayList<>();

    while (result.next()) {
      List<Object> rowValues = new ArrayList<>();
      for (int i = 0; i < result.getMetaData().getColumnCount(); i++) {
        rowValues.add(result.getObject(i + 1));
      }

      results.add(rowValues);
    }

    return results;
  }

  private Row row(Object... values) {
    return row(BASIC_SCHEMA, values);
  }

  private Row row(Schema schema, Object... values) {
    return Row.withSchema(schema).addValues(values).build();
  }
}
