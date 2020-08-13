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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.sql.meta.provider.ReadOnlyTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestBoundedTable;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestTableProvider;
import org.apache.beam.sdk.extensions.sql.meta.provider.test.TestUnboundedTable;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.util.ReleaseInfo;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.calcite.v1_20_0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.CalciteConnection;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.jdbc.CalciteSchema;
import org.apache.beam.vendor.calcite.v1_20_0.org.apache.calcite.schema.SchemaPlus;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test for {@link JdbcDriver}. */
public class JdbcDriverTest {
  public static final DateTime FIRST_DATE = new DateTime(1);

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

  private static final ReadOnlyTableProvider BOUNDED_TABLE =
      new ReadOnlyTableProvider(
          "test",
          ImmutableMap.of(
              "test",
              TestBoundedTable.of(
                      Schema.FieldType.INT32, "id",
                      Schema.FieldType.STRING, "name")
                  .addRows(1, "first")));

  @Rule public ExpectedException thrown = ExpectedException.none();

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

  /** Tests that the userAgent is set in the pipeline options of the connection. */
  @Test
  public void testDriverManager_defaultUserAgent() throws Exception {
    Connection connection = DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX);
    SchemaPlus rootSchema = ((CalciteConnection) connection).getRootSchema();
    BeamCalciteSchema beamSchema =
        (BeamCalciteSchema) CalciteSchema.from(rootSchema.getSubSchema("beam")).schema;
    Map<String, String> pipelineOptions = beamSchema.getPipelineOptions();
    assertThat(pipelineOptions.get("userAgent"), containsString("BeamSQL"));
  }

  /** Tests that userAgent is set. */
  @Test
  public void testDriverManager_hasUserAgent() throws Exception {
    JdbcConnection connection =
        (JdbcConnection) DriverManager.getConnection(JdbcDriver.CONNECT_STRING_PREFIX);
    BeamCalciteSchema schema = connection.getCurrentBeamSchema();
    assertThat(
        schema.getPipelineOptions().get("userAgent"),
        equalTo("BeamSQL/" + ReleaseInfo.getReleaseInfo().getVersion()));
  }

  /** Tests that userAgent can be overridden on the querystring. */
  @Test
  public void testDriverManager_setUserAgent() throws Exception {
    Connection connection =
        DriverManager.getConnection(
            JdbcDriver.CONNECT_STRING_PREFIX + "beam.userAgent=Secret Agent");
    SchemaPlus rootSchema = ((CalciteConnection) connection).getRootSchema();
    BeamCalciteSchema beamSchema =
        (BeamCalciteSchema) CalciteSchema.from(rootSchema.getSubSchema("beam")).schema;
    Map<String, String> pipelineOptions = beamSchema.getPipelineOptions();
    assertThat(pipelineOptions.get("userAgent"), equalTo("Secret Agent"));
  }

  /** Tests that unknown pipeline options are passed verbatim from the JDBC URI. */
  @Test
  public void testDriverManager_pipelineOptionsPlumbing() throws Exception {
    Connection connection =
        DriverManager.getConnection(
            JdbcDriver.CONNECT_STRING_PREFIX
                + "beam.foo=baz;beam.foobizzle=mahshizzle;other=smother");
    SchemaPlus rootSchema = ((CalciteConnection) connection).getRootSchema();
    BeamCalciteSchema beamSchema =
        (BeamCalciteSchema) CalciteSchema.from(rootSchema.getSubSchema("beam")).schema;
    Map<String, String> pipelineOptions = beamSchema.getPipelineOptions();
    assertThat(pipelineOptions.get("foo"), equalTo("baz"));
    assertThat(pipelineOptions.get("foobizzle"), equalTo("mahshizzle"));
    assertThat(pipelineOptions.get("other"), nullValue());
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

    // create external tables
    Statement statement = connection.createStatement();
    assertEquals(0, statement.executeUpdate("CREATE EXTERNAL TABLE test (id INTEGER) TYPE 'text'"));

    // Ensure table test
    resultSet = metadata.getTables(null, null, null, new String[] {"TABLE"});
    assertTrue(resultSet.next());
    assertEquals("test", resultSet.getString("TABLE_NAME"));
    assertFalse(resultSet.next());

    assertEquals(0, statement.executeUpdate("DROP TABLE test"));

    // Ensure no tables
    resultSet = metadata.getTables(null, null, null, new String[] {"TABLE"});
    assertFalse(resultSet.next());
  }

  @Test
  public void testSelectsFromExistingTable() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());

    connection
        .createStatement()
        .executeUpdate("CREATE EXTERNAL TABLE person (id BIGINT, name VARCHAR) TYPE 'test'");

    tableProvider.addRows("person", row(1L, "aaa"), row(2L, "bbb"));

    ResultSet selectResult =
        connection.createStatement().executeQuery("SELECT id, name FROM person");

    List<Row> resultRows =
        readResultSet(selectResult).stream()
            .map(values -> values.stream().collect(toRow(BASIC_SCHEMA)))
            .collect(Collectors.toList());

    assertThat(resultRows, containsInAnyOrder(row(1L, "aaa"), row(2L, "bbb")));
  }

  @Test
  public void testTimestampWithDefaultTimezone() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());

    // A table with one TIMESTAMP column
    Schema schema = Schema.builder().addLogicalTypeField("ts", SqlTypes.TIMESTAMP).build();
    connection
        .createStatement()
        .executeUpdate("CREATE EXTERNAL TABLE test (ts TIMESTAMP) TYPE 'test'");

    Instant july1 = Instant.parse("2018-07-01T01:02:03Z");
    tableProvider.addRows("test", Row.withSchema(schema).addValue(july1).build());

    ResultSet selectResult =
        connection.createStatement().executeQuery(String.format("SELECT ts FROM test"));
    selectResult.next();
    Timestamp ts = selectResult.getTimestamp(1);

    assertThat(
        String.format(
            "Wrote %s to a table, but got back %s",
            ISODateTimeFormat.basicDateTime().print(july1.toEpochMilli()),
            ISODateTimeFormat.basicDateTime().print(ts.getTime())),
        ts.getTime(),
        equalTo(july1.toEpochMilli()));
  }

  @Test
  @Ignore("https://issues.apache.org/jira/browse/CALCITE-2394")
  public void testTimestampWithNonzeroTimezone() throws Exception {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"), Locale.ROOT);
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());

    // A table with one TIMESTAMP column
    Schema schema = Schema.builder().addLogicalTypeField("ts", SqlTypes.TIMESTAMP).build();
    connection
        .createStatement()
        .executeUpdate("CREATE EXTERNAL TABLE test (ts TIMESTAMP) TYPE 'test'");

    Instant july1 = Instant.parse("2018-07-01T01:02:03Z");
    tableProvider.addRows("test", Row.withSchema(schema).addValue(july1).build());

    ResultSet selectResult =
        connection.createStatement().executeQuery(String.format("SELECT ts FROM test"));
    selectResult.next();
    Timestamp ts = selectResult.getTimestamp(1, cal);

    assertThat(
        String.format(
            "Wrote %s to a table, but got back %s",
            ISODateTimeFormat.basicDateTime().print(july1.toEpochMilli()),
            ISODateTimeFormat.basicDateTime().print(ts.getTime())),
        ts.getTime(),
        equalTo(july1.toEpochMilli()));
  }

  @Test
  public void testTimestampWithZeroTimezone() throws Exception {
    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"), Locale.ROOT);
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());

    // A table with one TIMESTAMP column
    Schema schema = Schema.builder().addLogicalTypeField("ts", SqlTypes.TIMESTAMP).build();
    connection
        .createStatement()
        .executeUpdate("CREATE EXTERNAL TABLE test (ts TIMESTAMP) TYPE 'test'");

    Instant july1 = Instant.parse("2018-07-01T01:02:03Z");
    tableProvider.addRows("test", Row.withSchema(schema).addValue(july1).build());

    ResultSet selectResult =
        connection.createStatement().executeQuery(String.format("SELECT ts FROM test"));
    selectResult.next();
    Timestamp ts = selectResult.getTimestamp(1, cal);

    assertThat(
        String.format(
            "Wrote %s to a table, but got back %s",
            ISODateTimeFormat.basicDateTime().print(july1.toEpochMilli()),
            ISODateTimeFormat.basicDateTime().print(ts.getTime())),
        ts.getTime(),
        equalTo(july1.toEpochMilli()));
  }

  @Test
  public void testSelectsFromExistingComplexTable() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());

    connection
        .createStatement()
        .executeUpdate(
            "CREATE EXTERNAL TABLE person ( \n"
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
        readResultSet(selectResult).stream()
            .map(values -> values.stream().collect(toRow(BASIC_SCHEMA)))
            .collect(Collectors.toList());

    assertThat(resultRows, containsInAnyOrder(row(1L, "aaa"), row(2L, "bbb")));
  }

  @Test
  public void testInsertIntoCreatedTable() throws Exception {
    TestTableProvider tableProvider = new TestTableProvider();
    Connection connection = JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());

    connection
        .createStatement()
        .executeUpdate("CREATE EXTERNAL TABLE person (id BIGINT, name VARCHAR) TYPE 'test'");

    connection
        .createStatement()
        .executeUpdate("CREATE EXTERNAL TABLE person_src (id BIGINT, name VARCHAR) TYPE 'test'");
    tableProvider.addRows("person_src", row(1L, "aaa"), row(2L, "bbb"));

    connection.createStatement().execute("INSERT INTO person SELECT id, name FROM person_src");

    ResultSet selectResult =
        connection.createStatement().executeQuery("SELECT id, name FROM person");

    List<Row> resultRows =
        readResultSet(selectResult).stream()
            .map(resultValues -> resultValues.stream().collect(toRow(BASIC_SCHEMA)))
            .collect(Collectors.toList());

    assertThat(resultRows, containsInAnyOrder(row(1L, "aaa"), row(2L, "bbb")));
  }

  @Test
  public void testInternalConnect_boundedTable() throws Exception {
    CalciteConnection connection =
        JdbcDriver.connect(BOUNDED_TABLE, PipelineOptionsFactory.create());
    Statement statement = connection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT * FROM test");
    assertTrue(resultSet.next());
    assertEquals(1, resultSet.getInt("id"));
    assertEquals("first", resultSet.getString("name"));
    assertFalse(resultSet.next());
  }

  @Test
  public void testInternalConnect_bounded_limit() throws Exception {
    ReadOnlyTableProvider tableProvider =
        new ReadOnlyTableProvider(
            "test",
            ImmutableMap.of(
                "test",
                TestBoundedTable.of(
                        Schema.FieldType.INT32, "id",
                        Schema.FieldType.STRING, "name")
                    .addRows(1, "first")
                    .addRows(1, "second first")
                    .addRows(2, "second")));

    CalciteConnection connection =
        JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());
    Statement statement = connection.createStatement();
    ResultSet resultSet1 = statement.executeQuery("SELECT * FROM test LIMIT 5");
    assertTrue(resultSet1.next());
    assertTrue(resultSet1.next());
    assertTrue(resultSet1.next());
    assertFalse(resultSet1.next());
    assertFalse(resultSet1.next());

    ResultSet resultSet2 = statement.executeQuery("SELECT * FROM test LIMIT 1");
    assertTrue(resultSet2.next());
    assertFalse(resultSet2.next());

    ResultSet resultSet3 = statement.executeQuery("SELECT * FROM test LIMIT 2");
    assertTrue(resultSet3.next());
    assertTrue(resultSet3.next());
    assertFalse(resultSet3.next());

    ResultSet resultSet4 = statement.executeQuery("SELECT * FROM test LIMIT 3");
    assertTrue(resultSet4.next());
    assertTrue(resultSet4.next());
    assertTrue(resultSet4.next());
    assertFalse(resultSet4.next());
  }

  @Test
  public void testInternalConnect_unbounded_limit() throws Exception {
    ReadOnlyTableProvider tableProvider =
        new ReadOnlyTableProvider(
            "test",
            ImmutableMap.of(
                "test",
                TestUnboundedTable.of(
                        Schema.FieldType.INT32,
                        "order_id",
                        Schema.FieldType.INT32,
                        "site_id",
                        Schema.FieldType.INT32,
                        "price",
                        Schema.FieldType.logicalType(SqlTypes.TIMESTAMP),
                        "order_time")
                    .timestampColumnIndex(3)
                    .addRows(Duration.ZERO, 1, 1, 1, FIRST_DATE, 1, 2, 6, FIRST_DATE)));

    CalciteConnection connection =
        JdbcDriver.connect(tableProvider, PipelineOptionsFactory.create());
    Statement statement = connection.createStatement();

    ResultSet resultSet1 = statement.executeQuery("SELECT * FROM test LIMIT 1");
    assertTrue(resultSet1.next());
    assertFalse(resultSet1.next());

    ResultSet resultSet2 = statement.executeQuery("SELECT * FROM test LIMIT 2");
    assertTrue(resultSet2.next());
    assertTrue(resultSet2.next());
    assertFalse(resultSet2.next());
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

  @Test
  public void testInternalConnect_setDirectRunner() throws Exception {
    CalciteConnection connection =
        JdbcDriver.connect(BOUNDED_TABLE, PipelineOptionsFactory.create());
    Statement statement = connection.createStatement();
    assertEquals(0, statement.executeUpdate("SET runner = direct"));
    assertTrue(statement.execute("SELECT * FROM test"));
  }

  @Test
  public void testInternalConnect_setBogusRunner() throws Exception {
    thrown.expectMessage("Unknown 'runner' specified 'bogus'");

    CalciteConnection connection =
        JdbcDriver.connect(BOUNDED_TABLE, PipelineOptionsFactory.create());
    Statement statement = connection.createStatement();
    assertEquals(0, statement.executeUpdate("SET runner = bogus"));
    assertTrue(statement.execute("SELECT * FROM test"));
  }

  @Test
  public void testInternalConnect_resetAll() throws Exception {
    CalciteConnection connection =
        JdbcDriver.connect(BOUNDED_TABLE, PipelineOptionsFactory.create());
    Statement statement = connection.createStatement();
    assertEquals(0, statement.executeUpdate("SET runner = bogus"));
    assertEquals(0, statement.executeUpdate("RESET ALL"));
    assertTrue(statement.execute("SELECT * FROM test"));
  }

  @Test
  public void testInternalConnect_driverManagerDifferentProtocol() throws Exception {
    thrown.expect(SQLException.class);
    thrown.expectMessage("No suitable driver found");

    DriverManager.getConnection("jdbc:baaaaaad");
  }
}
