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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.NetworkTestHelper;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PoolableDataSourceProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.derby.drda.NetworkServerControl;
import org.apache.derby.jdbc.ClientDataSource;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test on the JdbcIO. */
@RunWith(JUnit4.class)
public class JdbcIOTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcIOTest.class);
  private static final int EXPECTED_ROW_COUNT = 1000;
  private static final String BACKOFF_TABLE = "UT_WRITE_BACKOFF";

  private static NetworkServerControl derbyServer;
  private static ClientDataSource dataSource;

  private static int port;
  private static String readTableName;

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(JdbcIO.class);

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    port = NetworkTestHelper.getAvailableLocalPort();
    LOG.info("Starting Derby database on {}", port);

    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "target/derby.log");

    derbyServer = new NetworkServerControl(InetAddress.getByName("localhost"), port);
    StringWriter out = new StringWriter();
    derbyServer.start(new PrintWriter(out));
    boolean started = false;
    int count = 0;
    // Use two different methods to detect when server is started:
    // 1) Check the server stdout for the "started" string
    // 2) wait up to 15 seconds for the derby server to start based on a ping
    // on faster machines and networks, this may return very quick, but on slower
    // networks where the DNS lookups are slow, this may take a little time
    while (!started && count < 30) {
      if (out.toString().contains("started")) {
        started = true;
      } else {
        count++;
        Thread.sleep(500);
        try {
          derbyServer.ping();
          started = true;
        } catch (Throwable t) {
          // ignore, still trying to start
        }
      }
    }

    dataSource = new ClientDataSource();
    dataSource.setCreateDatabase("create");
    dataSource.setDatabaseName("target/beam");
    dataSource.setServerName("localhost");
    dataSource.setPortNumber(port);

    readTableName = DatabaseTestHelper.getTestTableName("UT_READ");

    DatabaseTestHelper.createTable(dataSource, readTableName);
    addInitialData(dataSource, readTableName);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try {
      DatabaseTestHelper.deleteTable(dataSource, readTableName);
    } finally {
      if (derbyServer != null) {
        derbyServer.shutdown();
      }
    }
  }

  @Test
  public void testDataSourceConfigurationDataSource() throws Exception {
    JdbcIO.DataSourceConfiguration config = JdbcIO.DataSourceConfiguration.create(dataSource);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationDataSourceWithoutPool() {
    assertTrue(
        JdbcIO.DataSourceConfiguration.create(dataSource).buildDatasource()
            instanceof ClientDataSource);
  }

  @Test
  public void testDataSourceConfigurationDataSourceWithPool() {
    assertTrue(
        JdbcIO.PoolableDataSourceProvider.of(JdbcIO.DataSourceConfiguration.create(dataSource))
                .apply(null)
            instanceof PoolingDataSource);
  }

  @Test
  public void testDataSourceConfigurationDriverAndUrl() throws Exception {
    JdbcIO.DataSourceConfiguration config =
        JdbcIO.DataSourceConfiguration.create(
            "org.apache.derby.jdbc.ClientDriver",
            "jdbc:derby://localhost:" + port + "/target/beam");
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationUsernameAndPassword() throws Exception {
    String username = "sa";
    String password = "sa";
    JdbcIO.DataSourceConfiguration config =
        JdbcIO.DataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                "jdbc:derby://localhost:" + port + "/target/beam")
            .withUsername(username)
            .withPassword(password);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationNullPassword() throws Exception {
    String username = "sa";
    String password = null;
    JdbcIO.DataSourceConfiguration config =
        JdbcIO.DataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                "jdbc:derby://localhost:" + port + "/target/beam")
            .withUsername(username)
            .withPassword(password);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationNullUsernameAndPassword() throws Exception {
    String username = null;
    String password = null;
    JdbcIO.DataSourceConfiguration config =
        JdbcIO.DataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                "jdbc:derby://localhost:" + port + "/target/beam")
            .withUsername(username)
            .withPassword(password);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testSetConnectoinInitSqlFailWithDerbyDB() {
    String username = "sa";
    String password = "sa";
    JdbcIO.DataSourceConfiguration config =
        JdbcIO.DataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                "jdbc:derby://localhost:" + port + "/target/beam")
            .withUsername(username)
            .withPassword(password)
            .withConnectionInitSqls(ImmutableList.of("SET innodb_lock_wait_timeout = 5"));

    assertThrows(
        "innodb_lock_wait_timeout",
        SQLException.class,
        () -> config.buildDatasource().getConnection());
  }

  /** Create test data that is consistent with that generated by TestRow. */
  private static void addInitialData(DataSource dataSource, String tableName) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(String.format("insert into %s values (?,?)", tableName))) {
        for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
          preparedStatement.clearParameters();
          preparedStatement.setInt(1, i);
          preparedStatement.setString(2, TestRow.getNameForSeed(i));
          preparedStatement.executeUpdate();
        }
      }
      connection.commit();
    }
  }

  @Test
  public void testRead() {
    PCollection<TestRow> rows =
        pipeline.apply(
            JdbcIO.<TestRow>read()
                .withFetchSize(12)
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                .withQuery("select name,id from " + readTableName)
                .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(rows.apply("Count All", Count.globally()))
        .isEqualTo((long) EXPECTED_ROW_COUNT);

    Iterable<TestRow> expectedValues = TestRow.getExpectedValues(0, EXPECTED_ROW_COUNT);
    PAssert.that(rows).containsInAnyOrder(expectedValues);

    pipeline.run();
  }

  @Test
  public void testReadWithSingleStringParameter() {
    PCollection<TestRow> rows =
        pipeline.apply(
            JdbcIO.<TestRow>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                .withQuery(String.format("select name,id from %s where name = ?", readTableName))
                .withStatementPreparator(
                    preparedStatement -> preparedStatement.setString(1, TestRow.getNameForSeed(1)))
                .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(rows.apply("Count All", Count.globally())).isEqualTo(1L);

    Iterable<TestRow> expectedValues = Collections.singletonList(TestRow.fromSeed(1));
    PAssert.that(rows).containsInAnyOrder(expectedValues);

    pipeline.run();
  }

  @Test
  public void testReadRowsWithDataSourceConfiguration() {
    PCollection<Row> rows =
        pipeline.apply(
            JdbcIO.readRows()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(dataSource))
                .withQuery(String.format("select name,id from %s where name = ?", readTableName))
                .withStatementPreparator(
                    preparedStatement ->
                        preparedStatement.setString(1, TestRow.getNameForSeed(1))));

    Schema expectedSchema =
        Schema.of(
            Schema.Field.of("NAME", LogicalTypes.variableLengthString(JDBCType.VARCHAR, 500))
                .withNullable(true),
            Schema.Field.of("ID", Schema.FieldType.INT32).withNullable(true));

    assertEquals(expectedSchema, rows.getSchema());

    PCollection<Row> output = rows.apply(Select.fieldNames("NAME", "ID"));
    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(Row.withSchema(expectedSchema).addValues("Testval1", 1).build()));

    pipeline.run();
  }

  @Test
  public void testReadRowsWithoutStatementPreparator() {
    SerializableFunction<Void, DataSource> dataSourceProvider = ignored -> dataSource;
    String name = TestRow.getNameForSeed(1);
    PCollection<Row> rows =
        pipeline.apply(
            JdbcIO.readRows()
                .withDataSourceProviderFn(dataSourceProvider)
                .withQuery(
                    String.format(
                        "select name,id from %s where name = '%s'", readTableName, name)));

    Schema expectedSchema =
        Schema.of(
            Schema.Field.of("NAME", LogicalTypes.variableLengthString(JDBCType.VARCHAR, 500))
                .withNullable(true),
            Schema.Field.of("ID", Schema.FieldType.INT32).withNullable(true));

    assertEquals(expectedSchema, rows.getSchema());

    PCollection<Row> output = rows.apply(Select.fieldNames("NAME", "ID"));
    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(Row.withSchema(expectedSchema).addValues(name, 1).build()));

    pipeline.run();
  }

  @Test
  public void testReadWithSchema() {
    SerializableFunction<Void, DataSource> dataSourceProvider = ignored -> dataSource;
    JdbcIO.RowMapper<RowWithSchema> rowMapper =
        rs -> new RowWithSchema(rs.getString("NAME"), rs.getInt("ID"));
    pipeline.getSchemaRegistry().registerJavaBean(RowWithSchema.class);

    PCollection<RowWithSchema> rows =
        pipeline.apply(
            JdbcIO.<RowWithSchema>read()
                .withDataSourceProviderFn(dataSourceProvider)
                .withQuery(String.format("select name,id from %s where name = ?", readTableName))
                .withRowMapper(rowMapper)
                .withCoder(SerializableCoder.of(RowWithSchema.class))
                .withStatementPreparator(
                    preparedStatement ->
                        preparedStatement.setString(1, TestRow.getNameForSeed(1))));

    Schema expectedSchema =
        Schema.of(
            Schema.Field.of("name", Schema.FieldType.STRING),
            Schema.Field.of("id", Schema.FieldType.INT32));

    assertEquals(expectedSchema, rows.getSchema());

    PCollection<Row> output = rows.apply(Select.fieldNames("name", "id"));
    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(Row.withSchema(expectedSchema).addValues("Testval1", 1).build()));

    pipeline.run();
  }

  @Test
  public void testWrite() throws Exception {
    final long rowsToAdd = 1000L;

    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE");
    DatabaseTestHelper.createTable(dataSource, tableName);
    try {
      ArrayList<KV<Integer, String>> data = getDataToWrite(rowsToAdd);
      pipeline.apply(Create.of(data)).apply(getJdbcWrite(tableName));

      pipeline.run();

      assertRowCount(tableName, EXPECTED_ROW_COUNT);
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }

  @Test
  public void testWriteWithResultsAndWaitOn() throws Exception {
    final long rowsToAdd = 1000L;

    String firstTableName = DatabaseTestHelper.getTestTableName("UT_WRITE");
    String secondTableName = DatabaseTestHelper.getTestTableName("UT_WRITE_AFTER_WAIT");
    DatabaseTestHelper.createTable(dataSource, firstTableName);
    DatabaseTestHelper.createTable(dataSource, secondTableName);
    try {
      ArrayList<KV<Integer, String>> data = getDataToWrite(rowsToAdd);

      PCollection<KV<Integer, String>> dataCollection = pipeline.apply(Create.of(data));
      PCollection<Void> rowsWritten =
          dataCollection.apply(getJdbcWrite(firstTableName).withResults());
      dataCollection.apply(Wait.on(rowsWritten)).apply(getJdbcWrite(secondTableName));

      pipeline.run();

      assertRowCount(firstTableName, EXPECTED_ROW_COUNT);
      assertRowCount(secondTableName, EXPECTED_ROW_COUNT);
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, firstTableName);
    }
  }

  private static JdbcIO.Write<KV<Integer, String>> getJdbcWrite(String tableName) {
    return JdbcIO.<KV<Integer, String>>write()
        .withDataSourceConfiguration(
            JdbcIO.DataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                "jdbc:derby://localhost:" + port + "/target/beam"))
        .withStatement(String.format("insert into %s values(?, ?)", tableName))
        .withBatchSize(10L)
        .withPreparedStatementSetter(
            (element, statement) -> {
              statement.setInt(1, element.getKey());
              statement.setString(2, element.getValue());
            });
  }

  private static ArrayList<KV<Integer, String>> getDataToWrite(long rowsToAdd) {
    ArrayList<KV<Integer, String>> data = new ArrayList<>();
    for (int i = 0; i < rowsToAdd; i++) {
      KV<Integer, String> kv = KV.of(i, "Test");
      data.add(kv);
    }
    return data;
  }

  private static void assertRowCount(String tableName, int expectedRowCount) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        try (ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName)) {
          resultSet.next();
          int count = resultSet.getInt(1);
          Assert.assertEquals(expectedRowCount, count);
        }
      }
    }
  }

  @Test
  public void testWriteWithBackoff() throws Exception {
    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE_BACKOFF");
    DatabaseTestHelper.createTable(dataSource, tableName);

    // lock table
    Connection connection = dataSource.getConnection();
    Statement lockStatement = connection.createStatement();
    lockStatement.execute("ALTER TABLE " + tableName + " LOCKSIZE TABLE");
    lockStatement.execute("LOCK TABLE " + tableName + " IN EXCLUSIVE MODE");

    // start a first transaction
    connection.setAutoCommit(false);
    PreparedStatement insertStatement =
        connection.prepareStatement("insert into " + tableName + " values(?, ?)");
    insertStatement.setInt(1, 1);
    insertStatement.setString(2, "TEST");
    insertStatement.execute();

    // try to write to this table
    pipeline
        .apply(Create.of(Collections.singletonList(KV.of(1, "TEST"))))
        .apply(
            JdbcIO.<KV<Integer, String>>write()
                .withDataSourceConfiguration(
                    JdbcIO.DataSourceConfiguration.create(
                        "org.apache.derby.jdbc.ClientDriver",
                        "jdbc:derby://localhost:" + port + "/target/beam"))
                .withStatement(String.format("insert into %s values(?, ?)", tableName))
                .withRetryStrategy(
                    (JdbcIO.RetryStrategy)
                        e -> {
                          return "XJ208"
                              .equals(e.getSQLState()); // we fake a deadlock with a lock here
                        })
                .withPreparedStatementSetter(
                    (element, statement) -> {
                      statement.setInt(1, element.getKey());
                      statement.setString(2, element.getValue());
                    }));

    // starting a thread to perform the commit later, while the pipeline is running into the backoff
    Thread commitThread =
        new Thread(
            () -> {
              try {
                Thread.sleep(10000);
                connection.commit();
              } catch (Exception e) {
                // nothing to do
              }
            });
    commitThread.start();
    pipeline.run();
    commitThread.join();

    // we verify the the backoff has been called thanks to the log message
    expectedLogs.verifyWarn("Deadlock detected, retrying");

    assertRowCount(tableName, 2);
  }

  @After
  public void tearDown() {
    try {
      DatabaseTestHelper.deleteTable(dataSource, BACKOFF_TABLE);
    } catch (Exception e) {
      // nothing to do
    }
  }

  @Test
  public void testWriteWithoutPreparedStatement() throws Exception {
    final int rowsToAdd = 10;

    Schema.Builder schemaBuilder = Schema.builder();
    schemaBuilder.addField(Schema.Field.of("column_boolean", Schema.FieldType.BOOLEAN));
    schemaBuilder.addField(Schema.Field.of("column_string", Schema.FieldType.STRING));
    schemaBuilder.addField(Schema.Field.of("column_int", Schema.FieldType.INT32));
    schemaBuilder.addField(Schema.Field.of("column_long", Schema.FieldType.INT64));
    schemaBuilder.addField(Schema.Field.of("column_float", Schema.FieldType.FLOAT));
    schemaBuilder.addField(Schema.Field.of("column_double", Schema.FieldType.DOUBLE));
    schemaBuilder.addField(Schema.Field.of("column_bigdecimal", Schema.FieldType.DECIMAL));
    schemaBuilder.addField(Schema.Field.of("column_date", LogicalTypes.JDBC_DATE_TYPE));
    schemaBuilder.addField(Schema.Field.of("column_time", LogicalTypes.JDBC_TIME_TYPE));
    schemaBuilder.addField(
        Schema.Field.of("column_timestamptz", LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE));
    schemaBuilder.addField(Schema.Field.of("column_timestamp", Schema.FieldType.DATETIME));
    schemaBuilder.addField(Schema.Field.of("column_short", Schema.FieldType.INT16));
    Schema schema = schemaBuilder.build();

    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE_PS");
    StringBuilder stmt = new StringBuilder("CREATE TABLE ");
    stmt.append(tableName);
    stmt.append(" (");
    stmt.append("column_boolean       BOOLEAN,"); // boolean
    stmt.append("column_string        VARCHAR(254),"); // String
    stmt.append("column_int           INTEGER,"); // int
    stmt.append("column_long          BIGINT,"); // long
    stmt.append("column_float         REAL,"); // float
    stmt.append("column_double        DOUBLE PRECISION,"); // double
    stmt.append("column_bigdecimal    DECIMAL(13,0),"); // BigDecimal
    stmt.append("column_date          DATE,"); // Date
    stmt.append("column_time          TIME,"); // Time
    stmt.append("column_timestamptz   TIMESTAMP,"); // Timestamp
    stmt.append("column_timestamp     TIMESTAMP,"); // Timestamp
    stmt.append("column_short         SMALLINT"); // short
    stmt.append(" )");
    DatabaseTestHelper.createTableWithStatement(dataSource, stmt.toString());
    try {
      ArrayList<Row> data = getRowsToWrite(rowsToAdd, schema);
      pipeline
          .apply(Create.of(data))
          .setRowSchema(schema)
          .apply(
              JdbcIO.<Row>write()
                  .withDataSourceConfiguration(
                      JdbcIO.DataSourceConfiguration.create(
                          "org.apache.derby.jdbc.ClientDriver",
                          "jdbc:derby://localhost:" + port + "/target/beam"))
                  .withBatchSize(10L)
                  .withTable(tableName));
      pipeline.run();
      assertRowCount(tableName, rowsToAdd);
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }

  @Test
  public void testWriteWithoutPreparedStatementWithReadRows() throws Exception {
    SerializableFunction<Void, DataSource> dataSourceProvider = ignored -> dataSource;
    PCollection<Row> rows =
        pipeline.apply(
            JdbcIO.readRows()
                .withDataSourceProviderFn(dataSourceProvider)
                .withQuery(String.format("select name,id from %s where name = ?", readTableName))
                .withStatementPreparator(
                    preparedStatement ->
                        preparedStatement.setString(1, TestRow.getNameForSeed(1))));

    String writeTableName = DatabaseTestHelper.getTestTableName("UT_WRITE_PS_WITH_READ_ROWS");
    DatabaseTestHelper.createTableForRowWithSchema(dataSource, writeTableName);
    try {
      rows.apply(
          JdbcIO.<Row>write()
              .withDataSourceConfiguration(
                  JdbcIO.DataSourceConfiguration.create(
                      "org.apache.derby.jdbc.ClientDriver",
                      "jdbc:derby://localhost:" + port + "/target/beam"))
              .withBatchSize(10L)
              .withTable(writeTableName));
      pipeline.run();
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, writeTableName);
    }
  }

  @Test
  public void testWriteWithoutPsWithNonNullableTableField() throws Exception {
    final int rowsToAdd = 10;

    Schema.Builder schemaBuilder = Schema.builder();
    schemaBuilder.addField(Schema.Field.of("column_boolean", Schema.FieldType.BOOLEAN));
    schemaBuilder.addField(Schema.Field.of("column_string", Schema.FieldType.STRING));
    Schema schema = schemaBuilder.build();

    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE");
    StringBuilder stmt = new StringBuilder("CREATE TABLE ");
    stmt.append(tableName);
    stmt.append(" (");
    stmt.append("column_boolean       BOOLEAN,");
    stmt.append("column_int           INTEGER NOT NULL");
    stmt.append(" )");
    DatabaseTestHelper.createTableWithStatement(dataSource, stmt.toString());
    try {
      ArrayList<Row> data = getRowsToWrite(rowsToAdd, schema);
      pipeline
          .apply(Create.of(data))
          .setRowSchema(schema)
          .apply(
              JdbcIO.<Row>write()
                  .withDataSourceConfiguration(
                      JdbcIO.DataSourceConfiguration.create(
                          "org.apache.derby.jdbc.ClientDriver",
                          "jdbc:derby://localhost:" + port + "/target/beam"))
                  .withBatchSize(10L)
                  .withTable(tableName));
      pipeline.run();
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
      thrown.expect(RuntimeException.class);
    }
  }

  @Test
  public void testWriteWithoutPreparedStatementAndNonRowType() throws Exception {
    final int rowsToAdd = 10;

    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE_PS_NON_ROW");
    DatabaseTestHelper.createTableForRowWithSchema(dataSource, tableName);
    try {
      List<RowWithSchema> data = getRowsWithSchemaToWrite(rowsToAdd);

      pipeline
          .apply(Create.of(data))
          .apply(
              JdbcIO.<RowWithSchema>write()
                  .withDataSourceConfiguration(
                      JdbcIO.DataSourceConfiguration.create(
                          "org.apache.derby.jdbc.ClientDriver",
                          "jdbc:derby://localhost:" + port + "/target/beam"))
                  .withBatchSize(10L)
                  .withTable(tableName));
      pipeline.run();
      assertRowCount(tableName, rowsToAdd);
    } finally {
      DatabaseTestHelper.deleteTable(dataSource, tableName);
    }
  }

  @Test
  public void testGetPreparedStatementSetCaller() throws Exception {

    Schema schema =
        Schema.builder()
            .addField("bigint_col", Schema.FieldType.INT64)
            .addField("binary_col", Schema.FieldType.BYTES)
            .addField("bit_col", Schema.FieldType.BOOLEAN)
            .addField("char_col", Schema.FieldType.STRING)
            .addField("decimal_col", Schema.FieldType.DECIMAL)
            .addField("double_col", Schema.FieldType.DOUBLE)
            .addField("float_col", Schema.FieldType.FLOAT)
            .addField("integer_col", Schema.FieldType.INT32)
            .addField("datetime_col", Schema.FieldType.DATETIME)
            .addField("int16_col", Schema.FieldType.INT16)
            .addField("byte_col", Schema.FieldType.BYTE)
            .build();
    Row row =
        Row.withSchema(schema)
            .addValues(
                42L,
                "binary".getBytes(Charset.forName("UTF-8")),
                true,
                "char",
                BigDecimal.valueOf(25L),
                20.5D,
                15.5F,
                10,
                new DateTime(),
                (short) 5,
                Byte.parseByte("1", 2))
            .build();

    PreparedStatement psMocked = mock(PreparedStatement.class);

    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.INT64)
        .set(row, psMocked, 0, SchemaUtil.FieldWithIndex.of(schema.getField(0), 0));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.BYTES)
        .set(row, psMocked, 1, SchemaUtil.FieldWithIndex.of(schema.getField(1), 1));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.BOOLEAN)
        .set(row, psMocked, 2, SchemaUtil.FieldWithIndex.of(schema.getField(2), 2));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.STRING)
        .set(row, psMocked, 3, SchemaUtil.FieldWithIndex.of(schema.getField(3), 3));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.DECIMAL)
        .set(row, psMocked, 4, SchemaUtil.FieldWithIndex.of(schema.getField(4), 4));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.DOUBLE)
        .set(row, psMocked, 5, SchemaUtil.FieldWithIndex.of(schema.getField(5), 5));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.FLOAT)
        .set(row, psMocked, 6, SchemaUtil.FieldWithIndex.of(schema.getField(6), 6));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.INT32)
        .set(row, psMocked, 7, SchemaUtil.FieldWithIndex.of(schema.getField(7), 7));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.DATETIME)
        .set(row, psMocked, 8, SchemaUtil.FieldWithIndex.of(schema.getField(8), 8));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.INT16)
        .set(row, psMocked, 9, SchemaUtil.FieldWithIndex.of(schema.getField(9), 9));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.BYTE)
        .set(row, psMocked, 10, SchemaUtil.FieldWithIndex.of(schema.getField(10), 10));

    verify(psMocked, times(1)).setLong(1, 42L);
    verify(psMocked, times(1)).setBytes(2, "binary".getBytes(Charset.forName("UTF-8")));
    verify(psMocked, times(1)).setBoolean(3, true);
    verify(psMocked, times(1)).setString(4, "char");
    verify(psMocked, times(1)).setBigDecimal(5, BigDecimal.valueOf(25L));
    verify(psMocked, times(1)).setDouble(6, 20.5D);
    verify(psMocked, times(1)).setFloat(7, 15.5F);
    verify(psMocked, times(1)).setInt(8, 10);
    verify(psMocked, times(1))
        .setTimestamp(9, new Timestamp(row.getDateTime("datetime_col").getMillis()));
    verify(psMocked, times(1)).setInt(10, (short) 5);
    verify(psMocked, times(1)).setByte(11, Byte.parseByte("1", 2));
  }

  @Test
  public void testGetPreparedStatementSetCallerForLogicalTypes() throws Exception {

    Schema schema =
        Schema.builder()
            .addField("logical_date_col", LogicalTypes.JDBC_DATE_TYPE)
            .addField("logical_time_col", LogicalTypes.JDBC_TIME_TYPE)
            .addField("logical_time_with_tz_col", LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE)
            .build();

    long epochMilli = 1558719710000L;
    DateTime dateTime = new DateTime(epochMilli, ISOChronology.getInstanceUTC());
    DateTime time =
        new DateTime(
            34567000L /* value must be less than num millis in one day */,
            ISOChronology.getInstanceUTC());

    Row row =
        Row.withSchema(schema).addValues(dateTime.withTimeAtStartOfDay(), time, dateTime).build();

    PreparedStatement psMocked = mock(PreparedStatement.class);

    JdbcUtil.getPreparedStatementSetCaller(LogicalTypes.JDBC_DATE_TYPE)
        .set(row, psMocked, 0, SchemaUtil.FieldWithIndex.of(schema.getField(0), 0));
    JdbcUtil.getPreparedStatementSetCaller(LogicalTypes.JDBC_TIME_TYPE)
        .set(row, psMocked, 1, SchemaUtil.FieldWithIndex.of(schema.getField(1), 1));
    JdbcUtil.getPreparedStatementSetCaller(LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE)
        .set(row, psMocked, 2, SchemaUtil.FieldWithIndex.of(schema.getField(2), 2));

    verify(psMocked, times(1)).setDate(1, new Date(row.getDateTime(0).getMillis()));
    verify(psMocked, times(1)).setTime(2, new Time(row.getDateTime(1).getMillis()));

    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTimeInMillis(epochMilli);

    verify(psMocked, times(1)).setTimestamp(3, new Timestamp(cal.getTime().getTime()), cal);
  }

  @Test
  public void testGetPreparedStatementSetCallerForArray() throws Exception {

    Schema schema =
        Schema.builder()
            .addField("string_array_col", Schema.FieldType.array(Schema.FieldType.STRING))
            .build();

    List<String> stringList = Arrays.asList("string 1", "string 2");

    Row row = Row.withSchema(schema).addValues(stringList).build();

    PreparedStatement psMocked = mock(PreparedStatement.class);
    Connection connectionMocked = mock(Connection.class);
    Array arrayMocked = mock(Array.class);

    when(psMocked.getConnection()).thenReturn(connectionMocked);
    when(connectionMocked.createArrayOf(anyString(), any())).thenReturn(arrayMocked);

    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.array(Schema.FieldType.STRING))
        .set(row, psMocked, 0, SchemaUtil.FieldWithIndex.of(schema.getField(0), 0));

    verify(psMocked, times(1)).setArray(1, arrayMocked);
  }

  private static ArrayList<Row> getRowsToWrite(long rowsToAdd, Schema schema) {

    ArrayList<Row> data = new ArrayList<>();
    for (int i = 0; i < rowsToAdd; i++) {
      List<Object> fields = new ArrayList<>();

      Row row =
          schema.getFields().stream()
              .map(field -> dummyFieldValue(field.getType()))
              .collect(Row.toRow(schema));
      data.add(row);
    }
    return data;
  }

  private static ArrayList<RowWithSchema> getRowsWithSchemaToWrite(long rowsToAdd) {

    ArrayList<RowWithSchema> data = new ArrayList<>();
    for (int i = 0; i < rowsToAdd; i++) {
      data.add(new RowWithSchema("Test", i));
    }
    return data;
  }

  private static Object dummyFieldValue(Schema.FieldType fieldType) {
    long epochMilli = 1558719710000L;
    if (fieldType.equals(Schema.FieldType.STRING)) {
      return "string value";
    } else if (fieldType.equals(Schema.FieldType.INT32)) {
      return 100;
    } else if (fieldType.equals(Schema.FieldType.DOUBLE)) {
      return 20.5D;
    } else if (fieldType.equals(Schema.FieldType.BOOLEAN)) {
      return Boolean.TRUE;
    } else if (fieldType.equals(Schema.FieldType.INT16)) {
      return Short.MAX_VALUE;
    } else if (fieldType.equals(Schema.FieldType.INT64)) {
      return Long.MAX_VALUE;
    } else if (fieldType.equals(Schema.FieldType.FLOAT)) {
      return 15.5F;
    } else if (fieldType.equals(Schema.FieldType.DECIMAL)) {
      return BigDecimal.ONE;
    } else if (fieldType.equals(LogicalTypes.JDBC_DATE_TYPE)) {
      return new DateTime(epochMilli, ISOChronology.getInstanceUTC()).withTimeAtStartOfDay();
    } else if (fieldType.equals(LogicalTypes.JDBC_TIME_TYPE)) {
      return new DateTime(epochMilli, ISOChronology.getInstanceUTC()).withDate(new LocalDate(0L));
    } else if (fieldType.equals(LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE)) {
      return new DateTime(epochMilli, ISOChronology.getInstanceUTC());
    } else if (fieldType.equals(Schema.FieldType.DATETIME)) {
      return new DateTime(epochMilli, ISOChronology.getInstanceUTC());
    } else {
      return null;
    }
  }

  @Test
  public void testWriteWithEmptyPCollection() {
    pipeline
        .apply(Create.empty(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of())))
        .apply(
            JdbcIO.<KV<Integer, String>>write()
                .withDataSourceConfiguration(
                    JdbcIO.DataSourceConfiguration.create(
                        "org.apache.derby.jdbc.ClientDriver",
                        "jdbc:derby://localhost:" + port + "/target/beam"))
                .withStatement("insert into BEAM values(?, ?)")
                .withPreparedStatementSetter(
                    (element, statement) -> {
                      statement.setInt(1, element.getKey());
                      statement.setString(2, element.getValue());
                    }));

    pipeline.run();
  }

  @Test
  public void testSerializationAndCachingOfPoolingDataSourceProvider() {
    SerializableFunction<Void, DataSource> provider =
        PoolableDataSourceProvider.of(
            JdbcIO.DataSourceConfiguration.create(
                "org.apache.derby.jdbc.ClientDriver",
                "jdbc:derby://localhost:" + port + "/target/beam"));
    SerializableFunction<Void, DataSource> deserializedProvider =
        SerializableUtils.ensureSerializable(provider);

    // Assert that that same instance is being returned even when there are multiple provider
    // instances with the same configuration. Also check that the deserialized provider was
    // able to produce an instance.
    assertSame(provider.apply(null), deserializedProvider.apply(null));
  }
}
