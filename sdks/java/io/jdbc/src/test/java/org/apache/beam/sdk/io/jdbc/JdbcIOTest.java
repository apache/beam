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

import static org.apache.beam.sdk.io.common.DatabaseTestHelper.assertRowCount;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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
import java.util.UUID;
import java.util.logging.LogRecord;
import javax.sql.DataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PoolableDataSourceProvider;
import org.apache.beam.sdk.io.jdbc.JdbcUtil.PartitioningFn;
import org.apache.beam.sdk.metrics.Lineage;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.FixedPrecisionNumeric;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.ExpectedLogs;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.LocalDate;
import org.joda.time.chrono.ISOChronology;
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
  private static final DataSourceConfiguration DATA_SOURCE_CONFIGURATION =
      DataSourceConfiguration.create(
          "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:testDB;create=true");
  private static final DataSource DATA_SOURCE = DATA_SOURCE_CONFIGURATION.buildDatasource();
  private static final int EXPECTED_ROW_COUNT = 1000;
  private static final String READ_TABLE_NAME = DatabaseTestHelper.getTestTableName("UT_READ");

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Rule public final transient TestPipeline secondPipeline = TestPipeline.create();

  @Rule public final transient ExpectedLogs expectedLogs = ExpectedLogs.none(JdbcIO.class);

  @Rule public transient ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");

    DatabaseTestHelper.createTable(DATA_SOURCE, READ_TABLE_NAME);
    addInitialData(DATA_SOURCE, READ_TABLE_NAME);
  }

  @Test
  public void testDataSourceConfigurationDataSourceWithoutPool() {
    assertThat(
        DATA_SOURCE_CONFIGURATION.buildDatasource(), not(instanceOf(PoolingDataSource.class)));
  }

  @Test
  public void testDataSourceConfigurationDataSourceWithPool() {
    assertTrue(
        JdbcIO.PoolableDataSourceProvider.of(DATA_SOURCE_CONFIGURATION).apply(null)
            instanceof PoolingDataSource);
  }

  @Test
  public void testDataSourceConfigurationDriverAndUrl() throws Exception {
    try (Connection conn = DATA_SOURCE_CONFIGURATION.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationUsernameAndPassword() throws Exception {
    String username = "sa";
    String password = "sa";
    JdbcIO.DataSourceConfiguration config =
        DATA_SOURCE_CONFIGURATION.withUsername(username).withPassword(password);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationNullPassword() throws Exception {
    String username = "sa";
    String password = null;
    JdbcIO.DataSourceConfiguration config =
        DATA_SOURCE_CONFIGURATION.withUsername(username).withPassword(password);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testDataSourceConfigurationNullUsernameAndPassword() throws Exception {
    String username = null;
    String password = null;
    JdbcIO.DataSourceConfiguration config =
        DATA_SOURCE_CONFIGURATION.withUsername(username).withPassword(password);
    try (Connection conn = config.buildDatasource().getConnection()) {
      assertTrue(conn.isValid(0));
    }
  }

  @Test
  public void testSetConnectoinInitSqlFailWithDerbyDB() {
    String username = "sa";
    String password = "sa";
    JdbcIO.DataSourceConfiguration config =
        DATA_SOURCE_CONFIGURATION
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
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withQuery("select name,id from " + READ_TABLE_NAME)
                .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(rows.apply("Count All", Count.globally()))
        .isEqualTo((long) EXPECTED_ROW_COUNT);

    Iterable<TestRow> expectedValues = TestRow.getExpectedValues(0, EXPECTED_ROW_COUNT);
    PAssert.that(rows).containsInAnyOrder(expectedValues);

    PipelineResult result = pipeline.run();
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SOURCE),
        hasItem(Lineage.getFqName("derby", ImmutableList.of("memory", "testDB", READ_TABLE_NAME))));
  }

  @Test
  public void testReadWithSingleStringParameter() {
    PCollection<TestRow> rows =
        pipeline.apply(
            JdbcIO.<TestRow>read()
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withQuery(String.format("select name,id from %s where name = ?", READ_TABLE_NAME))
                .withStatementPreparator(
                    preparedStatement -> preparedStatement.setString(1, TestRow.getNameForSeed(1)))
                .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                .withCoder(SerializableCoder.of(TestRow.class)));

    PAssert.thatSingleton(rows.apply("Count All", Count.globally())).isEqualTo(1L);

    Iterable<TestRow> expectedValues = Collections.singletonList(TestRow.fromSeed(1));
    PAssert.that(rows).containsInAnyOrder(expectedValues);

    PipelineResult result = pipeline.run();
    assertThat(
        Lineage.query(result.metrics(), Lineage.Type.SOURCE),
        hasItem(Lineage.getFqName("derby", ImmutableList.of("memory", "testDB", READ_TABLE_NAME))));
  }

  @Test
  public void testReadWithCoderInference() {
    PCollection<TestRow> rows =
        pipeline.apply(
            JdbcIO.<TestRow>read()
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withQuery(String.format("select name,id from %s where name = ?", READ_TABLE_NAME))
                .withStatementPreparator(
                    preparedStatement -> preparedStatement.setString(1, TestRow.getNameForSeed(1)))
                .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId()));

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
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withQuery(String.format("select name,id from %s where name = ?", READ_TABLE_NAME))
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
  public void testReadRowsWithNumericFields() {
    PCollection<Row> rows =
        pipeline.apply(
            JdbcIO.readRows()
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withQuery(
                    String.format(
                        "SELECT CAST(1 AS NUMERIC(1, 0)) AS T1 FROM %s WHERE name = ?",
                        READ_TABLE_NAME))
                .withStatementPreparator(
                    preparedStatement ->
                        preparedStatement.setString(1, TestRow.getNameForSeed(1))));

    Schema expectedSchema =
        Schema.of(
            Schema.Field.of(
                "T1", FieldType.logicalType(FixedPrecisionNumeric.of(1, 0)).withNullable(false)));

    assertEquals(expectedSchema, rows.getSchema());

    PCollection<Row> output = rows.apply(Select.fieldNames("T1"));
    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(
                Row.withSchema(expectedSchema).addValues(BigDecimal.valueOf(1)).build()));

    pipeline.run();
  }

  @Test
  @SuppressWarnings({"UnusedVariable", "AssertThrowsMultipleStatements"})
  public void testReadRowsFailedToGetSchema() {
    Exception exc =
        assertThrows(
            BeamSchemaInferenceException.class,
            () -> {
              // Using a new pipeline object to avoid the various checks made by TestPipeline in
              // this pipeline which is
              // expected to throw an exception.
              Pipeline pipeline = Pipeline.create();
              pipeline.apply(
                  JdbcIO.readRows()
                      .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                      .withQuery(
                          String.format(
                              "SELECT CAST(1 AS NUMERIC(1, 0)) AS T1 FROM %s", "unknown_table")));
              pipeline.run();
            });

    assertThat(exc.getMessage(), containsString("Failed to infer Beam schema"));
  }

  @Test
  public void testReadRowsWithNumericFieldsWithExcessPrecision() {
    PCollection<Row> rows =
        pipeline.apply(
            JdbcIO.readRows()
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withQuery(
                    String.format(
                        "SELECT CAST(1 AS NUMERIC(10, 2)) AS T1 FROM %s WHERE name = ?",
                        READ_TABLE_NAME))
                .withStatementPreparator(
                    preparedStatement ->
                        preparedStatement.setString(1, TestRow.getNameForSeed(1))));

    Schema expectedSchema =
        Schema.of(
            Schema.Field.of(
                "T1", FieldType.logicalType(FixedPrecisionNumeric.of(10, 2)).withNullable(false)));

    assertEquals(expectedSchema, rows.getSchema());

    PCollection<Row> output = rows.apply(Select.fieldNames("T1"));
    PAssert.that(output)
        .containsInAnyOrder(
            ImmutableList.of(
                Row.withSchema(expectedSchema)
                    .addValues(BigDecimal.valueOf(1).setScale(2, RoundingMode.HALF_UP))
                    .build()));

    pipeline.run();
  }

  @Test
  public void testReadRowsWithoutStatementPreparator() {
    SerializableFunction<Void, DataSource> dataSourceProvider = ignored -> DATA_SOURCE;
    String name = TestRow.getNameForSeed(1);
    PCollection<Row> rows =
        pipeline.apply(
            JdbcIO.readRows()
                .withDataSourceProviderFn(dataSourceProvider)
                .withQuery(
                    String.format(
                        "select name,id from %s where name = '%s'", READ_TABLE_NAME, name)));

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
    SerializableFunction<Void, DataSource> dataSourceProvider = ignored -> DATA_SOURCE;
    JdbcIO.RowMapper<RowWithSchema> rowMapper =
        rs -> new RowWithSchema(rs.getString("NAME"), rs.getInt("ID"));
    pipeline.getSchemaRegistry().registerJavaBean(RowWithSchema.class);

    PCollection<RowWithSchema> rows =
        pipeline.apply(
            JdbcIO.<RowWithSchema>read()
                .withDataSourceProviderFn(dataSourceProvider)
                .withQuery(String.format("select name,id from %s where name = ?", READ_TABLE_NAME))
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
  public void testReadWithPartitions() {
    PCollection<TestRow> rows =
        pipeline.apply(
            JdbcIO.<TestRow>readWithPartitions()
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                .withTable(READ_TABLE_NAME)
                .withNumPartitions(1)
                .withPartitionColumn("id")
                .withLowerBound(0L)
                .withUpperBound(1000L));
    PAssert.thatSingleton(rows.apply("Count All", Count.globally())).isEqualTo(1000L);
    pipeline.run();
  }

  @Test
  public void testReadWithPartitionsBySubqery() {
    PCollection<TestRow> rows =
        pipeline.apply(
            JdbcIO.<TestRow>readWithPartitions()
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                .withTable(String.format("(select * from %s) as subq", READ_TABLE_NAME))
                .withNumPartitions(10)
                .withPartitionColumn("id")
                .withLowerBound(0L)
                .withUpperBound(1000L));
    PAssert.thatSingleton(rows.apply("Count All", Count.globally())).isEqualTo(1000L);
    pipeline.run();
  }

  @Test
  public void testIfNumPartitionsIsZero() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("numPartitions can not be less than 1");
    pipeline.apply(
        JdbcIO.<TestRow>readWithPartitions()
            .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
            .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
            .withTable(READ_TABLE_NAME)
            .withNumPartitions(0)
            .withPartitionColumn("id")
            .withLowerBound(0L)
            .withUpperBound(1000L));
    pipeline.run();
  }

  @Test
  public void testLowerBoundIsMoreThanUpperBound() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(
        "The lower bound of partitioning column is larger or equal than the upper bound");
    pipeline.apply(
        JdbcIO.<TestRow>readWithPartitions()
            .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
            .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
            .withTable(READ_TABLE_NAME)
            .withNumPartitions(5)
            .withPartitionColumn("id")
            .withLowerBound(100L)
            .withUpperBound(100L));
    pipeline.run();
  }

  @Test
  public void testWrite() throws Exception {
    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE");
    DatabaseTestHelper.createTable(DATA_SOURCE, tableName);
    try {
      ArrayList<KV<Integer, String>> data = getDataToWrite(EXPECTED_ROW_COUNT);
      pipeline.apply(Create.of(data)).apply(getJdbcWrite(tableName));

      PipelineResult result = pipeline.run();
      assertRowCount(DATA_SOURCE, tableName, EXPECTED_ROW_COUNT);
      assertThat(
          Lineage.query(result.metrics(), Lineage.Type.SINK),
          hasItem(Lineage.getFqName("derby", ImmutableList.of("memory", "testDB", tableName))));
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, tableName);
    }
  }

  @Test
  public void testWriteWithAutosharding() throws Exception {
    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE");
    DatabaseTestHelper.createTable(DATA_SOURCE, tableName);
    TestStream.Builder<KV<Integer, String>> ts =
        TestStream.create(KvCoder.of(VarIntCoder.of(), StringUtf8Coder.of()))
            .advanceWatermarkTo(Instant.now());

    try {
      List<KV<Integer, String>> data = getDataToWrite(EXPECTED_ROW_COUNT);
      for (KV<Integer, String> elm : data) {
        ts = ts.addElements(elm);
      }
      pipeline
          .apply(ts.advanceWatermarkToInfinity())
          .apply(getJdbcWrite(tableName).withAutoSharding());

      pipeline.run().waitUntilFinish();

      assertRowCount(DATA_SOURCE, tableName, EXPECTED_ROW_COUNT);
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, tableName);
    }
  }

  @Test
  public void testWriteWithWriteResults() throws Exception {
    String firstTableName = DatabaseTestHelper.getTestTableName("UT_WRITE");
    DatabaseTestHelper.createTable(DATA_SOURCE, firstTableName);
    try {
      ArrayList<KV<Integer, String>> data = getDataToWrite(EXPECTED_ROW_COUNT);

      PCollection<KV<Integer, String>> dataCollection = pipeline.apply(Create.of(data));
      PCollection<JdbcTestHelper.TestDto> resultSetCollection =
          dataCollection.apply(
              getJdbcWrite(firstTableName)
                  .withWriteResults(
                      resultSet -> {
                        if (resultSet != null && resultSet.next()) {
                          return new JdbcTestHelper.TestDto(resultSet.getInt(1));
                        }
                        return new JdbcTestHelper.TestDto(JdbcTestHelper.TestDto.EMPTY_RESULT);
                      }));
      resultSetCollection.setCoder(JdbcTestHelper.TEST_DTO_CODER);

      PAssert.thatSingleton(resultSetCollection.apply(Count.globally()))
          .isEqualTo((long) EXPECTED_ROW_COUNT);

      List<JdbcTestHelper.TestDto> expectedResult = new ArrayList<>();
      for (int i = 0; i < EXPECTED_ROW_COUNT; i++) {
        expectedResult.add(new JdbcTestHelper.TestDto(JdbcTestHelper.TestDto.EMPTY_RESULT));
      }

      PAssert.that(resultSetCollection).containsInAnyOrder(expectedResult);

      pipeline.run();

      assertRowCount(DATA_SOURCE, firstTableName, EXPECTED_ROW_COUNT);
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, firstTableName);
    }
  }

  @Test
  public void testWriteWithResultsAndWaitOn() throws Exception {
    String firstTableName = DatabaseTestHelper.getTestTableName("UT_WRITE");
    String secondTableName = DatabaseTestHelper.getTestTableName("UT_WRITE_AFTER_WAIT");
    DatabaseTestHelper.createTable(DATA_SOURCE, firstTableName);
    DatabaseTestHelper.createTable(DATA_SOURCE, secondTableName);
    try {
      ArrayList<KV<Integer, String>> data = getDataToWrite(EXPECTED_ROW_COUNT);

      PCollection<KV<Integer, String>> dataCollection = pipeline.apply(Create.of(data));
      PCollection<Void> rowsWritten =
          dataCollection.apply(getJdbcWrite(firstTableName).withResults());
      dataCollection.apply(Wait.on(rowsWritten)).apply(getJdbcWrite(secondTableName));

      pipeline.run();

      assertRowCount(DATA_SOURCE, firstTableName, EXPECTED_ROW_COUNT);
      assertRowCount(DATA_SOURCE, secondTableName, EXPECTED_ROW_COUNT);
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, firstTableName);
    }
  }

  private static JdbcIO.Write<KV<Integer, String>> getJdbcWrite(String tableName) {
    return JdbcIO.<KV<Integer, String>>write()
        .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
        .withStatement(String.format("insert into %s values(?, ?)", tableName))
        .withBatchSize(10L)
        .withPreparedStatementSetter(
            (element, statement) -> {
              statement.setInt(1, element.getKey());
              statement.setString(2, element.getValue());
            });
  }

  private static JdbcIO.Write<Row> getJdbcWriteWithoutStatement(String tableName) {
    return JdbcIO.<Row>write()
        .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
        .withBatchSize(10L)
        .withTable(tableName);
  }

  private static ArrayList<KV<Integer, String>> getDataToWrite(long rowsToAdd) {
    ArrayList<KV<Integer, String>> data = new ArrayList<>();
    for (int i = 0; i < rowsToAdd; i++) {
      KV<Integer, String> kv = KV.of(i, "Test");
      data.add(kv);
    }
    return data;
  }

  @Test
  public void testWriteWithBackoff() throws Exception {
    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE_BACKOFF");
    DatabaseTestHelper.createTable(DATA_SOURCE, tableName);

    // lock table
    final Connection connection = DATA_SOURCE.getConnection();
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
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withStatement(String.format("insert into %s values(?, ?)", tableName))
                .withRetryStrategy(
                    (JdbcIO.RetryStrategy)
                        e -> {
                          return "40XL1"
                              .equals(e.getSQLState()); // we fake a deadlock with a lock here
                        })
                .withPreparedStatementSetter(
                    (element, statement) -> {
                      statement.setInt(1, element.getKey());
                      statement.setString(2, element.getValue());
                    }));

    // starting a thread to perform the commit later, while the pipeline is running into the backoff
    final Thread commitThread =
        new Thread(
            () -> {
              while (true) {
                try {
                  Thread.sleep(500);
                  expectedLogs.verifyWarn("Deadlock detected, retrying");
                  break;
                } catch (AssertionError | java.lang.InterruptedException e) {
                  // nothing to do
                }
              }
              try {
                connection.commit();
              } catch (Exception e) {
                // nothing to do.
              }
            });

    commitThread.start();
    PipelineResult result = pipeline.run();
    commitThread.join();
    result.waitUntilFinish();

    // we verify that the backoff has been called thanks to the log message
    expectedLogs.verifyWarn("Deadlock detected, retrying");

    assertRowCount(DATA_SOURCE, tableName, 2);
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
    schemaBuilder.addField(Schema.Field.of("column_blob", FieldType.BYTES));
    schemaBuilder.addField(Schema.Field.of("column_clob", FieldType.STRING));
    schemaBuilder.addField(Schema.Field.of("column_uuid", LogicalTypes.JDBC_UUID_TYPE));
    Schema schema = schemaBuilder.build();

    try (Connection connection = DATA_SOURCE.getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute("CREATE TYPE UUID EXTERNAL NAME 'java.util.UUID' LANGUAGE JAVA");
      }
    }

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
    stmt.append("column_short         SMALLINT,"); // short
    stmt.append("column_blob          BLOB,"); // blob
    stmt.append("column_clob          CLOB,"); // clob
    stmt.append("column_uuid          UUID"); // uuid
    stmt.append(" )");
    DatabaseTestHelper.createTableWithStatement(DATA_SOURCE, stmt.toString());
    try {
      ArrayList<Row> data = getRowsToWrite(rowsToAdd, schema);
      pipeline
          .apply(Create.of(data))
          .setRowSchema(schema)
          .apply(
              JdbcIO.<Row>write()
                  .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                  .withBatchSize(10L)
                  .withTable(tableName));
      pipeline.run();
      assertRowCount(DATA_SOURCE, tableName, rowsToAdd);
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, tableName);
    }
  }

  @Test
  public void testWriteWithoutPreparedStatementWithReadRows() throws Exception {
    SerializableFunction<Void, DataSource> dataSourceProvider = ignored -> DATA_SOURCE;
    PCollection<Row> rows =
        pipeline.apply(
            JdbcIO.readRows()
                .withDataSourceProviderFn(dataSourceProvider)
                .withQuery(String.format("select name,id from %s where name = ?", READ_TABLE_NAME))
                .withStatementPreparator(
                    preparedStatement ->
                        preparedStatement.setString(1, TestRow.getNameForSeed(1))));

    String writeTableName = DatabaseTestHelper.getTestTableName("UT_WRITE_PS_WITH_READ_ROWS");
    DatabaseTestHelper.createTable(DATA_SOURCE, writeTableName);
    try {
      rows.apply(
          JdbcIO.<Row>write()
              .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
              .withBatchSize(10L)
              .withTable(writeTableName));
      pipeline.run();
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, writeTableName);
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
    DatabaseTestHelper.createTableWithStatement(DATA_SOURCE, stmt.toString());
    try {
      ArrayList<Row> data = getRowsToWrite(rowsToAdd, schema);
      pipeline
          .apply(Create.of(data))
          .setRowSchema(schema)
          .apply(
              JdbcIO.<Row>write()
                  .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                  .withBatchSize(10L)
                  .withTable(tableName));
      pipeline.run();
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, tableName);
      thrown.expect(RuntimeException.class);
      thrown.expectMessage("Non nullable fields are not allowed without a matching schema.");
    }
  }

  @Test
  public void testWriteWithoutPreparedStatementAndNonRowType() throws Exception {
    final int rowsToAdd = 10;

    String tableName = DatabaseTestHelper.getTestTableName("UT_WRITE_PS_NON_ROW");
    DatabaseTestHelper.createTable(DATA_SOURCE, tableName);
    try {
      List<RowWithSchema> data = getRowsWithSchemaToWrite(rowsToAdd);

      pipeline
          .apply(Create.of(data))
          .apply(
              JdbcIO.<RowWithSchema>write()
                  .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                  .withBatchSize(10L)
                  .withTable(tableName));
      pipeline.run();
      assertRowCount(DATA_SOURCE, tableName, rowsToAdd);
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, tableName);
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
  public void testGetPreparedStatementSetNullsCaller() throws Exception {

    Schema schema =
        Schema.builder()
            // primitive
            .addField("bigint_col", Schema.FieldType.INT64.withNullable(true))
            .addField("bit_col", Schema.FieldType.BOOLEAN.withNullable(true))
            .addField("double_col", Schema.FieldType.DOUBLE.withNullable(true))
            .addField("float_col", Schema.FieldType.FLOAT.withNullable(true))
            .addField("integer_col", Schema.FieldType.INT32.withNullable(true))
            .addField("int16_col", Schema.FieldType.INT16.withNullable(true))
            .addField("byte_col", Schema.FieldType.BYTE.withNullable(true))
            // reference
            .addField("binary_col", Schema.FieldType.BYTES.withNullable(true))
            .addField("char_col", Schema.FieldType.STRING.withNullable(true))
            .addField("decimal_col", Schema.FieldType.DECIMAL.withNullable(true))
            .addField("datetime_col", Schema.FieldType.DATETIME.withNullable(true))
            .build();
    Row row =
        Row.withSchema(schema)
            .addValues(null, null, null, null, null, null, null, null, null, null, null)
            .build();

    PreparedStatement psMocked = mock(PreparedStatement.class);

    // primitive
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.INT64)
        .set(row, psMocked, 0, SchemaUtil.FieldWithIndex.of(schema.getField(0), 0));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.BOOLEAN)
        .set(row, psMocked, 1, SchemaUtil.FieldWithIndex.of(schema.getField(2), 2));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.DOUBLE)
        .set(row, psMocked, 2, SchemaUtil.FieldWithIndex.of(schema.getField(5), 5));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.FLOAT)
        .set(row, psMocked, 3, SchemaUtil.FieldWithIndex.of(schema.getField(6), 6));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.INT32)
        .set(row, psMocked, 4, SchemaUtil.FieldWithIndex.of(schema.getField(7), 7));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.INT16)
        .set(row, psMocked, 5, SchemaUtil.FieldWithIndex.of(schema.getField(9), 9));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.BYTE)
        .set(row, psMocked, 6, SchemaUtil.FieldWithIndex.of(schema.getField(10), 10));
    // reference
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.BYTES)
        .set(row, psMocked, 7, SchemaUtil.FieldWithIndex.of(schema.getField(1), 1));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.STRING)
        .set(row, psMocked, 8, SchemaUtil.FieldWithIndex.of(schema.getField(3), 3));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.DECIMAL)
        .set(row, psMocked, 9, SchemaUtil.FieldWithIndex.of(schema.getField(4), 4));
    JdbcUtil.getPreparedStatementSetCaller(Schema.FieldType.DATETIME)
        .set(row, psMocked, 10, SchemaUtil.FieldWithIndex.of(schema.getField(8), 8));

    // primitive
    verify(psMocked, times(1)).setNull(1, JDBCType.BIGINT.getVendorTypeNumber());
    verify(psMocked, times(1)).setNull(2, JDBCType.BOOLEAN.getVendorTypeNumber());
    verify(psMocked, times(1)).setNull(3, JDBCType.DOUBLE.getVendorTypeNumber());
    verify(psMocked, times(1)).setNull(4, JDBCType.FLOAT.getVendorTypeNumber());
    verify(psMocked, times(1)).setNull(5, JDBCType.INTEGER.getVendorTypeNumber());
    verify(psMocked, times(1)).setNull(6, JDBCType.SMALLINT.getVendorTypeNumber());
    verify(psMocked, times(1)).setNull(7, JDBCType.TINYINT.getVendorTypeNumber());
    // reference
    verify(psMocked, times(1)).setBytes(8, null);
    verify(psMocked, times(1)).setString(9, null);
    verify(psMocked, times(1)).setBigDecimal(10, null);
    verify(psMocked, times(1)).setTimestamp(11, null);
  }

  @Test
  public void testGetPreparedStatementSetCallerForLogicalTypes() throws Exception {
    FieldType fixedLengthStringType = LogicalTypes.fixedLengthString(JDBCType.VARCHAR, 4);
    Schema schema =
        Schema.builder()
            .addField("logical_date_col", LogicalTypes.JDBC_DATE_TYPE)
            .addField("logical_time_col", LogicalTypes.JDBC_TIME_TYPE)
            .addField("logical_time_with_tz_col", LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE)
            .addField("logical_fixed_length_string_col", fixedLengthStringType)
            .addField(
                "logical_fixed_length_string_nullable_col",
                fixedLengthStringType.withNullable(true))
            .addField("logical_uuid_col", LogicalTypes.JDBC_UUID_TYPE)
            .addField("logical_other_col", LogicalTypes.OTHER_AS_STRING_TYPE)
            .build();

    long epochMilli = 1558719710000L;
    DateTime dateTime = new DateTime(epochMilli, ISOChronology.getInstanceUTC());
    DateTime time =
        new DateTime(
            34567000L /* value must be less than num millis in one day */,
            ISOChronology.getInstanceUTC());

    Row row =
        Row.withSchema(schema)
            .addValues(
                dateTime.withTimeAtStartOfDay(),
                time,
                dateTime,
                "Test",
                null,
                UUID.randomUUID(),
                "{}")
            .build();

    PreparedStatement psMocked = mock(PreparedStatement.class);

    JdbcUtil.getPreparedStatementSetCaller(LogicalTypes.JDBC_DATE_TYPE)
        .set(row, psMocked, 0, SchemaUtil.FieldWithIndex.of(schema.getField(0), 0));
    JdbcUtil.getPreparedStatementSetCaller(LogicalTypes.JDBC_TIME_TYPE)
        .set(row, psMocked, 1, SchemaUtil.FieldWithIndex.of(schema.getField(1), 1));
    JdbcUtil.getPreparedStatementSetCaller(LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE)
        .set(row, psMocked, 2, SchemaUtil.FieldWithIndex.of(schema.getField(2), 2));
    JdbcUtil.getPreparedStatementSetCaller(fixedLengthStringType)
        .set(row, psMocked, 3, SchemaUtil.FieldWithIndex.of(schema.getField(3), 3));
    JdbcUtil.getPreparedStatementSetCaller(fixedLengthStringType.withNullable(true))
        .set(row, psMocked, 4, SchemaUtil.FieldWithIndex.of(schema.getField(4), 4));
    JdbcUtil.getPreparedStatementSetCaller(LogicalTypes.JDBC_UUID_TYPE)
        .set(row, psMocked, 5, SchemaUtil.FieldWithIndex.of(schema.getField(5), 5));
    JdbcUtil.getPreparedStatementSetCaller(LogicalTypes.OTHER_AS_STRING_TYPE)
        .set(row, psMocked, 6, SchemaUtil.FieldWithIndex.of(schema.getField(6), 6));

    verify(psMocked, times(1)).setDate(1, new Date(row.getDateTime(0).getMillis()));
    verify(psMocked, times(1)).setTime(2, new Time(row.getDateTime(1).getMillis()));

    Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    cal.setTimeInMillis(epochMilli);

    verify(psMocked, times(1)).setTimestamp(3, new Timestamp(cal.getTime().getTime()), cal);
    verify(psMocked, times(1)).setString(4, row.getString(3));
    verify(psMocked, times(1)).setString(5, row.getString(4));
    verify(psMocked, times(1)).setObject(6, row.getLogicalTypeValue(5, UUID.class));
    verify(psMocked, times(1)).setObject(7, row.getString(6), java.sql.Types.OTHER);
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

  private static ArrayList<Row> getRowsToWrite(long rowsToAdd, Schema schema, boolean hasNulls) {

    ArrayList<Row> data = new ArrayList<>();
    int numFields = schema.getFields().size();
    for (int i = 0; i < rowsToAdd; i++) {
      Row.Builder builder = Row.withSchema(schema);
      for (int j = 0; j < numFields; j++) {
        if (hasNulls && i % numFields == j && schema.getField(j).getType().getNullable()) {
          builder.addValue(null);
        } else {
          builder.addValue(dummyFieldValue(schema.getField(j).getType()));
        }
      }
      data.add(builder.build());
    }
    return data;
  }

  private static ArrayList<Row> getRowsToWrite(long rowsToAdd, Schema schema) {
    return getRowsToWrite(rowsToAdd, schema, false);
  }

  private static ArrayList<Row> getNullableRowsToWrite(long rowsToAdd, Schema schema) {
    return getRowsToWrite(rowsToAdd, schema, true);
  }

  private static ArrayList<RowWithSchema> getRowsWithSchemaToWrite(long rowsToAdd) {

    ArrayList<RowWithSchema> data = new ArrayList<>();
    for (int i = 0; i < rowsToAdd; i++) {
      data.add(new RowWithSchema("Test", i));
    }
    return data;
  }

  private static Object dummyFieldValue(Schema.FieldType maybeNullableType) {
    Schema.FieldType fieldType = maybeNullableType.withNullable(false);
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
    } else if (fieldType.equals(Schema.FieldType.DECIMAL)
        || (fieldType.getLogicalType() != null
            && fieldType
                .getLogicalType()
                .getIdentifier()
                .equals(FixedPrecisionNumeric.IDENTIFIER))) {
      return BigDecimal.ONE;
    } else if (fieldType.equals(LogicalTypes.JDBC_DATE_TYPE)) {
      return new DateTime(epochMilli, ISOChronology.getInstanceUTC()).withTimeAtStartOfDay();
    } else if (fieldType.equals(LogicalTypes.JDBC_TIME_TYPE)) {
      return new DateTime(epochMilli, ISOChronology.getInstanceUTC()).withDate(new LocalDate(0L));
    } else if (fieldType.equals(LogicalTypes.JDBC_TIMESTAMP_WITH_TIMEZONE_TYPE)) {
      return new DateTime(epochMilli, ISOChronology.getInstanceUTC());
    } else if (fieldType.equals(Schema.FieldType.DATETIME)) {
      return new DateTime(epochMilli, ISOChronology.getInstanceUTC());
    } else if (fieldType.equals(FieldType.BYTES)) {
      return "bytes".getBytes(StandardCharsets.UTF_8);
    } else if (fieldType.equals(LogicalTypes.JDBC_UUID_TYPE)) {
      return UUID.randomUUID();
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
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
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
        PoolableDataSourceProvider.of(DATA_SOURCE_CONFIGURATION);
    SerializableFunction<Void, DataSource> deserializedProvider =
        SerializableUtils.ensureSerializable(provider);

    // Assert that that same instance is being returned even when there are multiple provider
    // instances with the same configuration. Also check that the deserialized provider was
    // able to produce an instance.
    assertSame(provider.apply(null), deserializedProvider.apply(null));
  }

  @Test
  public void testCustomFluentBackOffConfiguration() throws Exception {
    String tableName = DatabaseTestHelper.getTestTableName("UT_FLUENT_BACKOFF");
    DatabaseTestHelper.createTable(DATA_SOURCE, tableName);

    // lock table
    Connection connection = DATA_SOURCE.getConnection();
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

    pipeline
        .apply(Create.of(Collections.singletonList(KV.of(1, "TEST"))))
        .apply(
            JdbcIO.<KV<Integer, String>>write()
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withStatement(String.format("insert into %s values(?, ?)", tableName))
                .withRetryStrategy(
                    (JdbcIO.RetryStrategy)
                        e -> {
                          return "40XL1"
                              .equals(e.getSQLState()); // we fake a deadlock with a lock here
                        })
                .withRetryConfiguration(
                    JdbcIO.RetryConfiguration.create(2, null, Duration.standardSeconds(1)))
                .withPreparedStatementSetter(
                    (element, statement) -> {
                      statement.setInt(1, element.getKey());
                      statement.setString(2, element.getValue());
                    }));

    PipelineExecutionException exception =
        assertThrows(
            PipelineExecutionException.class,
            () -> {
              pipeline.run().waitUntilFinish();
            });

    // Finally commit the original connection, now that the pipeline has failed due to deadlock.
    connection.commit();

    assertThat(
        exception.getMessage(),
        containsString(
            "java.sql.BatchUpdateException: A lock could not be obtained within the time requested"));

    // Verify that pipeline retried the write twice, but encountered a deadlock every time.
    expectedLogs.verifyLogRecords(
        new TypeSafeMatcher<Iterable<LogRecord>>() {
          @Override
          public void describeTo(Description description) {}

          @Override
          protected boolean matchesSafely(Iterable<LogRecord> logRecords) {
            int count = 0;
            for (LogRecord logRecord : logRecords) {
              if (logRecord.getMessage().contains("Deadlock detected, retrying")) {
                count += 1;
              }
            }
            // Max retries will be 2 + the original deadlock error.
            return count == 3;
          }
        });

    // Since the pipeline was unable to write, only the row from insertStatement was written.
    assertRowCount(DATA_SOURCE, tableName, 1);
  }

  @Test
  public void testDefaultRetryStrategy() {
    final JdbcIO.RetryStrategy strategy = new JdbcIO.DefaultRetryStrategy();
    assertTrue(strategy.apply(new SQLException("SQL deadlock", "40001")));
    assertTrue(strategy.apply(new SQLException("PostgreSQL deadlock", "40P01")));
    assertFalse(strategy.apply(new SQLException("Other code", "40X01")));
  }

  @Test
  public void testWriteRowsResultsAndWaitOn() throws Exception {
    String firstTableName = DatabaseTestHelper.getTestTableName("UT_WRITE_ROWS_PS");
    String secondTableName = DatabaseTestHelper.getTestTableName("UT_WRITE_ROWS_PS_AFTER_WAIT");
    DatabaseTestHelper.createTable(DATA_SOURCE, firstTableName);
    DatabaseTestHelper.createTable(DATA_SOURCE, secondTableName);

    Schema.Builder schemaBuilder = Schema.builder();
    schemaBuilder.addField(Schema.Field.of("id", Schema.FieldType.INT32));
    schemaBuilder.addField(Schema.Field.of("name", Schema.FieldType.STRING));
    Schema schema = schemaBuilder.build();
    try {
      ArrayList<Row> data = getRowsToWrite(EXPECTED_ROW_COUNT, schema);

      PCollection<Row> dataCollection = pipeline.apply(Create.of(data));
      PCollection<Void> rowsWritten =
          dataCollection
              .setRowSchema(schema)
              .apply(getJdbcWriteWithoutStatement(firstTableName).withResults());
      dataCollection
          .apply(Wait.on(rowsWritten))
          .setRowSchema(schema) // setRowSchema must be after .apply(Wait.on())
          .apply(getJdbcWriteWithoutStatement(secondTableName));

      pipeline.run();

      assertRowCount(DATA_SOURCE, firstTableName, EXPECTED_ROW_COUNT);
      assertRowCount(DATA_SOURCE, secondTableName, EXPECTED_ROW_COUNT);
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, firstTableName);
      DatabaseTestHelper.deleteTable(DATA_SOURCE, secondTableName);
    }
  }

  @Test
  public void testPartitioningDateTime() {
    PCollection<KV<DateTime, DateTime>> ranges =
        pipeline
            .apply(Create.of(KV.of(10L, KV.of(new DateTime(0), DateTime.now()))))
            .apply(
                ParDo.of(
                    new PartitioningFn<>(
                        JdbcUtil.getPartitionsHelper(TypeDescriptor.of(DateTime.class)))));

    PAssert.that(ranges.apply(Count.globally()))
        .satisfies(
            new SerializableFunction<Iterable<Long>, Void>() {
              @Override
              public Void apply(Iterable<Long> input) {
                // We must have exactly least one element
                Long count = input.iterator().next();
                // The implementation for range partitioning relies on millis from epoch.
                // We allow off-by-one differences because we can have slight differences
                // in integers when computing strides, and advancing through timestamps.
                assertThat(Double.valueOf(count), closeTo(10, 1));
                return null;
              }
            });
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testWriteReadNullableTypes() throws SQLException {
    // first setup data
    Schema.Builder schemaBuilder = Schema.builder();
    schemaBuilder.addField("column_id", FieldType.INT32.withNullable(false));
    schemaBuilder.addField("column_bigint", Schema.FieldType.INT64.withNullable(true));
    schemaBuilder.addField("column_boolean", FieldType.BOOLEAN.withNullable(true));
    schemaBuilder.addField("column_float", Schema.FieldType.FLOAT.withNullable(true));
    schemaBuilder.addField("column_double", Schema.FieldType.DOUBLE.withNullable(true));
    schemaBuilder.addField(
        "column_decimal",
        FieldType.logicalType(FixedPrecisionNumeric.of(13, 0)).withNullable(true));
    Schema schema = schemaBuilder.build();

    // some types not supported in derby (e.g. tinyint) are not tested here
    String tableName = DatabaseTestHelper.getTestTableName("UT_READ_NULLABLE_LG");
    StringBuilder stmt = new StringBuilder("CREATE TABLE ");
    stmt.append(tableName);
    stmt.append(" (");
    stmt.append("column_id      INTEGER NOT NULL,"); // Integer
    stmt.append("column_bigint  BIGINT,"); // int64
    stmt.append("column_boolean BOOLEAN,"); // boolean
    stmt.append("column_float  REAL,"); // float
    stmt.append("column_double  DOUBLE PRECISION,"); // double
    stmt.append("column_decimal    DECIMAL(13,0)"); // BigDecimal
    stmt.append(" )");
    DatabaseTestHelper.createTableWithStatement(DATA_SOURCE, stmt.toString());
    final int rowsToAdd = 10;
    try {
      // run write pipeline
      ArrayList<Row> data = getNullableRowsToWrite(rowsToAdd, schema);
      pipeline
          .apply(Create.of(data))
          .setRowSchema(schema)
          .apply(
              JdbcIO.<Row>write()
                  .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                  .withBatchSize(10L)
                  .withTable(tableName));
      pipeline.run();
      assertRowCount(DATA_SOURCE, tableName, rowsToAdd);

      // run read pipeline
      PCollection<Row> rows =
          secondPipeline.apply(
              JdbcIO.readRows()
                  .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                  .withQuery("SELECT * FROM " + tableName));
      PAssert.thatSingleton(rows.apply("Count All", Count.globally())).isEqualTo((long) rowsToAdd);
      PAssert.that(rows).containsInAnyOrder(data);

      secondPipeline.run();
    } finally {
      DatabaseTestHelper.deleteTable(DATA_SOURCE, tableName);
    }
  }

  @Test
  public void testPartitioningLongs() {
    PCollection<KV<Long, Long>> ranges =
        pipeline
            .apply(Create.of(KV.of(10L, KV.of(0L, 12346789L))))
            .apply(
                ParDo.of(
                    new PartitioningFn<>(JdbcUtil.getPartitionsHelper(TypeDescriptors.longs()))));

    PAssert.that(ranges.apply(Count.globally())).containsInAnyOrder(10L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testPartitioningStringsWithCustomPartitionsHelper() {
    JdbcReadWithPartitionsHelper<String> helper =
        new JdbcReadWithPartitionsHelper<String>() {
          @Override
          public Iterable<KV<String, String>> calculateRanges(
              String lowerBound, String upperBound, Long partitions) {
            // we expect the elements in the test case follow the format <common prefix>idx
            String prefix = StringUtils.getCommonPrefix(lowerBound, upperBound);
            int minChar = lowerBound.charAt(prefix.length());
            int maxChar = upperBound.charAt(prefix.length());
            int numPartition;
            if (maxChar - minChar < partitions) {
              LOG.warn(
                  "Partition large than possible! Adjust to {} partition instead",
                  maxChar - minChar);
              numPartition = maxChar - minChar;
            } else {
              numPartition = Math.toIntExact(partitions);
            }
            List<KV<String, String>> ranges = new ArrayList<>();
            int stride = (maxChar - minChar) / numPartition + 1;
            int highest = minChar;
            for (int i = minChar; i < maxChar - stride; i += stride) {
              ranges.add(KV.of(prefix + (char) i, prefix + (char) (i + stride)));
              highest = i + stride;
            }
            if (highest <= maxChar) {
              ranges.add(KV.of(prefix + (char) highest, prefix + (char) (highest + stride)));
            }
            return ranges;
          }

          @Override
          public void setParameters(
              KV<String, String> element, PreparedStatement preparedStatement) {
            try {
              preparedStatement.setString(1, element.getKey());
              preparedStatement.setString(2, element.getValue());
            } catch (SQLException e) {
              throw new RuntimeException(e);
            }
          }

          @Override
          public KV<Long, KV<String, String>> mapRow(ResultSet resultSet) throws Exception {
            if (resultSet.getMetaData().getColumnCount() == 3) {
              return KV.of(
                  resultSet.getLong(3), KV.of(resultSet.getString(1), resultSet.getString(2)));
            } else {
              return KV.of(0L, KV.of(resultSet.getString(1), resultSet.getString(2)));
            }
          }
        };

    PCollection<TestRow> rows =
        pipeline.apply(
            JdbcIO.<TestRow, String>readWithPartitions(helper)
                .withDataSourceConfiguration(DATA_SOURCE_CONFIGURATION)
                .withRowMapper(new JdbcTestHelper.CreateTestRowOfNameAndId())
                .withTable(READ_TABLE_NAME)
                .withNumPartitions(5)
                .withPartitionColumn("name"));
    PAssert.thatSingleton(rows.apply("Count All", Count.globally())).isEqualTo(1000L);
    pipeline.run();
  }
}
