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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JdbcSchemaIOProviderTest {

  private static final JdbcIO.DataSourceConfiguration DATA_SOURCE_CONFIGURATION =
      JdbcIO.DataSourceConfiguration.create(
          "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:testDB;create=true");
  private static final int EXPECTED_ROW_COUNT = 1000;

  private static final DataSource DATA_SOURCE = DATA_SOURCE_CONFIGURATION.buildDatasource();
  private static final String READ_TABLE_NAME = DatabaseTestHelper.getTestTableName("UT_READ");

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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
  public void testPartitionedRead() {
    JdbcSchemaIOProvider provider = new JdbcSchemaIOProvider();

    Row config =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("driverClassName", DATA_SOURCE_CONFIGURATION.getDriverClassName().get())
            .withFieldValue("jdbcUrl", DATA_SOURCE_CONFIGURATION.getUrl().get())
            .withFieldValue("username", "")
            .withFieldValue("password", "")
            .withFieldValue("partitionColumn", "id")
            .withFieldValue("partitions", (short) 10)
            .build();
    JdbcSchemaIOProvider.JdbcSchemaIO schemaIO =
        provider.from(READ_TABLE_NAME, config, Schema.builder().build());
    PCollection<Row> output = pipeline.apply(schemaIO.buildReader());
    Long expected = Long.valueOf(EXPECTED_ROW_COUNT);
    PAssert.that(output.apply(Count.globally())).containsInAnyOrder(expected);
    pipeline.run();
  }

  // This test shouldn't work because we only support numeric and datetime columns and we are trying
  // to use a string column as our partition source.
  @Test
  public void testPartitionedReadThatShouldntWork() throws Exception {
    JdbcSchemaIOProvider provider = new JdbcSchemaIOProvider();

    Row config =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("driverClassName", DATA_SOURCE_CONFIGURATION.getDriverClassName().get())
            .withFieldValue("jdbcUrl", DATA_SOURCE_CONFIGURATION.getUrl().get())
            .withFieldValue("username", "")
            .withFieldValue("password", "")
            .withFieldValue("partitionColumn", "name")
            .withFieldValue("partitions", (short) 10)
            .build();
    JdbcSchemaIOProvider.JdbcSchemaIO schemaIO =
        provider.from(READ_TABLE_NAME, config, Schema.builder().build());
    PCollection<Row> output = pipeline.apply(schemaIO.buildReader());
    Long expected = Long.valueOf(EXPECTED_ROW_COUNT);
    PAssert.that(output.apply(Count.globally())).containsInAnyOrder(expected);
    try {
      pipeline.run();
    } catch (Exception e) {
      e.printStackTrace();
      return;
    }
    throw new Exception("Did not throw an exception");
  }

  // This test verifies we can read back existing data source configuration. It also serves as
  // sanity check that the
  // getDataSourceConfiguration should keep in sync with JdbcSchemaIOProvider.configurationSchema.
  // Otherwise the test
  // would throw an exception.
  @Test
  public void testAbleToReadDataSourceConfiguration() {
    JdbcSchemaIOProvider provider = new JdbcSchemaIOProvider();

    Row config =
        Row.withSchema(provider.configurationSchema())
            .withFieldValue("driverClassName", "className")
            .withFieldValue("jdbcUrl", "url")
            .withFieldValue("username", "user")
            .withFieldValue("password", "passwd")
            .withFieldValue("connectionProperties", "connectionProp")
            .withFieldValue("connectionInitSqls", new ArrayList<>(Collections.singleton("initSql")))
            .withFieldValue("maxConnections", (short) 3)
            .withFieldValue("driverJars", "test.jar")
            .withFieldValue("writeBatchSize", 10L)
            .build();
    JdbcSchemaIOProvider.JdbcSchemaIO schemaIO =
        provider.from(READ_TABLE_NAME, config, Schema.builder().build());
    JdbcIO.DataSourceConfiguration dataSourceConf = schemaIO.getDataSourceConfiguration();
    assertEquals("className", Objects.requireNonNull(dataSourceConf.getDriverClassName()).get());
    assertEquals("url", Objects.requireNonNull(dataSourceConf.getUrl()).get());
    assertEquals("user", Objects.requireNonNull(dataSourceConf.getUsername()).get());
    assertEquals("passwd", Objects.requireNonNull(dataSourceConf.getPassword()).get());
    assertEquals(
        "connectionProp", Objects.requireNonNull(dataSourceConf.getConnectionProperties()).get());
    assertEquals(
        new ArrayList<>(Collections.singleton("initSql")),
        Objects.requireNonNull(dataSourceConf.getConnectionInitSqls()).get());
    assertEquals(3, (int) dataSourceConf.getMaxConnections().get());
    assertEquals("test.jar", Objects.requireNonNull(dataSourceConf.getDriverJars()).get());
    assertEquals(10L, schemaIO.config.getInt64("writeBatchSize").longValue());
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
}
