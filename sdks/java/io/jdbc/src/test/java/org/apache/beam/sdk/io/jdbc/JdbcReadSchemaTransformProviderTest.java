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

import static org.apache.beam.sdk.io.jdbc.JdbcUtil.JDBC_DRIVER_MAP;
import static org.apache.beam.sdk.io.jdbc.JdbcUtil.registerJdbcDriver;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;
import java.util.ServiceLoader;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.io.common.TestRow;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JdbcReadSchemaTransformProviderTest {

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

    registerJdbcDriver(
        ImmutableMap.of(
            "derby", Objects.requireNonNull(DATA_SOURCE_CONFIGURATION.getDriverClassName()).get()));

    DatabaseTestHelper.createTable(DATA_SOURCE, READ_TABLE_NAME);
    addInitialData(DATA_SOURCE, READ_TABLE_NAME);
  }

  @Test
  public void testInvalidReadSchemaOptions() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
              .setDriverClassName("")
              .setJdbcUrl("")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
              .setDriverClassName("ClassName")
              .setJdbcUrl("JdbcUrl")
              .setLocation("Location")
              .setReadQuery("Query")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
              .setDriverClassName("ClassName")
              .setJdbcUrl("JdbcUrl")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
              .setJdbcUrl("JdbcUrl")
              .setLocation("Location")
              .setJdbcType("invalidType")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
              .setJdbcUrl("JdbcUrl")
              .setLocation("Location")
              .build()
              .validate();
        });
  }

  @Test
  public void testValidReadSchemaOptions() {
    for (String jdbcType : JDBC_DRIVER_MAP.keySet()) {
      JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
          .setJdbcUrl("JdbcUrl")
          .setLocation("Location")
          .setJdbcType(jdbcType)
          .build()
          .validate();
    }
  }

  @Test
  public void testRead() {
    JdbcReadSchemaTransformProvider provider = null;
    for (SchemaTransformProvider p : ServiceLoader.load(SchemaTransformProvider.class)) {
      if (p instanceof JdbcReadSchemaTransformProvider) {
        provider = (JdbcReadSchemaTransformProvider) p;
        break;
      }
    }
    assertNotNull(provider);

    PCollection<Row> output =
        PCollectionRowTuple.empty(pipeline)
            .apply(
                provider.from(
                    JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
                        .setDriverClassName(DATA_SOURCE_CONFIGURATION.getDriverClassName().get())
                        .setJdbcUrl(DATA_SOURCE_CONFIGURATION.getUrl().get())
                        .setLocation(READ_TABLE_NAME)
                        .build()))
            .get("output");
    Long expected = Long.valueOf(EXPECTED_ROW_COUNT);
    PAssert.that(output.apply(Count.globally())).containsInAnyOrder(expected);
    pipeline.run();
  }

  @Test
  public void testReadWithJdbcTypeSpecified() {
    JdbcReadSchemaTransformProvider provider = null;
    for (SchemaTransformProvider p : ServiceLoader.load(SchemaTransformProvider.class)) {
      if (p instanceof JdbcReadSchemaTransformProvider) {
        provider = (JdbcReadSchemaTransformProvider) p;
        break;
      }
    }
    assertNotNull(provider);

    PCollection<Row> output =
        PCollectionRowTuple.empty(pipeline)
            .apply(
                provider.from(
                    JdbcReadSchemaTransformProvider.JdbcReadSchemaTransformConfiguration.builder()
                        .setJdbcUrl(DATA_SOURCE_CONFIGURATION.getUrl().get())
                        .setJdbcType("derby")
                        .setLocation(READ_TABLE_NAME)
                        .build()))
            .get("output");
    Long expected = Long.valueOf(EXPECTED_ROW_COUNT);
    PAssert.that(output.apply(Count.globally())).containsInAnyOrder(expected);
    pipeline.run();
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
