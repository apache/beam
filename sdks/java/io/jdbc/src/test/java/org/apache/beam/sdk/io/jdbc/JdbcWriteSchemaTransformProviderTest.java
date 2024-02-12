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

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.ServiceLoader;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.common.DatabaseTestHelper;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.SchemaTransformProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JdbcWriteSchemaTransformProviderTest {

  private static final JdbcIO.DataSourceConfiguration DATA_SOURCE_CONFIGURATION =
      JdbcIO.DataSourceConfiguration.create(
          "org.apache.derby.jdbc.EmbeddedDriver", "jdbc:derby:memory:testDB;create=true");
  private static final DataSource DATA_SOURCE = DATA_SOURCE_CONFIGURATION.buildDatasource();
  private String writeTableName;

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
  }

  @Before
  public void before() throws SQLException {
    writeTableName = DatabaseTestHelper.getTestTableName("UT_WRITE");
    DatabaseTestHelper.createTable(DATA_SOURCE, writeTableName);
  }

  @Test
  public void testInvalidWriteSchemaOptions() {
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
              .setDriverClassName("")
              .setJdbcUrl("")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
              .setDriverClassName("ClassName")
              .setJdbcUrl("JdbcUrl")
              .setLocation("Location")
              .setWriteStatement("WriteStatement")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
              .setDriverClassName("ClassName")
              .setJdbcUrl("JdbcUrl")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
              .setJdbcUrl("JdbcUrl")
              .setLocation("Location")
              .setJdbcType("invalidType")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
              .setJdbcUrl("JdbcUrl")
              .setLocation("Location")
              .build()
              .validate();
        });
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
              .setJdbcUrl("JdbcUrl")
              .setLocation("Location")
              .setDriverClassName("ClassName")
              .setJdbcType((String) JDBC_DRIVER_MAP.keySet().toArray()[0])
              .build()
              .validate();
        });
  }

  @Test
  public void testValidWriteSchemaOptions() {
    for (String jdbcType : JDBC_DRIVER_MAP.keySet()) {
      JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
          .setJdbcUrl("JdbcUrl")
          .setLocation("Location")
          .setJdbcType(jdbcType)
          .build()
          .validate();
    }
  }

  @Test
  public void testWriteToTable() throws SQLException {
    JdbcWriteSchemaTransformProvider provider = null;
    for (SchemaTransformProvider p : ServiceLoader.load(SchemaTransformProvider.class)) {
      if (p instanceof JdbcWriteSchemaTransformProvider) {
        provider = (JdbcWriteSchemaTransformProvider) p;
        break;
      }
    }
    assertNotNull(provider);

    Schema schema =
        Schema.of(
            Schema.Field.of("id", Schema.FieldType.INT64),
            Schema.Field.of("name", Schema.FieldType.STRING));

    List<Row> rows =
        ImmutableList.of(
            Row.withSchema(schema).attachValues(1L, "name1"),
            Row.withSchema(schema).attachValues(2L, "name2"));

    PCollectionRowTuple.of("input", pipeline.apply(Create.of(rows).withRowSchema(schema)))
        .apply(
            provider.from(
                JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
                    .setDriverClassName(DATA_SOURCE_CONFIGURATION.getDriverClassName().get())
                    .setJdbcUrl(DATA_SOURCE_CONFIGURATION.getUrl().get())
                    .setLocation(writeTableName)
                    .build()));
    pipeline.run();
    DatabaseTestHelper.assertRowCount(DATA_SOURCE, writeTableName, 2);
  }

  @Test
  public void testWriteToTableWithJdbcTypeSpecified() throws SQLException {
    JdbcWriteSchemaTransformProvider provider = null;
    for (SchemaTransformProvider p : ServiceLoader.load(SchemaTransformProvider.class)) {
      if (p instanceof JdbcWriteSchemaTransformProvider) {
        provider = (JdbcWriteSchemaTransformProvider) p;
        break;
      }
    }
    assertNotNull(provider);

    Schema schema =
        Schema.of(
            Schema.Field.of("id", Schema.FieldType.INT64),
            Schema.Field.of("name", Schema.FieldType.STRING));

    List<Row> rows =
        ImmutableList.of(
            Row.withSchema(schema).attachValues(1L, "name1"),
            Row.withSchema(schema).attachValues(2L, "name2"));

    PCollectionRowTuple.of("input", pipeline.apply(Create.of(rows).withRowSchema(schema)))
        .apply(
            provider.from(
                JdbcWriteSchemaTransformProvider.JdbcWriteSchemaTransformConfiguration.builder()
                    .setJdbcUrl(DATA_SOURCE_CONFIGURATION.getUrl().get())
                    .setJdbcType("derby")
                    .setLocation(writeTableName)
                    .build()));
    pipeline.run();
    DatabaseTestHelper.assertRowCount(DATA_SOURCE, writeTableName, 2);
  }
}
