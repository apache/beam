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

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.sql.SQLException;
import java.util.List;
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
  private static final String WRITE_TABLE_NAME = DatabaseTestHelper.getTestTableName("UT_WRITE");

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @BeforeClass
  public static void beforeClass() throws Exception {
    // by default, derby uses a lock timeout of 60 seconds. In order to speed up the test
    // and detect the lock faster, we decrease this timeout
    System.setProperty("derby.locks.waitTimeout", "2");
    System.setProperty("derby.stream.error.file", "build/derby.log");

    DatabaseTestHelper.createTable(DATA_SOURCE, WRITE_TABLE_NAME);
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
  }

  @Test
  public void testReadWriteToTable() throws SQLException {
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
                    .setLocation(WRITE_TABLE_NAME)
                    .build()));
    pipeline.run();
    DatabaseTestHelper.assertRowCount(DATA_SOURCE, WRITE_TABLE_NAME, 2);
  }
}
