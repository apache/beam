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
package org.apache.beam.sdk.extensions.sql.meta.provider.parquet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamPushDownIOSourceRel;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for ParquetTable. */
@RunWith(JUnit4.class)
public class ParquetTableProviderTest {
  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String FIELD_NAMES = "(name VARCHAR, age BIGINT, country VARCHAR)";
  private static final Schema TABLE_SCHEMA =
      Schema.builder()
          .addStringField("name")
          .addInt64Field("age")
          .addStringField("country")
          .build();

  private static final Row ROW_1 =
      Row.withSchema(TABLE_SCHEMA).addValues("Alan", 22L, "England").build();
  private static final Row ROW_2 =
      Row.withSchema(TABLE_SCHEMA).addValues("John", 42L, "USA").build();
  private static final List<Row> ALL_ROWS = Arrays.asList(ROW_1, ROW_2);

  private static final String ALL_TYPES_FIELDS =
      "(id INTEGER, ti TINYINT, si SMALLINT, bi BIGINT, f FLOAT, db DOUBLE, dc DECIMAL, b BOOLEAN,"
          + " d DATE, ts TIMESTAMP, vc VARCHAR, vb VARBINARY)";

  /** The values that {@link #writeAllTypesTable} writes to the row with id 1, by column. */
  private static final Map<String, Object> ALL_TYPES_VALUES =
      ImmutableMap.<String, Object>builder()
          .put("ti", (byte) 1)
          .put("si", (short) 2)
          .put("bi", 3L)
          .put("f", 1.5f)
          .put("db", 2.5d)
          .put("dc", new BigDecimal(4))
          .put("b", true)
          .put("d", LocalDate.of(2025, 7, 14))
          .put("ts", Instant.parse("2025-07-14T01:02:03Z"))
          .put("vc", "abc")
          .put("vb", new byte[] {1, 2})
          .build();

  private BeamSqlEnv env;

  @Before
  public void setUp() {
    env = BeamSqlEnv.inMemory(new ParquetTableProvider());
  }

  @Test
  public void testReadAndFilter() {
    File destinationDir = new File(tempFolder.getRoot(), "person-info");
    String locationPath = destinationDir.getAbsolutePath() + File.separator;

    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE PersonInfo %s TYPE parquet LOCATION '%s'",
            FIELD_NAMES, locationPath));

    BeamSqlRelUtils.toPCollection(
        writePipeline,
        env.parseQuery(
            "INSERT INTO PersonInfo VALUES ('Alan', 22, 'England'), ('John', 42, 'USA')"));
    writePipeline.run().waitUntilFinish();

    Schema projectedSchema = Schema.builder().addStringField("name").addInt64Field("age").build();
    PCollection<Row> filteredAndProjected =
        BeamSqlRelUtils.toPCollection(
            readPipeline, env.parseQuery("SELECT name, age FROM PersonInfo WHERE age > 25"));

    PAssert.that(filteredAndProjected)
        .containsInAnyOrder(Row.withSchema(projectedSchema).addValues("John", 42L).build());

    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testLocationPathConventions() {
    File destinationDir = new File(tempFolder.getRoot(), "path-test-data");
    String writeLocation = destinationDir.getAbsolutePath() + File.separator;
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE TmpWriteTable %s TYPE parquet LOCATION '%s'",
            FIELD_NAMES, writeLocation));
    BeamSqlRelUtils.toPCollection(
        writePipeline,
        env.parseQuery(
            "INSERT INTO TmpWriteTable VALUES ('Alan', 22, 'England'), ('John', 42, 'USA')"));
    writePipeline.run().waitUntilFinish();

    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE DirTable %s TYPE parquet LOCATION '%s'",
            FIELD_NAMES, writeLocation));
    PCollection<Row> dirResult =
        BeamSqlRelUtils.toPCollection(readPipeline, env.parseQuery("SELECT * FROM DirTable"));
    PAssert.that("Directory with '/' reads all files", dirResult).containsInAnyOrder(ALL_ROWS);

    String globPath = new File(destinationDir, "output-*").getAbsolutePath();
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE GlobTable %s TYPE parquet LOCATION '%s'",
            FIELD_NAMES, globPath));
    PCollection<Row> globResult =
        BeamSqlRelUtils.toPCollection(readPipeline, env.parseQuery("SELECT * FROM GlobTable"));
    PAssert.that("Glob 'output-*' reads all files", globResult).containsInAnyOrder(ALL_ROWS);

    File[] writtenFiles = destinationDir.listFiles((dir, name) -> name.startsWith("output-"));
    assertTrue(
        "Test setup failed: No output files found",
        writtenFiles != null && writtenFiles.length > 0);
    String singleFilePath = writtenFiles[0].getAbsolutePath();

    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE SingleFileTable %s TYPE parquet LOCATION '%s'",
            FIELD_NAMES, singleFilePath));
    PCollection<Row> singleFileResult =
        BeamSqlRelUtils.toPCollection(
            readPipeline, env.parseQuery("SELECT * FROM SingleFileTable"));

    PCollection<Long> count = singleFileResult.apply(Count.globally());
    PAssert.thatSingleton(count)
        .satisfies(
            actualCount -> {
              assertTrue("Count should be greater than 0", actualCount > 0L);
              return null;
            });

    PipelineResult.State state = readPipeline.run().waitUntilFinish();
    assertEquals(State.DONE, state);
  }

  @Test
  public void testReadAllTypes() {
    writeAllTypesTable();

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(readPipeline, env.parseQuery("SELECT * FROM AllTypes"));

    PAssert.that(rows).satisfies(ParquetTableProviderTest::checkAllTypesRows);
    readPipeline.run().waitUntilFinish();
  }

  @Test
  public void testReadAllTypesWithProjectionPushDown() {
    writeAllTypesTable();

    BeamRelNode relNode = env.parseQuery("SELECT d, si, ti, ts, id FROM AllTypes");
    assertThat(relNode, instanceOf(BeamPushDownIOSourceRel.class));
    PCollection<Row> rows = BeamSqlRelUtils.toPCollection(readPipeline, relNode);
    assertThat(rows.getSchema().getFieldNames(), contains("d", "si", "ti", "ts", "id"));

    PAssert.that(rows).satisfies(ParquetTableProviderTest::checkAllTypesRows);
    readPipeline.run().waitUntilFinish();
  }

  /** Writes a row with a value in every column (id 1) and a row with only nulls (id 2). */
  private void writeAllTypesTable() {
    String location =
        new File(tempFolder.getRoot(), "all-types").getAbsolutePath() + File.separator;
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE AllTypes %s TYPE parquet LOCATION '%s'",
            ALL_TYPES_FIELDS, location));
    BeamSqlRelUtils.toPCollection(
        writePipeline,
        env.parseQuery(
            "INSERT INTO AllTypes VALUES "
                + "(1, CAST(1 AS TINYINT), CAST(2 AS SMALLINT), CAST(3 AS BIGINT),"
                + " CAST(1.5 AS FLOAT), CAST(2.5 AS DOUBLE), CAST(4 AS DECIMAL), TRUE,"
                + " DATE '2025-07-14', TIMESTAMP '2025-07-14 01:02:03', 'abc', X'0102'), "
                + "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"));
    writePipeline.run().waitUntilFinish();
  }

  /**
   * Checks each column of the rows written by {@link #writeAllTypesTable}, including the Java type
   * of its value: for example, a DATE column must hold a {@link LocalDate}.
   */
  private static Void checkAllTypesRows(Iterable<Row> rows) {
    int count = 0;
    for (Row row : rows) {
      count++;
      int id = row.getInt32("id");
      for (String field : row.getSchema().getFieldNames()) {
        if (!"id".equals(field)) {
          Object expected = id == 1 ? ALL_TYPES_VALUES.get(field) : null;
          assertEquals(
              "Column " + field + " of row " + id,
              describe(expected),
              describe(row.getValue(field)));
        }
      }
    }
    assertEquals(2, count);
    return null;
  }

  private static String describe(Object value) {
    if (value == null) {
      return "null";
    }
    String valueString =
        value instanceof byte[] ? Arrays.toString((byte[]) value) : value.toString();
    return value.getClass().getName() + ": " + valueString;
  }
}
