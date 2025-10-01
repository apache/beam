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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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
}
