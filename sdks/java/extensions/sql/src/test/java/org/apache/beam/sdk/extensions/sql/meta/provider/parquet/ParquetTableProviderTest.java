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

import java.io.File;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv;
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamSqlRelUtils;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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
  private static final Schema PROJECTED_SCHEMA =
      Schema.builder().addInt64Field("age").addStringField("country").build();

  @Test
  public void testWriteAndReadTable() {
    File destinationFile = new File(tempFolder.getRoot(), "person-info/");

    BeamSqlEnv env = BeamSqlEnv.inMemory(new ParquetTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE PersonInfo %s TYPE parquet LOCATION '%s'",
            FIELD_NAMES, destinationFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        writePipeline,
        env.parseQuery(
            "INSERT INTO PersonInfo VALUES ('Alan', 22, 'England'), ('John', 42, 'USA')"));
    writePipeline.run().waitUntilFinish();

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(readPipeline, env.parseQuery("SELECT * FROM PersonInfo"));
    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(TABLE_SCHEMA).addValues("Alan", 22L, "England").build(),
            Row.withSchema(TABLE_SCHEMA).addValues("John", 42L, "USA").build());

    PCollection<Row> filtered =
        BeamSqlRelUtils.toPCollection(
            readPipeline, env.parseQuery("SELECT * FROM PersonInfo WHERE age > 25"));
    PAssert.that(filtered)
        .containsInAnyOrder(Row.withSchema(TABLE_SCHEMA).addValues("John", 42L, "USA").build());

    PCollection<Row> projected =
        BeamSqlRelUtils.toPCollection(
            readPipeline, env.parseQuery("SELECT age, country FROM PersonInfo"));
    PAssert.that(projected)
        .containsInAnyOrder(
            Row.withSchema(PROJECTED_SCHEMA).addValues(22L, "England").build(),
            Row.withSchema(PROJECTED_SCHEMA).addValues(42L, "USA").build());

    PCollection<Row> filteredAndProjected =
        BeamSqlRelUtils.toPCollection(
            readPipeline, env.parseQuery("SELECT age, country FROM PersonInfo WHERE age > 25"));
    PAssert.that(filteredAndProjected)
        .containsInAnyOrder(Row.withSchema(PROJECTED_SCHEMA).addValues(42L, "USA").build());

    PipelineResult.State state = readPipeline.run().waitUntilFinish();
    assertEquals(State.DONE, state);
  }
}
