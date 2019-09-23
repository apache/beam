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
package org.apache.beam.sdk.extensions.sql.meta.provider.avro;

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

/** Test for AvroTable. */
@RunWith(JUnit4.class)
public class AvroTableProviderTest {
  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String AVRO_FIELD_NAMES = "(name VARCHAR, age BIGINT, country VARCHAR)";

  private static final Schema OUTPUT_ROW_SCHEMA =
      Schema.builder().addInt64Field("age").addStringField("country").build();

  @Test
  public void testReadAndWriteAvroTable() {
    File destinationFile = new File(tempFolder.getRoot(), "person-info.avro");

    BeamSqlEnv env = BeamSqlEnv.inMemory(new AvroTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE PersonInfo %s TYPE avro LOCATION '%s'",
            AVRO_FIELD_NAMES, destinationFile.getAbsolutePath()));

    BeamSqlRelUtils.toPCollection(
        writePipeline,
        env.parseQuery(
            "INSERT INTO PersonInfo VALUES ('Alan', 22, 'England'), ('John', 42, 'USA')"));

    writePipeline.run().waitUntilFinish();

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(
            readPipeline, env.parseQuery("SELECT age, country FROM PersonInfo where age > 25"));

    PAssert.that(rows)
        .containsInAnyOrder(Row.withSchema(OUTPUT_ROW_SCHEMA).addValues(42L, "USA").build());

    PipelineResult.State state = readPipeline.run().waitUntilFinish();
    assertEquals(state, State.DONE);
  }
}
