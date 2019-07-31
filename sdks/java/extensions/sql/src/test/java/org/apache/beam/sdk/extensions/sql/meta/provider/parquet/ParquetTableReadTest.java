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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Test for ParquetTable. */
@RunWith(JUnit4.class)
public class ParquetTableReadTest {
  private static final Logger LOG = LoggerFactory.getLogger(ParquetTableReadTest.class);

  @Rule public TestPipeline pipeline = TestPipeline.create();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final String SQL_PARQUET_FIELD =
      "(name VARCHAR, favorite_color VARCHAR, favorite_numbers ARRAY<INTEGER>)";

  private static final Schema PARQUET_SCHEMA =
      Schema.builder()
          .addField("name", Schema.FieldType.STRING)
          .addNullableField("favorite_color", Schema.FieldType.STRING)
          .addArrayField("favorite_numbers", Schema.FieldType.INT32)
          .build();

  private String extractParquetFile(String fileName) throws IOException {
    InputStream inputStream = getClass().getResourceAsStream("/" + fileName);
    File root = temporaryFolder.getRoot();
    Path tempFilePath = new File(root, fileName).toPath();
    Files.copy(inputStream, tempFilePath);
    return tempFilePath.toString();
  }

  @Test
  public void testReadParquet() throws IOException {
    String parquetPath = extractParquetFile("users.parquet");

    BeamSqlEnv env = BeamSqlEnv.inMemory(new ParquetTableProvider());
    env.executeDdl(
        String.format(
            "CREATE EXTERNAL TABLE users %s TYPE parquet LOCATION '%s'",
            SQL_PARQUET_FIELD, parquetPath));

    PCollection<Row> rows =
        BeamSqlRelUtils.toPCollection(
            pipeline, env.parseQuery("SELECT name, favorite_color, favorite_numbers FROM users"));

    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(PARQUET_SCHEMA)
                .addValues("Alyssa", null, Arrays.asList(3, 9, 15, 20))
                .build(),
            Row.withSchema(PARQUET_SCHEMA).addValues("Ben", "red", Arrays.asList()).build());

    pipeline.run();
  }
}
