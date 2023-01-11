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
package org.apache.beam.sdk.io;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionRowTuple;
import org.apache.beam.sdk.values.Row;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class AvroSchemaTransformsTest {
  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final Schema SCHEMA =
      Schema.builder().addInt64Field("age").addStringField("age_str").build();

  private Row createRow(long l) {
    return Row.withSchema(SCHEMA).addValues(l, Long.valueOf(l).toString()).build();
  }

  @Test
  @Ignore
  @Category({NeedsRunner.class})
  public void testWriteAndReadTable() {
    File destinationFile = new File(tempFolder.getRoot(), "person-info-schematransform.avro");
    List<Row> rowList = Arrays.asList(createRow(1L), createRow(3L), createRow(4L));

    // Write values
    AvroWriteSchemaTransformProvider writeProvider = new AvroWriteSchemaTransformProvider();
    PCollectionRowTuple.of(
            "inout",
            writePipeline.apply("Create", Create.of(rowList).withCoder(RowCoder.of(SCHEMA))))
        .apply(
            writeProvider
                .from(
                    AvroWriteSchemaTransformProvider.AvroWriteSchemaTransformConfiguration.builder()
                        .setDataSchema(SCHEMA)
                        .setLocation(destinationFile.getAbsolutePath())
                        .build())
                .buildTransform());
    writePipeline.run();

    AvroReadSchemaTransformProvider readProvider = new AvroReadSchemaTransformProvider();
    PCollection<Row> read =
        PCollectionRowTuple.empty(readPipeline)
            .apply(
                readProvider
                    .from(
                        AvroReadSchemaTransformProvider.AvroReadSchemaTransformConfiguration
                            .builder()
                            .setDataSchema(SCHEMA)
                            .setLocation(destinationFile.getAbsolutePath())
                            .build())
                    .buildTransform())
            .get("output");
    readPipeline.run();
    PAssert.that(read).containsInAnyOrder(rowList);
  }
}
