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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.io.SchemaIO;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test for AvroSchemaIOProvider. */
@RunWith(JUnit4.class)
public class AvroSchemaIOProviderTest {
  @Rule public TestPipeline writePipeline = TestPipeline.create();
  @Rule public TestPipeline readPipeline = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static final Schema SCHEMA =
      Schema.builder().addInt64Field("age").addStringField("age_str").build();

  private Row createRow(long l) {
    return Row.withSchema(SCHEMA).addValues(l, Long.valueOf(l).toString()).build();
  }

  @Test
  @Category({NeedsRunner.class})
  public void testWriteAndReadTable() {
    File destinationFile = new File(tempFolder.getRoot(), "person-info.avro");

    AvroSchemaIOProvider provider = new AvroSchemaIOProvider();
    Row configuration = Row.withSchema(provider.configurationSchema()).addValue(null).build();
    SchemaIO io = provider.from(destinationFile.getAbsolutePath(), configuration, SCHEMA);

    List<Row> rowsList = Arrays.asList(createRow(1L), createRow(3L), createRow(4L));
    PCollection<Row> rows =
        writePipeline.apply("Create", Create.of(rowsList).withCoder(RowCoder.of(SCHEMA)));
    rows.apply(io.buildWriter());
    writePipeline.run();

    PCollection<Row> read = readPipeline.begin().apply(io.buildReader());
    PAssert.that(read).containsInAnyOrder(rowsList);
    readPipeline.run();
  }

  @Test
  @Category({NeedsRunner.class})
  public void testStreamingWriteDefault() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "person-info");

    AvroSchemaIOProvider provider = new AvroSchemaIOProvider();
    Row config = Row.withSchema(provider.configurationSchema()).addValue(null).build();
    SchemaIO writeIO = provider.from(destinationFile.getAbsolutePath(), config, SCHEMA);

    TestStream<Row> createEvents =
        TestStream.create(RowCoder.of(SCHEMA))
            .addElements(TimestampedValue.of(createRow(1L), new Instant(1L)))
            .addElements(TimestampedValue.of(createRow(2L), Instant.ofEpochSecond(120L)))
            .advanceWatermarkToInfinity();

    writePipeline.apply("create", createEvents).apply("write", writeIO.buildWriter());
    writePipeline.run();

    // Verify we wrote two files.
    String wildcardPath = destinationFile.getAbsolutePath() + "*";
    MatchResult result = FileSystems.match(wildcardPath);
    assertEquals(2, result.metadata().size());

    // Verify results of the files.
    SchemaIO readIO = provider.from(wildcardPath, config, SCHEMA);
    PCollection<Row> read = readPipeline.begin().apply("read", readIO.buildReader());
    PAssert.that(read).containsInAnyOrder(createRow(1L), createRow(2L));
    readPipeline.run();
  }

  @Test
  @Category({NeedsRunner.class})
  public void testStreamingCustomWindowSize() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "person-info");

    AvroSchemaIOProvider provider = new AvroSchemaIOProvider();
    Row config =
        Row.withSchema(provider.configurationSchema())
            .addValue(Duration.ofMinutes(4).getSeconds())
            .build();
    SchemaIO writeIO = provider.from(destinationFile.getAbsolutePath(), config, SCHEMA);

    TestStream<Row> createEvents =
        TestStream.create(RowCoder.of(SCHEMA))
            .addElements(TimestampedValue.of(createRow(1L), new Instant(1L)))
            .addElements(TimestampedValue.of(createRow(2L), Instant.ofEpochSecond(120L)))
            .advanceWatermarkToInfinity();

    writePipeline.apply("create", createEvents).apply("write", writeIO.buildWriter());
    writePipeline.run();

    // Verify we wrote one file.
    String wildcardPath = destinationFile.getAbsolutePath() + "*";
    MatchResult result = FileSystems.match(wildcardPath);
    assertEquals(1, result.metadata().size());

    // Verify results of the files.
    SchemaIO readIO = provider.from(wildcardPath, config, SCHEMA);
    PCollection<Row> read = readPipeline.begin().apply("read", readIO.buildReader());
    PAssert.that(read).containsInAnyOrder(createRow(1L), createRow(2L));
    readPipeline.run();
  }

  @Test
  @Category({NeedsRunner.class})
  public void testBatchCustomWindowSize() throws Exception {
    File destinationFile = new File(tempFolder.getRoot(), "person-info");

    AvroSchemaIOProvider provider = new AvroSchemaIOProvider();
    Row config =
        Row.withSchema(provider.configurationSchema())
            .addValue(Duration.ofMinutes(4).getSeconds())
            .build();
    SchemaIO writeIO = provider.from(destinationFile.getAbsolutePath(), config, SCHEMA);

    List<Row> rowsList = Arrays.asList(createRow(1L), createRow(3L), createRow(4L));
    PCollection<Row> rows =
        writePipeline.apply("Create", Create.of(rowsList).withCoder(RowCoder.of(SCHEMA)));

    rows.apply("write", writeIO.buildWriter());
    writePipeline.run();

    // Verify we wrote one file.
    String wildcardPath = destinationFile.getAbsolutePath() + "*";
    MatchResult result = FileSystems.match(wildcardPath);
    assertEquals(1, result.metadata().size());

    // Verify results of the files.
    SchemaIO readIO = provider.from(wildcardPath, config, SCHEMA);
    PCollection<Row> read = readPipeline.begin().apply("read", readIO.buildReader());
    PAssert.that(read).containsInAnyOrder(rowsList);
    readPipeline.run();
  }
}
