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
package org.apache.beam.sdk.io.csv;

import static java.util.Objects.requireNonNull;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.sdk.io.csv.CsvIOTestData.DATA;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.common.SchemaAwareJavaBeans.AllPrimitiveDataTypes;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link CsvIO.Write}. */
@RunWith(JUnit4.class)
public class CsvIOWriteTest {
  @Rule public TestPipeline writePipeline = TestPipeline.create();

  @Rule public TestPipeline readPipeline = TestPipeline.create();

  @Rule
  public TestPipeline errorPipeline = TestPipeline.create().enableAbandonedNodeEnforcement(false);

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void headersWithCommentsWrittenFirstOnEachShard() {
    File folder =
        createFolder(
            AllPrimitiveDataTypes.class.getSimpleName(),
            "headersWithCommentsWrittenFirstOnEachShard");

    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.allPrimitiveDataTypeRows)
                .withRowSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA));
    String expectedHeader = "aBoolean,aDecimal,aDouble,aFloat,aLong,aString,anInteger";
    CSVFormat csvFormat =
        CSVFormat.DEFAULT.withHeaderComments("foo", "bar", "baz").withCommentMarker('#');

    input.apply(CsvIO.writeRows(toFilenamePrefix(folder), csvFormat).withNumShards(3));
    writePipeline.run().waitUntilFinish();

    PCollection<FileIO.ReadableFile> files =
        readPipeline
            .apply(FileIO.match().filepattern(toFilenamePrefix(folder) + "*"))
            .apply(FileIO.readMatches());
    PAssert.that(files)
        .satisfies(
            (Iterable<FileIO.ReadableFile> itr) -> {
              Iterable<FileIO.ReadableFile> safeItr = requireNonNull(itr);
              for (FileIO.ReadableFile file : safeItr) {
                try {
                  List<String> lines = Splitter.on('\n').splitToList(file.readFullyAsUTF8String());
                  assertFalse(lines.isEmpty());
                  assertEquals("# foo", lines.get(0));
                  assertEquals("# bar", lines.get(1));
                  assertEquals("# baz", lines.get(2));
                  assertEquals(expectedHeader, lines.get(3));

                  assertTrue(
                      lines.subList(4, lines.size()).stream().noneMatch(expectedHeader::equals));
                } catch (IOException e) {
                  fail(e.getMessage());
                }
              }
              return null;
            });

    readPipeline.run();
  }

  @Test
  public void headersWrittenFirstOnEachShard() {
    File folder =
        createFolder(AllPrimitiveDataTypes.class.getSimpleName(), "headersWrittenFirstOnEachShard");

    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.allPrimitiveDataTypeRows)
                .withRowSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA));
    String expectedHeader = "aBoolean,aDecimal,aDouble,aFloat,aLong,aString,anInteger";
    CSVFormat csvFormat = CSVFormat.DEFAULT;

    input.apply(CsvIO.writeRows(toFilenamePrefix(folder), csvFormat).withNumShards(3));
    writePipeline.run().waitUntilFinish();

    PCollection<FileIO.ReadableFile> files =
        readPipeline
            .apply(FileIO.match().filepattern(toFilenamePrefix(folder) + "*"))
            .apply(FileIO.readMatches());
    PAssert.that(files)
        .satisfies(
            (Iterable<FileIO.ReadableFile> itr) -> {
              Iterable<FileIO.ReadableFile> safeItr = requireNonNull(itr);
              for (FileIO.ReadableFile file : safeItr) {
                try {
                  List<String> lines = Splitter.on('\n').splitToList(file.readFullyAsUTF8String());
                  assertFalse(lines.isEmpty());
                  assertEquals(expectedHeader, lines.get(0));
                  assertTrue(
                      lines.subList(1, lines.size()).stream().noneMatch(expectedHeader::equals));
                } catch (IOException e) {
                  fail(e.getMessage());
                }
              }
              return null;
            });

    readPipeline.run();
  }

  @Test
  public void writesUserDefinedTypes() {
    File folder =
        createFolder(AllPrimitiveDataTypes.class.getSimpleName(), "writesUserDefinedTypes");

    PCollection<AllPrimitiveDataTypes> input =
        writePipeline.apply(
            Create.of(
                allPrimitiveDataTypes(false, BigDecimal.TEN, 1.0, 1.0f, 1, 1L, "a"),
                allPrimitiveDataTypes(
                    false, BigDecimal.TEN.add(BigDecimal.TEN), 2.0, 2.0f, 2, 2L, "b"),
                allPrimitiveDataTypes(
                    false,
                    BigDecimal.TEN.add(BigDecimal.TEN).add(BigDecimal.TEN),
                    3.0,
                    3.0f,
                    3,
                    3L,
                    "c")));

    String expectedHeader = "aBoolean,aDecimal,aDouble,aFloat,aLong,aString,anInteger";
    CSVFormat csvFormat = CSVFormat.DEFAULT;
    input.apply(
        CsvIO.<AllPrimitiveDataTypes>write(toFilenamePrefix(folder), csvFormat).withNumShards(1));

    writePipeline.run().waitUntilFinish();

    PAssert.that(readPipeline.apply(TextIO.read().from(toFilenamePrefix(folder) + "*")))
        .containsInAnyOrder(
            expectedHeader,
            "false,10,1.0,1.0,1,a,1",
            "false,20,2.0,2.0,2,b,2",
            "false,30,3.0,3.0,3,c,3");

    readPipeline.run();
  }

  @Test
  public void nonNullCSVFormatHeaderWritesSelectedSchemaFields() {
    File folder =
        createFolder(
            AllPrimitiveDataTypes.class.getSimpleName(),
            "nonNullCSVFormatHeaderWritesSelectedSchemaFields");
    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.allPrimitiveDataTypeRows)
                .withRowSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA));

    String expectedHeader = "aFloat,aString,aDecimal";

    CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader("aFloat", "aString", "aDecimal");
    input.apply(CsvIO.writeRows(toFilenamePrefix(folder), csvFormat).withNumShards(1));
    writePipeline.run().waitUntilFinish();

    PAssert.that(readPipeline.apply(TextIO.read().from(toFilenamePrefix(folder) + "*")))
        .containsInAnyOrder(expectedHeader, "1.0,a,10", "2.0,b,20", "3.0,c,30");

    readPipeline.run();
  }

  @Test
  public void withSkipHeaderRecordOnlyWritesRows() {
    File folder =
        createFolder(
            AllPrimitiveDataTypes.class.getSimpleName(),
            "withSkipHeaderRecordWritesCsvFilesWithoutHeaders");

    CSVFormat csvFormat = CSVFormat.DEFAULT.withSkipHeaderRecord();

    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.allPrimitiveDataTypeRows)
                .withRowSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA));

    input.apply(CsvIO.writeRows(toFilenamePrefix(folder), csvFormat).withNumShards(1));

    writePipeline.run().waitUntilFinish();

    PAssert.that(readPipeline.apply(TextIO.read().from(toFilenamePrefix(folder) + "*")))
        .containsInAnyOrder(
            "false,10,1.0,1.0,1,a,1", "false,20,2.0,2.0,2,b,2", "false,30,3.0,3.0,3,c,3");

    readPipeline.run();
  }

  @Test
  public void nullCSVFormatHeaderWritesAllSchemaFields() {
    File folder =
        createFolder(
            AllPrimitiveDataTypes.class.getSimpleName(),
            "nullCSVFormatHeaderWritesAllSchemaFields");

    PCollection<Row> input =
        writePipeline.apply(
            Create.of(DATA.allPrimitiveDataTypeRows)
                .withRowSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA));
    String expectedHeader = "aBoolean,aDecimal,aDouble,aFloat,aLong,aString,anInteger";
    CSVFormat csvFormat = CSVFormat.DEFAULT;

    input.apply(CsvIO.writeRows(toFilenamePrefix(folder), csvFormat).withNumShards(1));

    writePipeline.run().waitUntilFinish();

    PAssert.that(readPipeline.apply(TextIO.read().from(toFilenamePrefix(folder) + "*")))
        .containsInAnyOrder(
            expectedHeader,
            "false,10,1.0,1.0,1,a,1",
            "false,20,2.0,2.0,2,b,2",
            "false,30,3.0,3.0,3,c,3");

    readPipeline.run();
  }

  @Test
  public void nonNullHeaderCommentsRequiresHeaderMarker() {
    PCollection<Row> input =
        errorPipeline.apply(
            Create.of(DATA.allPrimitiveDataTypeRows)
                .withRowSchema(ALL_PRIMITIVE_DATA_TYPES_SCHEMA));
    IllegalArgumentException nullHeaderMarker =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                input.apply(
                    CsvIO.writeRows(
                        "somewhere",
                        CSVFormat.DEFAULT.withHeaderComments("some", "header", "comments"))));

    assertEquals(
        "CSVFormat withCommentMarker required when withHeaderComments",
        nullHeaderMarker.getMessage());
  }

  private static String toFilenamePrefix(File folder) {
    checkArgument(folder.isDirectory());
    return folder.getAbsolutePath() + "/out";
  }

  private File createFolder(String... paths) {
    try {
      return tempFolder.newFolder(paths);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
