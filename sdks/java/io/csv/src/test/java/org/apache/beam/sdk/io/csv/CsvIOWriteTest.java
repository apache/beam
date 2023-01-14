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

import static org.apache.beam.sdk.io.csv.CsvIOTestData.DATA;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.ALL_PRIMITIVE_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.AllPrimitiveDataTypes;
import static org.apache.beam.sdk.io.csv.CsvIOTestJavaBeans.allPrimitiveDataTypes;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
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

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void headersWrittenFirstOnEachShard() {}

  @Test
  public void headersDefaultToAllSortedSchemaFields() {}

  @Test
  public void headerCommentsWrittenFirstOnEachShard() {}

  @Test
  public void throwsCSVFormatValidationErrors() {}

  @Test
  public void writesUserDefinedTypes() {
    File folder =
        createFolder(AllPrimitiveDataTypes.class.getSimpleName(), "writesUserDefinedTypes");

    PCollection<AllPrimitiveDataTypes> input =
        writePipeline.apply(
            Create.of(
                allPrimitiveDataTypes(
                    false, (byte) 1, BigDecimal.TEN, 1.0, 1.0f, (short) 1.0, 1, 1L, "a"),
                allPrimitiveDataTypes(
                    false,
                    (byte) 2,
                    BigDecimal.TEN.add(BigDecimal.TEN),
                    2.0,
                    2.0f,
                    (short) 2.0,
                    2,
                    2L,
                    "b"),
                allPrimitiveDataTypes(
                    false,
                    (byte) 3,
                    BigDecimal.TEN.add(BigDecimal.TEN).add(BigDecimal.TEN),
                    3.0,
                    3.0f,
                    (short) 3.0,
                    3,
                    3L,
                    "c")));

    String expectedHeader = "aBoolean,aByte,aDecimal,aDouble,aFloat,aLong,aShort,aString,anInteger";
    CSVFormat csvFormat = CSVFormat.DEFAULT;
    input.apply(
        CsvIO.<AllPrimitiveDataTypes>write(toFilenamePrefix(folder), csvFormat).withNumShards(1));

    writePipeline.run().waitUntilFinish();

    PAssert.that(readPipeline.apply(TextIO.read().from(toFilenamePrefix(folder) + "*")))
        .containsInAnyOrder(
            expectedHeader,
            "false,1,10,1.0,1.0,1,1,a,1",
            "false,2,20,2.0,2.0,2,2,b,2",
            "false,3,30,3.0,3.0,3,3,c,3");

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
    input.apply(CsvIO.writeRowsTo(toFilenamePrefix(folder), csvFormat).withNumShards(1));
    writePipeline.run().waitUntilFinish();

    PAssert.that(readPipeline.apply(TextIO.read().from(toFilenamePrefix(folder) + "*")))
        .containsInAnyOrder(expectedHeader, "1.0,a,10", "2.0,b,20", "3.0,c,30");

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
    String expectedHeader = "aBoolean,aByte,aDecimal,aDouble,aFloat,aLong,aShort,aString,anInteger";
    CSVFormat csvFormat = CSVFormat.DEFAULT;

    input.apply(CsvIO.writeRowsTo(toFilenamePrefix(folder), csvFormat).withNumShards(1));

    writePipeline.run().waitUntilFinish();

    PAssert.that(readPipeline.apply(TextIO.read().from(toFilenamePrefix(folder) + "*")))
        .containsInAnyOrder(
            expectedHeader,
            "false,1,10,1.0,1.0,1,1,a,1",
            "false,2,20,2.0,2.0,2,2,b,2",
            "false,3,30,3.0,3.0,3,3,c,3");

    readPipeline.run();
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
