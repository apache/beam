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

import static org.apache.beam.sdk.io.csv.CsvIOTestHelpers.ALL_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvIOTestHelpers.rowOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link CsvIO.Write} */
public class CsvIOWriteTest {

  @Rule public TemporaryFolder tmpFolder = TemporaryFolder.builder().build();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private final List<Row> rows =
      Arrays.asList(
          rowOf(
              true,
              (byte) 0,
              Instant.ofEpochMilli(1670358365856L).toDateTime(),
              BigDecimal.valueOf(1L),
              3.12345,
              4.1f,
              (short) 5,
              2,
              7L,
              "asdfjkl;"),
          rowOf(
              false,
              (byte) 1,
              Instant.ofEpochMilli(1670358365856L).toDateTime(),
              BigDecimal.valueOf(-1L),
              -3.12345,
              -4.1f,
              (short) -5,
              -2,
              -7L,
              "1234567"));

  @Test
  public void hasNoSchema() {}

  @Test
  public void withPreamble() {}

  @Test
  public void rowsWithCsvFormatNonDefault() {}

  @Test
  public void rowsWithDefaults() throws IOException {
    String to = "foo";
    String regex = String.format("^%s.*$", to);
    pipeline
        .apply(Create.of(rows).withRowSchema(ALL_DATA_TYPES_SCHEMA))
        .apply(CsvIO.writeRows().to(tmpFolder.getRoot().getAbsolutePath() + "/" + to));
    pipeline.run().waitUntilFinish();
    String[] files = filesMatching(regex);
    assertTrue(
        "CsvIO.writeRows should write to " + tmpFolder.getRoot().getAbsolutePath() + "/" + to,
        files.length > 0);
    String expectedHeader = CsvUtils.buildHeaderFrom(ALL_DATA_TYPES_SCHEMA, CSVFormat.DEFAULT);
    for (String name : files) {
      Path p = Paths.get(tmpFolder.getRoot().getAbsolutePath(), name);
      File f = new File(p.toString());
      assertFileContentsMatchInAnyOrder(
          null,
          expectedHeader,
          f,
          "asdfjkl;,1,2022-12-06T20:26:05.856Z,true,0,3.12345,4.1,5,2,7",
          "1234567,1,-3.12345,-4.1,-5,-2,-7,-1,2022-12-06T20:26:05.856Z,false");
    }
  }

  @Test
  public void userTypesWithDefaults() {}

  private static List<String> readLinesFromFile(File f) throws IOException {
    List<String> currentFile = new ArrayList<>();
    try (BufferedReader reader = Files.newBufferedReader(f.toPath(), Charsets.UTF_8)) {
      while (true) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        currentFile.add(line);
      }
    }
    return currentFile;
  }

  private void assertFileContentsMatchInAnyOrder(
      @Nullable String preamble, String header, File f, String... expected) throws IOException {
    List<String> actual = readLinesFromFile(f);
    int headerIndex = 0;
    if (preamble != null) {
      String[] preambleLines = preamble.split("\n");
      assertArrayEquals(preambleLines, actual.subList(0, preambleLines.length).toArray());
      headerIndex = preambleLines.length;
    }

    assertEquals(header, actual.get(headerIndex));

    String[] rest = actual.subList(headerIndex + 1, actual.size()).toArray(new String[0]);
    List<String> expectedList = Arrays.stream(expected).collect(Collectors.toList());
    String expectedString = String.join(", ", expectedList);
    for (String line : rest) {
      String message = String.format("[%s]\nshould contain %s", expectedString, line);
      assertTrue(message, expectedList.contains(line));
    }
  }

  private String[] filesMatching(String regex) {
    Pattern p = Pattern.compile(regex);
    return tmpFolder.getRoot().list((file, s) -> p.matcher(s).matches());
  }
}
