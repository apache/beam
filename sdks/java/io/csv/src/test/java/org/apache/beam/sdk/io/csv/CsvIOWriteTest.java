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

import static org.apache.beam.sdk.io.csv.CsvIO.DEFAULT_FILENAME_SUFFIX;
import static org.apache.beam.sdk.io.csv.CsvIOTestHelpers.ALL_DATA_TYPES_SCHEMA;
import static org.apache.beam.sdk.io.csv.CsvIOTestHelpers.rowOf;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.csv.CsvIOTestHelpers.AllDataTypes;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayData.Identifier;
import org.apache.beam.sdk.transforms.display.DisplayData.Item;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.commons.csv.CSVFormat;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for {@link CsvIO.Write}. */
public class CsvIOWriteTest {

  @Rule public TemporaryFolder tmpFolder = TemporaryFolder.builder().build();

  @Rule public TestPipeline pipeline = TestPipeline.create();

  private final List<AllDataTypes> userTypes =
      Arrays.asList(
          AllDataTypes.of(
              true,
              (byte) 0,
              Instant.ofEpochMilli(0L).toDateTime(),
              BigDecimal.valueOf(0L),
              0.1,
              0.2f,
              (short) 3,
              4,
              5L,
              "a"),
          AllDataTypes.of(
              true,
              (byte) 1,
              Instant.ofEpochMilli(1L).toDateTime(),
              BigDecimal.valueOf(1L),
              1.1,
              1.2f,
              (short) 4,
              5,
              6L,
              "b"),
          AllDataTypes.of(
              false,
              (byte) 2,
              Instant.ofEpochMilli(2L).toDateTime(),
              BigDecimal.valueOf(2L),
              2.0,
              2.1f,
              (short) 5,
              6,
              7L,
              "c"));

  private final List<Row> rows =
      Arrays.asList(
          rowOf(
              true,
              (byte) 0,
              Instant.ofEpochMilli(0L).toDateTime(),
              BigDecimal.valueOf(0L),
              0.1,
              0.2f,
              (short) 3,
              4,
              5L,
              "a"),
          rowOf(
              true,
              (byte) 1,
              Instant.ofEpochMilli(1L).toDateTime(),
              BigDecimal.valueOf(1L),
              1.1,
              1.2f,
              (short) 4,
              5,
              6L,
              "b"),
          rowOf(
              false,
              (byte) 2,
              Instant.ofEpochMilli(2L).toDateTime(),
              BigDecimal.valueOf(2L),
              2.0,
              2.1f,
              (short) 5,
              6,
              7L,
              "c"));

  @Test
  public void populateDisplayData() {
    CsvIO.Write<Row> write = CsvIO.writeRows().to("somewhere");
    Map<String, Object> commonWithoutCsvFormat =
        ImmutableMap.of("filenameSuffix", DEFAULT_FILENAME_SUFFIX, "filenamePrefix", "somewhere");
    Map<String, Object> common =
        ImmutableMap.<String, Object>builder()
            .putAll(commonWithoutCsvFormat)
            .put("csvFormat", CSVFormat.DEFAULT.toString())
            .build();
    assertDisplayDataEquals(common, DisplayData.from(write));
    assertDisplayDataEquals(
        ImmutableMap.<String, Object>builder()
            .putAll(commonWithoutCsvFormat)
            .put("csvFormat", CSVFormat.MONGODB_CSV.toString())
            .build(),
        DisplayData.from(write.withCSVFormat(CSVFormat.MONGODB_CSV)));
    assertDisplayDataEquals(
        ImmutableMap.<String, Object>builder()
            .putAll(common)
            .put("preamble", "preamblevalue")
            .build(),
        DisplayData.from(write.withPreamble("preamblevalue")));
    assertDisplayDataEquals(
        ImmutableMap.<String, Object>builder()
            .putAll(common)
            .put("compression", Compression.GZIP.name())
            .build(),
        DisplayData.from(write.withCompression(Compression.GZIP)));
    assertDisplayDataEquals(
        ImmutableMap.<String, Object>builder().putAll(common).put("numShards", 9L).build(),
        DisplayData.from(write.withNumShards(9)));
    assertDisplayDataEquals(
        ImmutableMap.<String, Object>builder()
            .putAll(common)
            .put("tempDirectory", tmpFolder.getRoot().getAbsolutePath())
            .build(),
        DisplayData.from(
            write.withTempDirectory(new MockResourceId(Paths.get(tmpFolder.getRoot().toURI())))));
  }

  private static void assertDisplayDataEquals(
      Map<String, Object> expected, DisplayData displayData) {
    Map<String, Object> actual = mapFrom(displayData);
    Set<String> keys =
        ImmutableSet.<String>builder().addAll(expected.keySet()).addAll(actual.keySet()).build();
    for (String key : keys) {
      assertEquals(key, expected.get(key), actual.get(key));
    }
  }

  private static Map<String, Object> mapFrom(DisplayData displayData) {
    Map<String, Object> result = new HashMap<>();
    for (Map.Entry<Identifier, Item> entry : displayData.asMap().entrySet()) {
      result.put(entry.getKey().getKey(), entry.getValue().getValue());
    }
    return result;
  }

  @Test
  public void withInvalidType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CsvIO.write().to("badtype").expand(pipeline.apply(Create.of("1,2,3", "4,5,6"))));
    pipeline.run();
  }

  @Test
  public void withPreamble() throws IOException {
    String preamble =
        "id=44ebe8ee-dc0f-4c93-8b47-2e627e1eb988\ndate=2022-01-02\ntest=e3bbfc47-9bcb-46f0-8944-829827ee5222";
    String to = "rows_preamble";
    String regex = String.format("^%s.*$", to);
    pipeline
        .apply(Create.of(rows).withRowSchema(ALL_DATA_TYPES_SCHEMA))
        .apply(
            CsvIO.writeRows()
                .to(tmpFolder.getRoot().getAbsolutePath() + "/" + to)
                .withPreamble(preamble));
    pipeline.run().waitUntilFinish();
    String[] files = filesMatching(regex);
    assertTrue(
        "CsvIO.writeRows should write to " + tmpFolder.getRoot().getAbsolutePath() + "/" + to,
        files.length > 0);
    String expectedHeader =
        "aBoolean,aByte,aDouble,aFloat,aLong,aShort,anInt,dateTime,decimal,string";
    for (String name : files) {
      Path p = Paths.get(tmpFolder.getRoot().getAbsolutePath(), name);
      File f = new File(p.toString());
      assertFileContentsMatchInAnyOrder(
          preamble,
          expectedHeader,
          f,
          "true,0,0.1,0.2,5,3,4,1970-01-01T00:00:00.000Z,0,a",
          "true,1,1.1,1.2,6,4,5,1970-01-01T00:00:00.001Z,1,b",
          "false,2,2.0,2.1,7,5,6,1970-01-01T00:00:00.002Z,2,c");
    }
  }

  @Test
  public void rowsWithCsvFormatNonDefault() throws IOException {
    String to = "rows_non_default";
    String regex = String.format("^%s.*$", to);
    pipeline
        .apply(Create.of(rows).withRowSchema(ALL_DATA_TYPES_SCHEMA))
        .apply(
            CsvIO.writeRows()
                .to(tmpFolder.getRoot().getAbsolutePath() + "/" + to)
                .withCSVFormat(CSVFormat.MYSQL));
    pipeline.run().waitUntilFinish();
    String[] files = filesMatching(regex);
    assertTrue(
        "CsvIO.writeRows should write to " + tmpFolder.getRoot().getAbsolutePath() + "/" + to,
        files.length > 0);
    String expectedHeader =
        "aBoolean\taByte\taDouble\taFloat\taLong\taShort\tanInt\tdateTime\tdecimal\tstring";
    for (String name : files) {
      Path p = Paths.get(tmpFolder.getRoot().getAbsolutePath(), name);
      File f = new File(p.toString());
      assertFileContentsMatchInAnyOrder(
          null,
          expectedHeader,
          f,
          "true\t0\t0.1\t0.2\t5\t3\t4\t1970-01-01T00:00:00.000Z\t0\ta",
          "true\t1\t1.1\t1.2\t6\t4\t5\t1970-01-01T00:00:00.001Z\t1\tb",
          "false\t2\t2.0\t2.1\t7\t5\t6\t1970-01-01T00:00:00.002Z\t2\tc");
    }
  }

  @Test
  public void rowsWithDefaults() throws IOException {
    String to = "rows";
    String regex = String.format("^%s.*$", to);
    pipeline
        .apply(Create.of(rows).withRowSchema(ALL_DATA_TYPES_SCHEMA))
        .apply(CsvIO.writeRows().to(tmpFolder.getRoot().getAbsolutePath() + "/" + to));
    pipeline.run().waitUntilFinish();
    String[] files = filesMatching(regex);
    assertTrue(
        "CsvIO.writeRows should write to " + tmpFolder.getRoot().getAbsolutePath() + "/" + to,
        files.length > 0);
    String expectedHeader =
        "aBoolean,aByte,aDouble,aFloat,aLong,aShort,anInt,dateTime,decimal,string";
    for (String name : files) {
      Path p = Paths.get(tmpFolder.getRoot().getAbsolutePath(), name);
      File f = new File(p.toString());
      assertFileContentsMatchInAnyOrder(
          null,
          expectedHeader,
          f,
          "true,0,0.1,0.2,5,3,4,1970-01-01T00:00:00.000Z,0,a",
          "true,1,1.1,1.2,6,4,5,1970-01-01T00:00:00.001Z,1,b",
          "false,2,2.0,2.1,7,5,6,1970-01-01T00:00:00.002Z,2,c");
    }
  }

  @Test
  public void userTypesWithDefaults() throws IOException {
    String to = "user_types";
    String regex = String.format("^%s.*$", to);
    pipeline
        .apply(Create.of(userTypes))
        .apply(CsvIO.<AllDataTypes>write().to(tmpFolder.getRoot().getAbsolutePath() + "/" + to));
    pipeline.run().waitUntilFinish();
    String[] files = filesMatching(regex);
    assertTrue(
        "CsvIO.writeRows should write to " + tmpFolder.getRoot().getAbsolutePath() + "/" + to,
        files.length > 0);
    String expectedHeader =
        "aBoolean,aByte,aDouble,aFloat,aLong,aShort,anInt,dateTime,decimal,string";
    for (String name : files) {
      Path p = Paths.get(tmpFolder.getRoot().getAbsolutePath(), name);
      File f = new File(p.toString());
      assertFileContentsMatchInAnyOrder(
          null,
          expectedHeader,
          f,
          "true,0,0.1,0.2,5,3,4,1970-01-01T00:00:00.000Z,0,a",
          "true,1,1.1,1.2,6,4,5,1970-01-01T00:00:00.001Z,1,b",
          "false,2,2.0,2.1,7,5,6,1970-01-01T00:00:00.002Z,2,c");
    }
  }

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

  private static class MockResourceId implements ResourceId {

    private final Path basis;

    MockResourceId(Path basis) {
      this.basis = basis;
    }

    @Override
    public ResourceId resolve(String other, ResolveOptions resolveOptions) {
      return new MockResourceId(Paths.get(basis.toAbsolutePath().toString(), other));
    }

    @Override
    public ResourceId getCurrentDirectory() {
      return new MockResourceId(basis.getRoot());
    }

    @Override
    public String getScheme() {
      return basis.toAbsolutePath().toUri().getScheme();
    }

    @Override
    public String getFilename() {
      return basis.toAbsolutePath().toString();
    }

    @Override
    public boolean isDirectory() {
      return basis.toFile().isDirectory();
    }
  }
}
