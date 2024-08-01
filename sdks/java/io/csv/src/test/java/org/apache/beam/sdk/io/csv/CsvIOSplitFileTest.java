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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.commons.csv.CSVFormat;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

public class CsvIOSplitFileTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule public final TestPipeline pipeline = TestPipeline.create();

  @Rule public final TestName testName = new TestName();

  @Test
  public void isSerializable() throws Exception {
    SerializableUtils.ensureSerializable(CsvIOSplitFile.class);
  }

  @Test
  public void givenEmptyFile_returnsEmptyOutput() throws IOException {
    PCollection<FileIO.ReadableFile> input = filesOf("");
    CsvIOSplitFile underTest = new CsvIOSplitFile(csvFormat());
    PCollectionTuple output = input.apply(underTest);
    PAssert.that(output.get(underTest.outputTag)).empty();
    PAssert.that(output.get(underTest.errorTag)).empty();
    pipeline.run();
  }

  @Test
  public void givenNoMultilineRecords_splits() throws IOException {
    PCollection<FileIO.ReadableFile> input = filesOf("foo,8,12.14", "bar,7,11.4");
    CsvIOSplitFile underTest = new CsvIOSplitFile(csvFormat());
    List<KV<String, List<String>>> want =
        Arrays.asList(
            KV.of(testName.getMethodName() + ".csv", Arrays.asList("foo", "8", "12.14")),
            KV.of(testName.getMethodName() + ".csv", Arrays.asList("bar", "7", "11.4")));
    PCollectionTuple output = input.apply(underTest);
    PAssert.that(output.get(underTest.outputTag)).containsInAnyOrder(want);
    PAssert.that(output.get(underTest.errorTag)).empty();
    pipeline.run();
  }

  @Test
  public void givenMultilineRecords_splits() throws IOException {
    PCollection<FileIO.ReadableFile> input = filesOf("\"foo\nbar\",8,12.14", "\"pop\nbob\",9,14.0");
    CsvIOSplitFile underTest = new CsvIOSplitFile(csvFormat());
    List<KV<String, List<String>>> want =
        Arrays.asList(
            KV.of(testName.getMethodName() + ".csv", Arrays.asList("foo\nbar", "8", "12.14")),
            KV.of(testName.getMethodName() + ".csv", Arrays.asList("pop\nbob", "9", "14.0")));
    PCollectionTuple output = input.apply(underTest);
    PAssert.that(output.get(underTest.outputTag)).containsInAnyOrder(want);
    PAssert.that(output.get(underTest.errorTag)).empty();
    pipeline.run();
  }

  // create a file in temp folder with lines given
  private String fileOf(String... lines) throws IOException {
    File file = tempFolder.newFile(testName.getMethodName() + ".csv");

    Path path =
        Files.write(
            file.toPath(), Arrays.asList(lines), StandardCharsets.UTF_8, StandardOpenOption.WRITE);
    return path.toString();
  }

  private PCollection<FileIO.ReadableFile> filesOf(String... lines) throws IOException {
    return pipeline.apply(FileIO.match().filepattern(fileOf(lines))).apply(FileIO.readMatches());
  }

  private CSVFormat csvFormat() {
    return CSVFormat.DEFAULT
        .withHeader("a_string", "an_integer", "a_double")
        .withAllowDuplicateHeaderNames(false);
  }
}
