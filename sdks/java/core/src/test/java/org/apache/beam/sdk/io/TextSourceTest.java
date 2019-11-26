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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO.ReadableFile;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for TextSource class. */
@RunWith(JUnit4.class)
public class TextSourceTest {
  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  @Category(NeedsRunner.class)
  public void testRemoveUtf8BOM() throws Exception {
    Path p1 = createTestFile("test_txt_ascii", Charset.forName("US-ASCII"), "1,p1", "2,p1");
    Path p2 =
        createTestFile(
            "test_txt_utf8_no_bom",
            Charset.forName("UTF-8"),
            "1,p2-Japanese:テスト",
            "2,p2-Japanese:テスト");
    Path p3 =
        createTestFile(
            "test_txt_utf8_bom",
            Charset.forName("UTF-8"),
            "\uFEFF1,p3-テストBOM",
            "\uFEFF2,p3-テストBOM");
    PCollection<String> contents =
        pipeline
            .apply("Create", Create.of(p1.toString(), p2.toString(), p3.toString()))
            .setCoder(StringUtf8Coder.of())
            // PCollection<String>
            .apply("Read file", new TextFileReadTransform());
    // PCollection<KV<String, String>>: tableName, line

    // Validate that the BOM bytes (\uFEFF) at the beginning of the first line have been removed.
    PAssert.that(contents)
        .containsInAnyOrder(
            "1,p1",
            "2,p1",
            "1,p2-Japanese:テスト",
            "2,p2-Japanese:テスト",
            "1,p3-テストBOM",
            "\uFEFF2,p3-テストBOM");

    pipeline.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testPreserveNonBOMBytes() throws Exception {
    // Contains \uFEFE, not UTF BOM.
    Path p1 =
        createTestFile(
            "test_txt_utf_bom", Charset.forName("UTF-8"), "\uFEFE1,p1テスト", "\uFEFE2,p1テスト");
    PCollection<String> contents =
        pipeline
            .apply("Create", Create.of(p1.toString()))
            .setCoder(StringUtf8Coder.of())
            // PCollection<String>
            .apply("Read file", new TextFileReadTransform());

    PAssert.that(contents).containsInAnyOrder("\uFEFE1,p1テスト", "\uFEFE2,p1テスト");

    pipeline.run();
  }

  private static class FileReadDoFn extends DoFn<ReadableFile, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      ReadableFile file = c.element();
      ValueProvider<String> filenameProvider =
          ValueProvider.StaticValueProvider.of(file.getMetadata().resourceId().getFilename());
      // Create a TextSource, passing null as the delimiter to use the default
      // delimiters ('\n', '\r', or '\r\n').
      TextSource textSource = new TextSource(filenameProvider, null, null);
      try {
        BoundedSource.BoundedReader<String> reader =
            textSource
                .createForSubrangeOfFile(file.getMetadata(), 0, file.getMetadata().sizeBytes())
                .createReader(c.getPipelineOptions());
        for (boolean more = reader.start(); more; more = reader.advance()) {
          c.output(reader.getCurrent());
        }
      } catch (IOException e) {
        throw new RuntimeException(
            "Unable to readFile: " + file.getMetadata().resourceId().toString());
      }
    }
  }

  /** A transform that reads CSV file records. */
  private static class TextFileReadTransform
      extends PTransform<PCollection<String>, PCollection<String>> {
    public TextFileReadTransform() {}

    @Override
    public PCollection<String> expand(PCollection<String> files) {
      return files
          // PCollection<String>
          .apply(FileIO.matchAll().withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW))
          // PCollection<Match.Metadata>
          .apply(FileIO.readMatches())
          // PCollection<FileIO.ReadableFile>
          .apply("Read lines", ParDo.of(new FileReadDoFn()));
      // PCollection<String>: line
    }
  }

  private Path createTestFile(String filename, Charset charset, String... lines)
      throws IOException {
    Path path = Files.createTempFile(filename, ".csv");
    try (BufferedWriter writer = Files.newBufferedWriter(path, charset)) {
      for (String line : lines) {
        writer.write(line);
        writer.write('\n');
      }
    }
    return path;
  }
}
