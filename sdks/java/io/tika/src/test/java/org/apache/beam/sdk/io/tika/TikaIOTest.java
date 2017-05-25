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
package org.apache.beam.sdk.io.tika;

import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tika.exception.TikaException;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests TikaInput.
 */
public class TikaIOTest {
  private static final String[] PDF_FILE = new String[] {
      "Combining", "can help to ingest", "Apache Beam", "in most known formats.",
      "the content from the files", "and", "Apache Tika"
  };
  private static final String[] PDF_ZIP_FILE = new String[] {
      "Combining", "can help to ingest", "Apache Beam", "in most known formats.",
      "the content from the files", "and", "Apache Tika",
      "apache-beam-tika.pdf"
  };
  private static final String[] ODT_FILE = new String[] {
      "Combining", "can help to ingest", "Apache", "Beam", "in most known formats.",
      "the content from the files", "and", "Apache Tika"
  };
  private static final String[] ODT_FILE_WITH_METADATA = new String[] {
      "Combining", "can help to ingest", "Apache", "Beam", "in most known formats.",
      "the content from the files", "and", "Apache Tika",
      "Author=BeamTikaUser"
  };
  private static final String[] ODT_FILES = new String[] {
      "Combining", "can help to ingest", "Apache", "Beam", "in most known formats.",
      "the content from the files", "and", "Apache Tika",
      "Open Office", "Text", "PDF", "Excel", "Scientific",
      "and other formats", "are supported."
  };

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testReadPdfFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika.pdf").getPath();

    doTestReadFiles(resourcePath, PDF_FILE);
  }

  @Test
  public void testReadZipPdfFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika-pdf.zip").getPath();

    doTestReadFiles(resourcePath, PDF_ZIP_FILE);
  }

  @Test
  public void testReadOdtFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();

    doTestReadFiles(resourcePath, ODT_FILE);
  }

  @Test
  public void testReadOdtFiles() throws IOException {
    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();
    resourcePath = resourcePath.replace("apache-beam-tika1", "*");

    doTestReadFiles(resourcePath, ODT_FILES);
  }

  private void doTestReadFiles(String resourcePath, String[] expected) throws IOException {
    PCollection<String> output = p.apply("ParseFiles", TikaIO.read().from(resourcePath));
    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testReadOdtFileWithMetadata() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();

    PCollection<String> output = p.apply("ParseOdtFile",
        TikaIO.read().from(resourcePath).withReadOutputMetadata(true))
        .apply(ParDo.of(new FilterMetadataFn()));
    PAssert.that(output).containsInAnyOrder(ODT_FILE_WITH_METADATA);
    p.run();
  }

  @Test
  public void testReadPdfFileSync() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika.pdf").getPath();

    PCollection<String> output = p.apply("ParsePdfFile",
        TikaIO.read().from(resourcePath).withParseSynchronously(true));
    PAssert.that(output).containsInAnyOrder(PDF_FILE);
    p.run();
  }

  @Test
  public void testReadDamagedPdfFile() throws IOException {

    doTestReadDamagedPdfFile(false);
  }

  @Test
  public void testReadDamagedPdfFileSync() throws IOException {
    doTestReadDamagedPdfFile(true);
  }

  private void doTestReadDamagedPdfFile(boolean sync) throws IOException {

    String resourcePath = getClass().getResource("/damaged.pdf").getPath();

    p.apply("ParseInvalidPdfFile",
        TikaIO.read().from(resourcePath).withParseSynchronously(sync));
    try {
        p.run();
        fail("Transform failure is expected");
    } catch (RuntimeException ex) {
      assertTrue(ex.getCause().getCause() instanceof TikaException);
    }
  }

  @Test
  public void testReadDisplayData() {
    TikaIO.Read read = TikaIO.read()
        .from("foo.*")
        .withTikaConfigPath("tikaconfigpath")
        .withContentTypeHint("application/pdf")
        .withReadOutputMetadata(true);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("tikaConfigPath", "tikaconfigpath"));
    assertThat(displayData, hasDisplayItem("inputMetadata",
        "[Content-Type=application/pdf]"));
    assertThat(displayData, hasDisplayItem("readOutputMetadata", "true"));
    assertThat(displayData, hasDisplayItem("parseMode", "asynchronous"));
    assertThat(displayData, hasDisplayItem("queuePollTime", "50"));
    assertThat(displayData, hasDisplayItem("queueMaxPollTime", "3000"));
    assertEquals(7, displayData.items().size());
  }

  @Test
  public void testReadDisplayDataSyncMode() {
    TikaIO.Read read = TikaIO.read()
        .from("foo.*")
        .withParseSynchronously(true);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("parseMode", "synchronous"));
    assertEquals(2, displayData.items().size());
  }

  @Test
  public void testReadDisplayDataWithDefaultOptions() {
    final String[] args = new String[]{"--input=/input/tika.pdf"};
    TikaIO.Read read = TikaIO.read().withOptions(createOptions(args));

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "/input/tika.pdf"));
    assertThat(displayData, hasDisplayItem("parseMode", "asynchronous"));
    assertThat(displayData, hasDisplayItem("queuePollTime", "50"));
    assertThat(displayData, hasDisplayItem("queueMaxPollTime", "3000"));
    assertEquals(4, displayData.items().size());
  }
  @Test
  public void testReadDisplayDataWithCustomOptions() {
    final String[] args = new String[]{"--input=/input/tika.pdf",
                                       "--tikaConfigPath=/tikaConfigPath",
                                       "--queuePollTime=10",
                                       "--queueMaxPollTime=1000",
                                       "--contentTypeHint=application/pdf",
                                       "--readOutputMetadata=true"};
    TikaIO.Read read = TikaIO.read().withOptions(createOptions(args));

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "/input/tika.pdf"));
    assertThat(displayData, hasDisplayItem("tikaConfigPath", "/tikaConfigPath"));
    assertThat(displayData, hasDisplayItem("parseMode", "asynchronous"));
    assertThat(displayData, hasDisplayItem("queuePollTime", "10"));
    assertThat(displayData, hasDisplayItem("queueMaxPollTime", "1000"));
    assertThat(displayData, hasDisplayItem("inputMetadata",
        "[Content-Type=application/pdf]"));
    assertThat(displayData, hasDisplayItem("readOutputMetadata", "true"));
    assertEquals(7, displayData.items().size());
  }

  private static TikaOptions createOptions(String[] args) {
    return PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(TikaOptions.class);
  }

  static class FilterMetadataFn extends DoFn<String, String> {
    private static final long serialVersionUID = 6338014219600516621L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      String word = c.element();
      if (word.contains("=") && !word.startsWith("Author")) {
        return;
      }
      c.output(word);
    }
  }
}
