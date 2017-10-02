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
import org.apache.tika.metadata.Metadata;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

/**
 * Tests TikaInput.
 */
public class TikaIOTest {
  private static final String PDF_FILE =
      "Combining\n\nApache Beam\n\nand\n\nApache Tika\n\ncan help to ingest\n\n"
      + "the content from the files\n\nin most known formats.";

  private static final String PDF_ZIP_FILE =
      "apache-beam-tika.pdf\n\n\nCombining\n\n\nApache Beam\n\n\nand\n\n\nApache Tika\n\n\n"
      + "can help to ingest\n\n\nthe content from the files\n\n\nin most known formats.";

  private static final String ODT_FILE =
      "Combining\nApache Beam\nand\nApache Tika\ncan help to ingest\n"
      + "the content from the files\nin most known formats.";

  private static final String ODT_FILE2 =
      "Open Office\nPDF\nExcel\nText\nScientific\nand other formats\nare supported.";

  @Rule
  public TestPipeline p = TestPipeline.create();

  @Test
  public void testReadPdfFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika.pdf").getPath();

    doTestReadFiles(resourcePath, new ParseResult(resourcePath, PDF_FILE));
  }

  @Test
  public void testReadZipPdfFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika-pdf.zip").getPath();

    doTestReadFiles(resourcePath, new ParseResult(resourcePath, PDF_ZIP_FILE));
  }

  @Test
  public void testReadOdtFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();

    doTestReadFiles(resourcePath, new ParseResult(resourcePath, ODT_FILE, getOdtMetadata()));
  }

  @Test
  public void testReadOdtFiles() throws IOException {
    String resourcePath1 = getClass().getResource("/apache-beam-tika1.odt").getPath();
    String resourcePath2 = getClass().getResource("/apache-beam-tika2.odt").getPath();
    String resourcePath = resourcePath1.replace("apache-beam-tika1", "*");

    doTestReadFiles(resourcePath, new ParseResult(resourcePath1, ODT_FILE, getOdtMetadata()),
        new ParseResult(resourcePath2, ODT_FILE2));
  }

  private void doTestReadFiles(String resourcePath, ParseResult... expectedResults)
    throws IOException {
    PCollection<ParseResult> output = p.apply("ParseFiles",
        TikaIO.read().from(resourcePath))
        .apply(ParDo.of(new FilterMetadataFn()));
    PAssert.that(output).containsInAnyOrder(expectedResults);
    p.run();
  }

  @Test
  public void testReadDamagedPdfFile() throws IOException {

    String resourcePath = getClass().getResource("/damaged.pdf").getPath();

    p.apply("ParseInvalidPdfFile",
        TikaIO.read().from(resourcePath));
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
        .withContentTypeHint("application/pdf");

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("tikaConfigPath", "tikaconfigpath"));
    assertThat(displayData, hasDisplayItem("inputMetadata",
        "[Content-Type=application/pdf]"));
    assertEquals(3, displayData.items().size());
  }

  @Test
  public void testReadDisplayDataWithDefaultOptions() {
    final String[] args = new String[]{"--input=/input/tika.pdf"};
    TikaIO.Read read = TikaIO.read().withOptions(createOptions(args));

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "/input/tika.pdf"));
    assertEquals(1, displayData.items().size());
  }
  @Test
  public void testReadDisplayDataWithCustomOptions() {
    final String[] args = new String[]{"--input=/input/tika.pdf",
                                       "--tikaConfigPath=/tikaConfigPath",
                                       "--contentTypeHint=application/pdf"};
    TikaIO.Read read = TikaIO.read().withOptions(createOptions(args));

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "/input/tika.pdf"));
    assertThat(displayData, hasDisplayItem("tikaConfigPath", "/tikaConfigPath"));
    assertThat(displayData, hasDisplayItem("inputMetadata",
        "[Content-Type=application/pdf]"));
    assertEquals(3, displayData.items().size());
  }

  private static TikaOptions createOptions(String[] args) {
    return PipelineOptionsFactory.fromArgs(args)
        .withValidation().as(TikaOptions.class);
  }

  static class FilterMetadataFn extends DoFn<ParseResult, ParseResult> {
    private static final long serialVersionUID = 6338014219600516621L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      ParseResult result = c.element();
      Metadata m = new Metadata();
      if (result.getFileLocation().endsWith("apache-beam-tika1.odt")) {
          m.set("Author", result.getMetadata().get("Author"));
      }
      ParseResult newResult = new ParseResult(result.getFileLocation(), result.getContent(), m);
      c.output(newResult);
    }
  }

  static Metadata getOdtMetadata() {
    Metadata m = new Metadata();
    m.set("Author", "BeamTikaUser");
    return m;
  }
}
