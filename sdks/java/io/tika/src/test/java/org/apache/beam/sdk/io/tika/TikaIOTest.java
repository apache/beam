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
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests TikaInput.
 */
public class TikaIOTest {
  private static final String PDF_FILE =
      "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
      + "Combining\n\nApache Beam\n\nand\n\nApache Tika\n\ncan help to ingest\n\n"
      + "the content from the files\n\nin most known formats.\n\n\n";

  private static final String PDF_ZIP_FILE =
      "\n\n\n\n\n\n\n\napache-beam-tika.pdf\n\n\nCombining\n\n\nApache Beam\n\n\n"
      + "and\n\n\nApache Tika\n\n\ncan help to ingest\n\n\nthe content from the files\n\n\n"
      + "in most known formats.\n\n\n\n\n\n\n";

  private static final String ODT_FILE =
      "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
      + "Combining\nApache Beam\nand\nApache Tika\ncan help to ingest\nthe content from the"
      + " files\nin most known formats.\n";

  private static final String ODT_FILE2 =
      "\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n"
      + "Open Office\nPDF\nExcel\nText\nScientific\nand other formats\nare supported.\n";

  @Rule
  public TestPipeline p = TestPipeline.create();
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testParseAllPdfFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika.pdf").getPath();

    doTestParseAllFiles(resourcePath, new ParseResult(resourcePath, PDF_FILE));
  }

  @Test
  public void testParseAllZipPdfFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika-pdf.zip").getPath();

    doTestParseAllFiles(resourcePath, new ParseResult(resourcePath, PDF_ZIP_FILE));
  }

  @Test
  public void testParseAllOdtFile() throws IOException {

    String resourcePath = getClass().getResource("/apache-beam-tika1.odt").getPath();

    doTestParseAllFiles(resourcePath, new ParseResult(resourcePath, ODT_FILE, getOdtMetadata()));
  }

  @Test
  public void testParseAllOdtFiles() throws IOException {
    String resourcePath1 = getClass().getResource("/apache-beam-tika1.odt").getPath();
    String resourcePath2 = getClass().getResource("/apache-beam-tika2.odt").getPath();
    String resourcePath = resourcePath1.replace("apache-beam-tika1", "*");

    doTestParseAllFiles(resourcePath, new ParseResult(resourcePath1, ODT_FILE, getOdtMetadata()),
        new ParseResult(resourcePath2, ODT_FILE2));
  }

  private void doTestParseAllFiles(String resourcePath, ParseResult... expectedResults)
     throws IOException {
    PCollection<ParseResult> output =
        p.apply("ParseFiles", FileIO.match().filepattern(resourcePath))
        .apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
        .apply(TikaIO.parseAll())
        .apply(ParDo.of(new FilterMetadataFn()));
    PAssert.that(output).containsInAnyOrder(expectedResults);
    p.run();
  }

  @Test
  public void testParseAllDamagedPdfFile() throws IOException {
    thrown.expectCause(isA(TikaException.class));
    String resourcePath = getClass().getResource("/damaged.pdf").getPath();

    p.apply("ParseInvalidPdfFile", FileIO.match().filepattern(resourcePath))
      .apply(FileIO.readMatches())
      .apply(TikaIO.parseAll());
    p.run();
  }

  @Test
  public void testParseAllDisplayData() {
    TikaIO.ParseAll parseAll = TikaIO.parseAll()
        .withTikaConfigPath("tikaconfigpath")
        .withContentTypeHint("application/pdf");

    DisplayData displayData = DisplayData.from(parseAll);

    assertThat(displayData, hasDisplayItem("tikaConfigPath", "tikaconfigpath"));
    assertThat(displayData, hasDisplayItem("inputMetadata",
        "Content-Type=application/pdf"));
    assertEquals(2, displayData.items().size());
  }

  @Test
  public void testParseAllDisplayDataWithCustomOptions() {
    TikaIO.ParseAll parseAll = TikaIO.parseAll()
        .withTikaConfigPath("/tikaConfigPath")
        .withContentTypeHint("application/pdf");

    DisplayData displayData = DisplayData.from(parseAll);

    assertThat(displayData, hasDisplayItem("tikaConfigPath", "/tikaConfigPath"));
    assertThat(displayData, hasDisplayItem("inputMetadata",
        "Content-Type=application/pdf"));
    assertEquals(2, displayData.items().size());
  }

  static class FilterMetadataFn extends DoFn<ParseResult, ParseResult> {
    private static final long serialVersionUID = 6338014219600516621L;

    @ProcessElement
    public void processElement(ProcessContext c) {
      ParseResult result = c.element();
      Metadata m = new Metadata();
      // Files contain many metadata properties. This function drops all but the"Author"
      // property manually added to "apache-beam-tika1.odt" resource only to make
      // the tests simpler
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
