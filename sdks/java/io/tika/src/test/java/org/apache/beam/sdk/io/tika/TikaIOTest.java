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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TikaIO}. */
@RunWith(JUnit4.class)
public class TikaIOTest implements Serializable {
  private static final String PDF_ZIP_FILE =
      "\n\n\n\n\n\n\n\napache-beam-tika.pdf\n\n\nCombining\n\n\nApache Beam\n\n\n"
          + "and\n\n\nApache Tika\n\n\ncan help to ingest\n\n\nthe content from the files\n\n\n"
          + "in most known formats.\n\n\n\n\n\n\n";

  private static final String ODT_FILE =
      "\n\n\n\n\n\n\n"
          + "Combining\nApache Beam\nand\nApache Tika\ncan help to ingest\nthe content from the"
          + " files\nin most known formats.\n";

  @Rule public transient TestPipeline p = TestPipeline.create();

  @Test
  public void testParseAndParseFiles() throws IOException {
    Path root =
        Paths.get(getClass().getResource("/valid/apache-beam-tika.odt").getPath()).getParent();

    List<ParseResult> expected =
        Arrays.asList(
            ParseResult.success(
                root.resolve("apache-beam-tika.odt").toString(), ODT_FILE, getOdtMetadata()),
            ParseResult.success(root.resolve("apache-beam-tika-pdf.zip").toString(), PDF_ZIP_FILE));

    PCollection<ParseResult> parse =
        p.apply("Parse", TikaIO.parse().filepattern(root.resolve("*").toString()))
            .apply("FilterParse", ParDo.of(new FilterMetadataFn()));
    PAssert.that(parse).containsInAnyOrder(expected);

    PCollection<ParseResult> parseFiles =
        p.apply("ParseFiles", FileIO.match().filepattern(root.resolve("*").toString()))
            .apply(FileIO.readMatches().withCompression(Compression.UNCOMPRESSED))
            .apply(TikaIO.parseFiles())
            .apply("FilterParseFiles", ParDo.of(new FilterMetadataFn()));
    PAssert.that(parseFiles).containsInAnyOrder(expected);
    p.run();
  }

  private static Metadata getOdtMetadata() {
    Metadata m = new Metadata();
    m.set("Author", "BeamTikaUser");
    return m;
  }

  private static class FilterMetadataFn extends DoFn<ParseResult, ParseResult> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      ParseResult result = c.element();
      Metadata m = new Metadata();
      // Files contain many metadata properties. This function drops all but the "Author"
      // property manually added to "apache-beam-tika.odt" resource only to make
      // the tests simpler
      if (result.getFileLocation().endsWith("valid/apache-beam-tika.odt")) {
        m.set("Author", result.getMetadata().get("Author"));
      }
      ParseResult newResult = ParseResult.success(result.getFileLocation(), result.getContent(), m);
      c.output(newResult);
    }
  }

  @Test
  public void testParseDamagedPdfFile() throws IOException {
    String path = getClass().getResource("/damaged.pdf").getPath();
    PCollection<ParseResult> res = p.apply("ParseInvalidPdfFile", TikaIO.parse().filepattern(path));

    PAssert.thatSingleton(res)
        .satisfies(
            input -> {
              assertEquals(path, input.getFileLocation());
              assertFalse(input.isSuccess());
              assertTrue(input.getError() instanceof TikaException);
              return null;
            });
    p.run();
  }

  @Test
  public void testParseDisplayData() {
    TikaIO.Parse parse = TikaIO.parse().filepattern("file.pdf");

    DisplayData displayData = DisplayData.from(parse);

    assertThat(displayData, hasDisplayItem("filePattern", "file.pdf"));
    assertEquals(1, displayData.items().size());
  }

  @Test
  public void testParseFilesDisplayData() {
    TikaIO.ParseFiles parseFiles =
        TikaIO.parseFiles()
            .withTikaConfigPath("/tikaConfigPath")
            .withContentTypeHint("application/pdf");

    DisplayData displayData = DisplayData.from(parseFiles);

    assertThat(displayData, hasDisplayItem("tikaConfigPath", "/tikaConfigPath"));
    assertThat(displayData, hasDisplayItem("contentTypeHint", "application/pdf"));
  }
}
