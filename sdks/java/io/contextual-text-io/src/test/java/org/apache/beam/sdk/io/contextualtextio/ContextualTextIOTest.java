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
package org.apache.beam.sdk.io.contextualtextio;

import static java.nio.charset.StandardCharsets.UTF_8;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;
import static org.apache.beam.sdk.io.Compression.AUTO;
import static org.apache.beam.sdk.io.Compression.BZIP2;
import static org.apache.beam.sdk.io.Compression.DEFLATE;
import static org.apache.beam.sdk.io.Compression.GZIP;
import static org.apache.beam.sdk.io.Compression.UNCOMPRESSED;
import static org.apache.beam.sdk.io.Compression.ZIP;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileBasedSource;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesUnboundedSplittableParDo;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

/** Tests for {@link ContextualTextIO.Read}. */
public class ContextualTextIOTest {
  private static final int NUM_LINES_FOR_LARGE = 1024;

  private static final List<String> EMPTY = Collections.emptyList();

  private static final List<String> TINY = Arrays.asList("ABC", "DEF", "HIJ");

  private static final List<String> LARGE = makeLines(NUM_LINES_FOR_LARGE);

  private static File writeToFile(
      List<String> lines, TemporaryFolder folder, String fileName, Compression compression)
      throws IOException {
    File file = folder.getRoot().toPath().resolve(fileName).toFile();
    OutputStream output = new FileOutputStream(file);
    switch (compression) {
      case UNCOMPRESSED:
        break;
      case GZIP:
        output = new GZIPOutputStream(output);
        break;
      case BZIP2:
        output = new BZip2CompressorOutputStream(output);
        break;
      case ZIP:
        ZipOutputStream zipOutput = new ZipOutputStream(output);
        zipOutput.putNextEntry(new ZipEntry("entry"));
        output = zipOutput;
        break;
      case DEFLATE:
        output = new DeflateCompressorOutputStream(output);
        break;
      default:
        throw new UnsupportedOperationException(compression.toString());
    }
    writeToStreamAndClose(lines, output);
    return file;
  }

  /**
   * Helper that writes the given lines (adding a newline in between) to a stream, then closes the
   * stream.
   */
  private static void writeToStreamAndClose(List<String> lines, OutputStream outputStream) {
    try (PrintStream writer = new PrintStream(outputStream)) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

  /** Helper to make an array of compressible strings. Returns ["line" i] for i in range(0,n). */
  private static List<String> makeLines(int n) {
    List<String> lines = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      lines.add("Line " + i);
    }
    return lines;
  }

  private static class ConvertRecordWithMetadataToString extends DoFn<RecordWithMetadata, String> {
    @ProcessElement
    public void processElement(@Element RecordWithMetadata record, OutputReceiver<String> out) {
      String file = record.getFileName().substring(record.getFileName().lastIndexOf('/') + 1);
      out.output(file + " " + record.getRecordNum() + " " + record.getRecordValue());
    }
  }

  /**
   * Helper method that runs a variety of ways to read a single file using ContextualTextIO and
   * checks that they all match the given expected output.
   *
   * <p>The transforms being verified are:
   *
   * <ul>
   *   <li>ContextualTextIO.read().from(filename).withCompression(compressionType).withHintMatchesManyFiles()
   *   <li>ContextualTextIO.read().from(filename).withCompression(compressionType)
   *   <li>ContextualTextIO.read().from(filename).withCompression(compressionType).withHasMultilineCSV(true)
   *   <li>ContextualTextIO.readFiles().withCompression(compressionType)
   * </ul>
   */
  private static void assertReadingCompressedFileMatchesExpected(
      File file, Compression compression, List<String> expected, Pipeline p) {

    ContextualTextIO.Read read =
        ContextualTextIO.read().from(file.getPath()).withCompression(compression);

    // Convert the expected output into RecordWithMetadata output Format
    List<String> expectedOutput = new ArrayList<>();
    for (int lineNum = 0; lineNum < expected.size(); ++lineNum) {
      expectedOutput.add(file.getName() + " " + lineNum + " " + expected.get(lineNum));
    }

    PAssert.that(
            p.apply("Read_" + file + "_" + compression.toString(), read)
                .apply(
                    "ConvertRecordWithMetadataToString",
                    ParDo.of(new ConvertRecordWithMetadataToString())))
        .containsInAnyOrder(expectedOutput);
    PAssert.that(
            p.apply(
                    "Read_" + file + "_" + compression.toString() + "_many",
                    read.withHintMatchesManyFiles())
                .apply(
                    "ConvertRecordWithMetadataToString" + "_many",
                    ParDo.of(new ConvertRecordWithMetadataToString())))
        .containsInAnyOrder(expectedOutput);

    PAssert.that(
            p.apply(
                    "Read_" + file + "_" + compression.toString() + "_withRFC4180",
                    read.withHasMultilineCSVRecords(true))
                .apply(
                    "ConvertRecordWithMetadataToString" + "_withRFC4180",
                    ParDo.of(new ConvertRecordWithMetadataToString())))
        .containsInAnyOrder(expectedOutput);

    PAssert.that(
            p.apply("Create_Paths_ReadFiles_" + file, Create.of(file.getPath()))
                .apply("Match_" + file, FileIO.matchAll())
                .apply("ReadMatches_" + file, FileIO.readMatches().withCompression(compression))
                .apply("ReadFiles_" + compression.toString(), ContextualTextIO.readFiles())
                .apply(
                    "ConvertRecordWithMetadataToStringWithFileIO",
                    ParDo.of(new ConvertRecordWithMetadataToString())))
        .containsInAnyOrder(expectedOutput);
  }

  /**
   * Create a zip file with the given lines.
   *
   * @param expected A list of expected lines, populated in the zip file.
   * @param folder A temporary folder used to create files.
   * @param filename Optionally zip file name (can be null).
   * @param fieldsEntries Fields to write in zip entries.
   * @return The zip filename.
   * @throws Exception In case of a failure during zip file creation.
   */
  private static File createZipFile(
      List<String> expected, TemporaryFolder folder, String filename, String[]... fieldsEntries)
      throws Exception {
    File tmpFile = folder.getRoot().toPath().resolve(filename).toFile();

    ZipOutputStream out = new ZipOutputStream(new FileOutputStream(tmpFile));
    PrintStream writer = new PrintStream(out, true /* auto-flush on write */);

    int index = 0;
    for (String[] entry : fieldsEntries) {
      out.putNextEntry(new ZipEntry(Integer.toString(index)));
      for (String field : entry) {
        writer.println(field);
        expected.add(field);
      }
      out.closeEntry();
      index++;
    }

    writer.close();
    out.close();

    return tmpFile;
  }

  private static ContextualTextIOSource prepareSource(
      TemporaryFolder temporaryFolder, byte[] data, byte[] delimiter, boolean hasRFC4180Multiline)
      throws IOException {
    Path path = temporaryFolder.newFile().toPath();
    Files.write(path, data);
    return new ContextualTextIOSource(
        ValueProvider.StaticValueProvider.of(path.toString()),
        EmptyMatchTreatment.DISALLOW,
        delimiter,
        hasRFC4180Multiline);
  }

  private static String getFileSuffix(Compression compression) {
    switch (compression) {
      case UNCOMPRESSED:
        return ".txt";
      case GZIP:
        return ".gz";
      case BZIP2:
        return ".bz2";
      case ZIP:
        return ".zip";
      case DEFLATE:
        return ".deflate";
      default:
        return "";
    }
  }
  /** Tests for reading from different size of files with various Compression. */
  @RunWith(Parameterized.class)
  public static class CompressedReadTest {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule public TestPipeline p = TestPipeline.create();

    @Parameterized.Parameters(name = "{index}: {1}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {EMPTY, UNCOMPRESSED})
          .add(new Object[] {EMPTY, GZIP})
          .add(new Object[] {EMPTY, BZIP2})
          .add(new Object[] {EMPTY, ZIP})
          .add(new Object[] {EMPTY, DEFLATE})
          .add(new Object[] {TINY, UNCOMPRESSED})
          .add(new Object[] {TINY, GZIP})
          .add(new Object[] {TINY, BZIP2})
          .add(new Object[] {TINY, ZIP})
          .add(new Object[] {TINY, DEFLATE})
          .add(new Object[] {LARGE, UNCOMPRESSED})
          .add(new Object[] {LARGE, GZIP})
          .add(new Object[] {LARGE, BZIP2})
          .add(new Object[] {LARGE, ZIP})
          .add(new Object[] {LARGE, DEFLATE})
          .build();
    }

    @Parameterized.Parameter(0)
    public List<String> lines;

    @Parameterized.Parameter(1)
    public Compression compression;

    /** Tests reading from a small, compressed file with no extension. */
    @Test
    @Category(NeedsRunner.class)
    public void testCompressedReadWithoutExtension() throws Exception {
      String fileName = lines.size() + "_" + compression + "_no_extension";
      File fileWithNoExtension = writeToFile(lines, tempFolder, fileName, compression);
      assertReadingCompressedFileMatchesExpected(fileWithNoExtension, compression, lines, p);
      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testCompressedReadWithExtension() throws Exception {
      String fileName =
          lines.size() + "_" + compression + "_no_extension" + getFileSuffix(compression);
      File fileWithExtension = writeToFile(lines, tempFolder, fileName, compression);

      // Sanity check that we're properly testing compression.
      if (lines.size() == NUM_LINES_FOR_LARGE && !compression.equals(UNCOMPRESSED)) {
        File uncompressedFile = writeToFile(lines, tempFolder, "large.txt", UNCOMPRESSED);
        assertThat(uncompressedFile.length(), greaterThan(fileWithExtension.length()));
      }

      assertReadingCompressedFileMatchesExpected(fileWithExtension, compression, lines, p);
      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadWithAuto() throws Exception {
      // Files with non-compressed extensions should work in AUTO and UNCOMPRESSED modes.
      String fileName =
          lines.size() + "_" + compression + "_no_extension" + getFileSuffix(compression);
      File fileWithExtension = writeToFile(lines, tempFolder, fileName, compression);
      assertReadingCompressedFileMatchesExpected(fileWithExtension, AUTO, lines, p);
      p.run();
    }
  }

  /** Tests for reading files with various delimiters. */
  @RunWith(Parameterized.class)
  public static class ReadWithDelimiterTest {
    private static final ImmutableList<String> EXPECTED = ImmutableList.of("asdf", "hjkl", "xyz");
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          //          .add(new Object[] {"\n\n\n", ImmutableList.of("", "", "")})
          .add(new Object[] {"asdf\nhjkl\nxyz\n", EXPECTED})
          .add(new Object[] {"asdf\rhjkl\rxyz\r", EXPECTED})
          .add(new Object[] {"asdf\r\nhjkl\r\nxyz\r\n", EXPECTED})
          .add(new Object[] {"asdf\rhjkl\r\nxyz\n", EXPECTED})
          .add(new Object[] {"asdf\nhjkl\nxyz", EXPECTED})
          .add(new Object[] {"asdf\rhjkl\rxyz", EXPECTED})
          .add(new Object[] {"asdf\r\nhjkl\r\nxyz", EXPECTED})
          .add(new Object[] {"asdf\rhjkl\r\nxyz", EXPECTED})
          .build();
    }

    @Parameterized.Parameter(0)
    public String line;

    @Parameterized.Parameter(1)
    public ImmutableList<String> expected;

    @Test
    public void testReadLinesWithDelimiter() throws Exception {
      runTestReadWithData(line.getBytes(UTF_8), expected);
    }

    private ContextualTextIOSource prepareSource(byte[] data, boolean hasRFC4180Multiline)
        throws IOException {
      return ContextualTextIOTest.prepareSource(tempFolder, data, null, hasRFC4180Multiline);
    }

    private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
      ContextualTextIOSource source = prepareSource(data, false);
      List<RecordWithMetadata> actual =
          SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
      List<String> actualOutput = new ArrayList<>();
      actual.forEach(
          (RecordWithMetadata L) -> {
            actualOutput.add(L.getRecordValue());
          });
      assertThat(
          actualOutput,
          containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
    }
  }

  @RunWith(Parameterized.class)
  public static class ReadWithDelimiterAndRFC4180 {
    static final ImmutableList<String> EXPECTED = ImmutableList.of("\"asdf\nhjkl\nmnop\"", "xyz");
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {"\n\n\n", ImmutableList.of("", "", "")})
          .add(new Object[] {"\"asdf\nhjkl\"\nxyz\n", ImmutableList.of("\"asdf\nhjkl\"", "xyz")})
          .add(new Object[] {"\"asdf\nhjkl\nmnop\"\nxyz\n", EXPECTED})
          .add(new Object[] {"\"asdf\nhjkl\nmnop\"\nxyz\r", EXPECTED})
          .add(new Object[] {"\"asdf\nhjkl\nmnop\"\r\nxyz\n", EXPECTED})
          .add(new Object[] {"\"asdf\nhjkl\nmnop\"\r\nxyz\r\n", EXPECTED})
          .add(new Object[] {"\"asdf\nhjkl\nmnop\"\rxyz\r\n", EXPECTED})
          .build();
    }

    @Parameterized.Parameter(0)
    public String line;

    @Parameterized.Parameter(1)
    public ImmutableList<String> expected;

    @Test
    public void testReadLinesWithDelimiter() throws Exception {
      runTestReadWithData(line.getBytes(UTF_8), expected);
    }

    private ContextualTextIOSource prepareSource(byte[] data, boolean hasRFC4180Multiline)
        throws IOException {
      return ContextualTextIOTest.prepareSource(tempFolder, data, null, hasRFC4180Multiline);
    }

    private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
      ContextualTextIOSource source = prepareSource(data, true);
      List<RecordWithMetadata> actual =
          SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
      List<String> actualOutput = new ArrayList<>();
      actual.forEach(
          (RecordWithMetadata L) -> {
            actualOutput.add(L.getRecordValue());
          });
      assertThat(
          actualOutput,
          containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
    }
  }

  /** Tests Specific for checking functionality of ContextualTextIO. */
  @RunWith(JUnit4.class)
  public static class ContextualTextIOSpecificTests {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule public TestPipeline p = TestPipeline.create();

    public static final char CR = (char) 0x0D;
    public static final char LF = (char) 0x0A;

    public static final String CRLF = "" + CR + LF;

    public String createFileFromList(List<String> input) throws Exception {

      File tmpFile = tempFolder.newFile();
      String filename = tmpFile.getPath();

      try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
        for (String elem : input) {
          byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
          String line = new String(encodedElem, Charsets.UTF_8);
          writer.println(line);
        }
      }
      return filename;
    }

    @Test
    @Category(NeedsRunner.class)
    public void multipleFilesTest() throws Exception {
      List<File> files =
          Arrays.asList(
              tempFolder.newFile("File1"),
              tempFolder.newFile("File2"),
              tempFolder.newFile("File3"));

      int numLines = 10;
      for (File tmpFile : files) {
        numLines += 2;
        String filename = tmpFile.getPath();
        try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
          for (int lineNum = 0; lineNum < numLines; ++lineNum) {
            String elem = filename + " " + lineNum;
            byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
            String line = new String(encodedElem, Charsets.UTF_8);
            writer.println(line);
          }
        }
      }
      String filePath = tempFolder.getRoot().toPath() + "/*";
      p.apply(ContextualTextIO.read().from(filePath))
          .apply(
              MapElements.into(strings())
                  .via(
                      (RecordWithMetadata L) -> {
                        String expectedLineNum =
                            L.getRecordValue().substring(L.getRecordValue().lastIndexOf(' ') + 1);
                        assertEquals(Long.parseLong(expectedLineNum), (long) L.getRecordNum());
                        return "";
                      }));

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testWithHintMatchesManyFiles() throws IOException {
      List<File> files =
          Arrays.asList(
              tempFolder.newFile("File1"),
              tempFolder.newFile("File2"),
              tempFolder.newFile("File3"));

      int num = 0;
      for (File tmpFile : files) {
        num += 2;
        String filename = tmpFile.getPath();
        try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
          for (int lineNum = 0; lineNum < 10 + num; ++lineNum) {
            String elem = filename + " " + lineNum;
            byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
            String line = new String(encodedElem, Charsets.UTF_8);
            writer.println(line);
          }
        }
      }
      String filePath = tempFolder.getRoot().toPath() + "/*";
      p.apply(ContextualTextIO.read().from(filePath).withHintMatchesManyFiles())
          .apply(
              MapElements.into(strings())
                  .via(
                      (RecordWithMetadata L) -> {
                        String expectedLineNum =
                            L.getRecordValue().substring(L.getRecordValue().lastIndexOf(' ') + 1);
                        assertEquals(Long.parseLong(expectedLineNum), (long) L.getRecordNum());
                        return "";
                      }));

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void runBasicReadTest() throws Exception {

      List<String> input = ImmutableList.of("1", "2");
      ContextualTextIO.Read read = ContextualTextIO.read().from(createFileFromList(input));
      PCollection<RecordWithMetadata> output = p.apply(read);

      PCollection<String> result =
          output.apply(MapElements.into(strings()).via(x -> String.valueOf(x.getRecordValue())));

      PAssert.that(result).containsInAnyOrder("1", "2");

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void runBasicReadTestWithRFC4180Set() throws Exception {

      List<String> input = ImmutableList.of("1", "2");

      ContextualTextIO.Read read =
          ContextualTextIO.read().from(createFileFromList(input)).withHasMultilineCSVRecords(true);
      PCollection<RecordWithMetadata> output = p.apply(read);

      PCollection<String> result =
          output.apply(MapElements.into(strings()).via(x -> String.valueOf(x.getRecordValue())));

      PAssert.that(result).containsInAnyOrder("1", "2");

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    /** Test to read files with using MultiLine columns as per RFC4180 */
    public void runSmallRFC4180MultiLineReadTest() throws Exception {

      // Generate lines of format "1\n1" where number changes per line.
      List<String> input =
          IntStream.range(0, 2)
              .<String>mapToObj(x -> "\"" + x + CRLF + x + "\"")
              .collect(Collectors.toList());

      ContextualTextIO.Read read =
          ContextualTextIO.read().from(createFileFromList(input)).withHasMultilineCSVRecords(true);
      PCollection<RecordWithMetadata> output = p.apply(read);

      PCollection<String> result =
          output.apply(
              MapElements.into(strings())
                  .via(
                      x -> {
                        return String.valueOf(x.getRecordValue());
                      }));

      PAssert.that(result).containsInAnyOrder(input);

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    /** Test to read files with using MultiLine columns as per RFC4180 */
    public void runSmallRFC4180EscapedCharcatersReadTest() throws Exception {

      // Generate lines of format  "aaa","b""bb","ccc" where number changes per line.
      List<String> input =
          IntStream.range(0, 2)
              .<String>mapToObj(x -> "\"aaa\",\"b\"\"bb\",\"ccc\"")
              .collect(Collectors.toList());

      ContextualTextIO.Read read =
          ContextualTextIO.read().from(createFileFromList(input)).withHasMultilineCSVRecords(true);
      PCollection<RecordWithMetadata> output = p.apply(read);

      PCollection<String> result =
          output.apply(
              MapElements.into(strings())
                  .via(
                      x -> {
                        return String.valueOf(x.getRecordValue());
                      }));

      PAssert.that(result).containsInAnyOrder(input);

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    /** Test to read files with using MultiLine columns as per RFC4180 */
    public void runLargeRFC4180MultiLineReadTest() throws Exception {

      // Generate lines of format "1\n1" where number changes per line.
      List<String> input =
          IntStream.range(0, 1000)
              .<String>mapToObj(x -> "\"" + x + CRLF + x + "\"")
              .collect(Collectors.toList());

      ContextualTextIO.Read read =
          ContextualTextIO.read().from(createFileFromList(input)).withHasMultilineCSVRecords(true);
      PCollection<RecordWithMetadata> output = p.apply(read);

      PCollection<String> result =
          output.apply(MapElements.into(strings()).via(x -> String.valueOf(x.getRecordValue())));

      PAssert.that(result).containsInAnyOrder(input);

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    /** Test to read files with using MultiLine columns as per RFC4180 */
    public void runLargeRFC4180MultiLineAndEscapedReadTest() throws Exception {

      // Generate lines of format  "aaa","b""\nbb","ccc","""\nHello" where number changes per line.
      List<String> input =
          IntStream.range(0, 1000)
              .<String>mapToObj(
                  x -> "\"a" + CRLF + "aa\",\"b\"\"" + CRLF + "bb\",\"ccc\",\"\"\"\\nHello\"")
              .collect(Collectors.toList());

      ContextualTextIO.Read read =
          ContextualTextIO.read().from(createFileFromList(input)).withHasMultilineCSVRecords(true);
      PCollection<RecordWithMetadata> output = p.apply(read);

      PCollection<String> result =
          output.apply(MapElements.into(strings()).via(x -> String.valueOf(x.getRecordValue())));

      PAssert.that(result).containsInAnyOrder(input);

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    /** Test to read files with using MultiLine columns as per RFC4180 */
    public void testFileNameIsPreserved() throws Exception {

      List<String> input =
          IntStream.range(1, 1000)
              .<String>mapToObj(x -> Integer.toString(x))
              .collect(Collectors.toList());

      ContextualTextIO.Read read =
          ContextualTextIO.read().from(createFileFromList(input)).withHasMultilineCSVRecords(true);
      PCollection<RecordWithMetadata> output = p.apply(read);

      PCollection<String> result =
          output.apply(MapElements.into(strings()).via(x -> String.valueOf(x.getRecordValue())));

      PAssert.that(result).containsInAnyOrder(input);

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testFileNameIsPreservedWithoutLineMetadata() throws Exception {

      List<String> input =
          IntStream.range(1, 1000)
              .<String>mapToObj(x -> Integer.toString(x))
              .collect(Collectors.toList());

      ContextualTextIO.Read read =
          ContextualTextIO.read()
              .from(createFileFromList(input))
              .withHasMultilineCSVRecords(true)
              .withoutRecordNumMetadata();
      PCollection<RecordWithMetadata> output = p.apply(read);

      PCollection<String> result =
          output.apply(MapElements.into(strings()).via(x -> String.valueOf(x.getRecordValue())));

      PAssert.that(result).containsInAnyOrder(input);

      p.run();
    }
  }

  /** Tests for some basic operations in {@link ContextualTextIO.Read}. */
  @RunWith(JUnit4.class)
  public static class BasicIOTest {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule public TestPipeline p = TestPipeline.create();

    public static class GetLines extends DoFn<RecordWithMetadata, String> {
      @ProcessElement
      public void processElement(@Element RecordWithMetadata record, OutputReceiver<String> out) {
        out.output(record.getRecordValue());
      }
    }

    public static class GetDetails extends DoFn<RecordWithMetadata, String> {
      @ProcessElement
      public void processElement(@Element RecordWithMetadata record, OutputReceiver<String> out) {
        out.output(
            record.getFileName() + " " + record.getRecordNum() + " " + record.getRecordValue());
      }
    }

    private void runTestRead(String[] expected) throws Exception {
      File tmpFile = tempFolder.newFile();
      String filename = tmpFile.getPath();

      try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
        for (String elem : expected) {
          byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
          String line = new String(encodedElem, Charsets.UTF_8);
          writer.println(line);
        }
      }

      ContextualTextIO.Read read = ContextualTextIO.read().from(filename);
      PCollection<String> output =
          p.apply(read)
              .apply(MapElements.into(strings()).via((RecordWithMetadata L) -> L.getRecordValue()));

      PAssert.that(output).containsInAnyOrder(expected);
      p.run();
    }

    private void runTestReadLineNumsAndFileName(String[] expected) throws Exception {
      File tmpFile = tempFolder.newFile();
      String filename = tmpFile.getPath();

      List<String> actualExpected = new ArrayList<>();

      try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
        int lineNum = 0;
        for (String elem : expected) {
          byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
          String line = new String(encodedElem, Charsets.UTF_8);
          writer.println(line);
          actualExpected.add(lineNum + " " + filename + " " + line);
          lineNum++;
        }
      }

      ContextualTextIO.Read read = ContextualTextIO.read().from(filename);
      PCollection<String> output =
          p.apply(read)
              .apply(
                  MapElements.into(strings())
                      .via(
                          (RecordWithMetadata L) ->
                              L.getRecordNum() + " " + L.getFileName() + " " + L.getRecordValue()));

      PAssert.that(output).containsInAnyOrder(actualExpected);
      p.run();
    }

    @Test
    public void testDelimiterSelfOverlaps() {
      assertFalse(ContextualTextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c'}));
      assertFalse(
          ContextualTextIO.Read.isSelfOverlapping(new byte[] {'c', 'a', 'b', 'd', 'a', 'b'}));
      assertFalse(
          ContextualTextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c', 'a', 'b', 'd'}));
      assertTrue(ContextualTextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'a'}));
      assertTrue(ContextualTextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c', 'a', 'b'}));
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadStringsWithCustomDelimiter() throws Exception {
      final String[] inputStrings =
          new String[] {
            // incomplete delimiter
            "To be, or not to be: that |is the question: ",
            // incomplete delimiter
            "To be, or not to be: that *is the question: ",
            // complete delimiter
            "Whether 'tis nobler in the mind to suffer |*",
            // truncated delimiter
            "The slings and arrows of outrageous fortune,|"
          };

      File tmpFile = tempFolder.newFile("tmpfile.txt");
      String filename = tmpFile.getPath();

      try (Writer writer = Files.newBufferedWriter(tmpFile.toPath(), UTF_8)) {
        writer.write(Joiner.on("").join(inputStrings));
      }

      PAssert.that(
              p.apply(ContextualTextIO.read().from(filename).withDelimiter(new byte[] {'|', '*'}))
                  .apply(ParDo.of(new GetLines())))
          .containsInAnyOrder(
              "To be, or not to be: that |is the question: To be, or not to be: "
                  + "that *is the question: Whether 'tis nobler in the mind to suffer ",
              "The slings and arrows of outrageous fortune,|");

      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadStringsWithCustomDelimiterAndContext() throws Exception {
      final String[] inputStrings =
          new String[] {
            // incomplete delimiter
            "To be, or not to be: that |is the question: ",
            // incomplete delimiter
            "To be, or not to be: that *is the question: ",
            // complete delimiter
            "Whether 'tis nobler in the mind to suffer |*",
            // truncated delimiter
            "The slings and arrows of outrageous fortune,|"
          };

      File tmpFile = tempFolder.newFile("tmpfile.txt");
      String filename = tmpFile.getPath();

      try (Writer writer = Files.newBufferedWriter(tmpFile.toPath(), UTF_8)) {
        writer.write(Joiner.on("").join(inputStrings));
      }

      PAssert.that(
              p.apply(ContextualTextIO.read().from(filename).withDelimiter(new byte[] {'|', '*'}))
                  .apply(ParDo.of(new GetDetails())))
          .containsInAnyOrder(
              filename
                  + " 0 To be, or not to be: that |is the question: To be, or not to be: "
                  + "that *is the question: Whether 'tis nobler in the mind to suffer ",
              filename + " 1 The slings and arrows of outrageous fortune,|");
      p.run();
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadStrings() throws Exception {
      runTestRead(LINES_ARRAY);
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadStringsWithContext() throws Exception {
      runTestReadLineNumsAndFileName(LINES_ARRAY);
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadEmptyStrings() throws Exception {
      runTestRead(NO_LINES_ARRAY);
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadEmptyStringsWithContext() throws Exception {
      runTestReadLineNumsAndFileName(NO_LINES_ARRAY);
    }

    @Test
    public void testReadDisplayData() {
      ContextualTextIO.Read read = ContextualTextIO.read().from("foo.*").withCompression(BZIP2);

      DisplayData displayData = DisplayData.from(read);

      assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
      assertThat(displayData, hasDisplayItem("compressionType", BZIP2.toString()));
    }

    /** Options for testing. */
    public interface RuntimeTestOptions extends PipelineOptions {
      ValueProvider<String> getInput();

      void setInput(ValueProvider<String> value);
    }

    @Test
    public void testRuntimeOptionsNotCalledInApply() throws Exception {
      p.enableAbandonedNodeEnforcement(false);

      RuntimeTestOptions options = PipelineOptionsFactory.as(RuntimeTestOptions.class);

      p.apply(ContextualTextIO.read().from(options.getInput()));
    }

    @Test
    public void testCompressionIsSet() throws Exception {
      ContextualTextIO.Read read = ContextualTextIO.read().from("/tmp/test");
      assertEquals(AUTO, read.getCompression());
      read = ContextualTextIO.read().from("/tmp/test").withCompression(GZIP);
      assertEquals(GZIP, read.getCompression());
    }

    /**
     * Tests reading from a small, uncompressed file with .gz extension. This must work in GZIP
     * modes. This is needed because some network file systems / HTTP clients will transparently
     * decompress gzipped content.
     */
    @Test
    @Category(NeedsRunner.class)
    public void testSmallCompressedGzipReadActuallyUncompressed() throws Exception {
      File smallGzNotCompressed =
          writeToFile(TINY, tempFolder, "tiny_uncompressed.gz", UNCOMPRESSED);
      // Should work with GZIP compression set.
      assertReadingCompressedFileMatchesExpected(smallGzNotCompressed, GZIP, TINY, p);
      p.run();
    }

    /**
     * Tests reading from a small, uncompressed file with .gz extension. This must work in AUTO
     * modes. This is needed because some network file systems / HTTP clients will transparently
     * decompress gzipped content.
     */
    @Test
    @Category(NeedsRunner.class)
    public void testSmallCompressedAutoReadActuallyUncompressed() throws Exception {
      File smallGzNotCompressed =
          writeToFile(TINY, tempFolder, "tiny_uncompressed.gz", UNCOMPRESSED);
      // Should also work with AUTO mode set.
      assertReadingCompressedFileMatchesExpected(smallGzNotCompressed, AUTO, TINY, p);
      p.run();
    }

    /**
     * Tests a zip file with no entries. This is a corner case not tested elsewhere as the default
     * test zip files have a single entry.
     */
    @Test
    @Category(NeedsRunner.class)
    public void testZipCompressedReadWithNoEntries() throws Exception {
      File file = createZipFile(new ArrayList<>(), tempFolder, "empty zip file");
      assertReadingCompressedFileMatchesExpected(file, ZIP, EMPTY, p);
      p.run();
    }

    /**
     * Tests a zip file with multiple entries. This is a corner case not tested elsewhere as the
     * default test zip files have a single entry.
     */
    @Test
    @Category(NeedsRunner.class)
    public void testZipCompressedReadWithMultiEntriesFile() throws Exception {
      String[] entry0 = new String[] {"first", "second", "three"};
      String[] entry1 = new String[] {"four", "five", "six"};
      String[] entry2 = new String[] {"seven", "eight", "nine"};

      List<String> expected = new ArrayList<>();

      File file = createZipFile(expected, tempFolder, "multiple entries", entry0, entry1, entry2);
      assertReadingCompressedFileMatchesExpected(file, ZIP, expected, p);
      p.run();
    }

    /**
     * Read a ZIP compressed file containing data, multiple empty entries, and then more data. We
     * expect just the data back.
     */
    @Test
    @Category(NeedsRunner.class)
    public void testZipCompressedReadWithComplexEmptyAndPresentEntries() throws Exception {
      File file =
          createZipFile(
              new ArrayList<>(),
              tempFolder,
              "complex empty and present entries",
              new String[] {"cat"},
              new String[] {},
              new String[] {},
              new String[] {"dog"});

      assertReadingCompressedFileMatchesExpected(file, ZIP, Arrays.asList("cat", "dog"), p);
      p.run();
    }

    @Test
    public void testContextualTextIOGetName() {
      assertEquals("ContextualTextIO.Read", ContextualTextIO.read().from("somefile").getName());
      assertEquals("ContextualTextIO.Read", ContextualTextIO.read().from("somefile").toString());
    }

    private ContextualTextIOSource prepareSource(byte[] data, boolean hasRFC4180Multiline)
        throws IOException {
      return ContextualTextIOTest.prepareSource(tempFolder, data, null, hasRFC4180Multiline);
    }

    private ContextualTextIOSource prepareSource(byte[] data) throws IOException {
      return ContextualTextIOTest.prepareSource(tempFolder, data, null, false);
    }

    @Test
    public void testProgressEmptyFile() throws IOException {
      try (BoundedSource.BoundedReader<RecordWithMetadata> reader =
          prepareSource(new byte[0]).createReader(PipelineOptionsFactory.create())) {
        // Check preconditions before starting.
        assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

        // Assert empty
        assertFalse(reader.start());

        // Check postconditions after finishing
        assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(0, reader.getSplitPointsRemaining());
      }
    }

    @Test
    public void testProgressTextFile() throws IOException {
      String file = "line1\nline2\nline3";
      try (BoundedSource.BoundedReader<RecordWithMetadata> reader =
          prepareSource(file.getBytes(Charsets.UTF_8))
              .createReader(PipelineOptionsFactory.create())) {
        // Check preconditions before starting
        assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

        // Line 1
        assertTrue(reader.start());
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

        // Line 2
        assertTrue(reader.advance());
        assertEquals(1, reader.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

        // Line 3
        assertTrue(reader.advance());
        assertEquals(2, reader.getSplitPointsConsumed());
        assertEquals(1, reader.getSplitPointsRemaining());

        // Check postconditions after finishing
        assertFalse(reader.advance());
        assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
        assertEquals(3, reader.getSplitPointsConsumed());
        assertEquals(0, reader.getSplitPointsRemaining());
      }
    }

    @Test
    public void testProgressAfterSplitting() throws IOException {
      String file = "line1\nline2\nline3";
      BoundedSource<RecordWithMetadata> source = prepareSource(file.getBytes(Charsets.UTF_8));
      BoundedSource<RecordWithMetadata> remainder;

      // Create the remainder, verifying properties pre- and post-splitting.
      try (BoundedSource.BoundedReader<RecordWithMetadata> readerOrig =
          source.createReader(PipelineOptionsFactory.create())) {
        // Preconditions.
        assertEquals(0.0, readerOrig.getFractionConsumed(), 1e-6);
        assertEquals(0, readerOrig.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, readerOrig.getSplitPointsRemaining());

        // First record, before splitting.
        assertTrue(readerOrig.start());
        assertEquals(0, readerOrig.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, readerOrig.getSplitPointsRemaining());

        // Split. 0.1 is in line1, so should now be able to detect last record.
        remainder = readerOrig.splitAtFraction(0.1);
        System.err.println(readerOrig.getCurrentSource());
        assertNotNull(remainder);

        // First record, after splitting.
        assertEquals(0, readerOrig.getSplitPointsConsumed());
        assertEquals(1, readerOrig.getSplitPointsRemaining());

        // Finish and postconditions.
        assertFalse(readerOrig.advance());
        assertEquals(1.0, readerOrig.getFractionConsumed(), 1e-6);
        assertEquals(1, readerOrig.getSplitPointsConsumed());
        assertEquals(0, readerOrig.getSplitPointsRemaining());
      }

      // Check the properties of the remainder.
      try (BoundedSource.BoundedReader<RecordWithMetadata> reader =
          remainder.createReader(PipelineOptionsFactory.create())) {
        // Preconditions.
        assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

        // First record should be line 2.
        assertTrue(reader.start());
        assertEquals(0, reader.getSplitPointsConsumed());
        assertEquals(
            BoundedSource.BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

        // Second record is line 3
        assertTrue(reader.advance());
        assertEquals(1, reader.getSplitPointsConsumed());
        assertEquals(1, reader.getSplitPointsRemaining());

        // Check postconditions after finishing
        assertFalse(reader.advance());
        assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
        assertEquals(2, reader.getSplitPointsConsumed());
        assertEquals(0, reader.getSplitPointsRemaining());
      }
    }

    @Test
    public void testInitialSplitAutoModeGz() throws Exception {
      PipelineOptions options = TestPipeline.testingPipelineOptions();
      long desiredBundleSize = 1000;
      File largeGz = writeToFile(LARGE, tempFolder, "large.gz", GZIP);
      // Sanity check: file is at least 2 bundles long.
      assertThat(largeGz.length(), greaterThan(2 * desiredBundleSize));

      FileBasedSource<RecordWithMetadata> source =
          ContextualTextIO.read().from(largeGz.getPath()).getSource();
      List<? extends FileBasedSource<RecordWithMetadata>> splits =
          source.split(desiredBundleSize, options);

      // Exactly 1 split, even in AUTO mode, since it is a gzip file.
      assertThat(splits, hasSize(equalTo(1)));
      SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
    }

    @Test
    public void testInitialSplitGzipModeTxt() throws Exception {
      PipelineOptions options = TestPipeline.testingPipelineOptions();
      long desiredBundleSize = 1000;
      File largeTxt = writeToFile(LARGE, tempFolder, "large.txt", UNCOMPRESSED);
      // Sanity check: file is at least 2 bundles long.
      assertThat(largeTxt.length(), greaterThan(2 * desiredBundleSize));

      FileBasedSource<RecordWithMetadata> source =
          ContextualTextIO.read().from(largeTxt.getPath()).withCompression(GZIP).getSource();
      List<? extends FileBasedSource<RecordWithMetadata>> splits =
          source.split(desiredBundleSize, options);

      // Exactly 1 split, even though splittable text file, since using GZIP mode.
      assertThat(splits, hasSize(equalTo(1)));
      SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadFiles() throws IOException {
      Path tempFolderPath = tempFolder.getRoot().toPath();
      writeToFile(TINY, tempFolder, "readAllTiny1.zip", ZIP);
      writeToFile(TINY, tempFolder, "readAllTiny2.txt", UNCOMPRESSED);
      writeToFile(LARGE, tempFolder, "readAllLarge1.zip", ZIP);
      writeToFile(LARGE, tempFolder, "readAllLarge2.txt", UNCOMPRESSED);
      PCollection<String> lines =
          p.apply(
                  Create.of(
                      tempFolderPath.resolve("readAllTiny*").toString(),
                      tempFolderPath.resolve("readAllLarge*").toString()))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches().withCompression(AUTO))
              .apply(ContextualTextIO.readFiles().withDesiredBundleSizeBytes(10))
              .apply(MapElements.into(strings()).via((RecordWithMetadata L) -> L.getRecordValue()));
      PAssert.that(lines).containsInAnyOrder(Iterables.concat(TINY, TINY, LARGE, LARGE));
      p.run();
    }

    @Test
    @Category({NeedsRunner.class, UsesUnboundedSplittableParDo.class})
    public void testReadWatchForNewFiles() throws IOException, InterruptedException {
      final Path basePath = tempFolder.getRoot().toPath().resolve("readWatch");
      basePath.toFile().mkdir();

      p.apply(GenerateSequence.from(0).to(10).withRate(1, Duration.millis(100)))
          .apply(
              Window.<Long>into(FixedWindows.of(Duration.millis(150)))
                  .withAllowedLateness(Duration.ZERO)
                  .triggering(Repeatedly.forever(AfterPane.elementCountAtLeast(1)))
                  .discardingFiredPanes())
          .apply(ToString.elements())
          .apply(
              TextIO.write()
                  .to(basePath.resolve("data").toString())
                  .withNumShards(1)
                  .withWindowedWrites());

      PCollection<String> lines =
          p.apply(
                  FileIO.match()
                      .filepattern(basePath.resolve("*").toString())
                      .continuously(
                          Duration.millis(100),
                          Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3))))
              .apply(FileIO.readMatches())
              .apply(ContextualTextIO.readFiles())
              .apply(MapElements.into(strings()).via((RecordWithMetadata L) -> L.getRecordValue()));

      PAssert.that(lines).containsInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
      p.run();
    }
  }
}
