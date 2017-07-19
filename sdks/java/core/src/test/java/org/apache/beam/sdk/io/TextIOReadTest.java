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

import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;
import static org.apache.beam.sdk.io.TextIO.CompressionType.AUTO;
import static org.apache.beam.sdk.io.TextIO.CompressionType.BZIP2;
import static org.apache.beam.sdk.io.TextIO.CompressionType.DEFLATE;
import static org.apache.beam.sdk.io.TextIO.CompressionType.GZIP;
import static org.apache.beam.sdk.io.TextIO.CompressionType.UNCOMPRESSED;
import static org.apache.beam.sdk.io.TextIO.CompressionType.ZIP;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.TextIO.CompressionType;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TextIO.Read}. */
@RunWith(JUnit4.class)
public class TextIOReadTest {
  private static final List<String> EMPTY = Collections.emptyList();
  private static final List<String> TINY =
      Arrays.asList("Irritable eagle", "Optimistic jay", "Fanciful hawk");
  private static final List<String> LARGE = makeLines(1000);
  private static int uniquifier = 0;

  private static Path tempFolder;
  private static File emptyTxt;
  private static File tinyTxt;
  private static File largeTxt;
  private static File emptyGz;
  private static File tinyGz;
  private static File largeGz;
  private static File emptyBzip2;
  private static File tinyBzip2;
  private static File largeBzip2;
  private static File emptyZip;
  private static File tinyZip;
  private static File largeZip;
  private static File emptyDeflate;
  private static File tinyDeflate;
  private static File largeDeflate;

  @Rule public TestPipeline p = TestPipeline.create();

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private static File writeToFile(List<String> lines, String filename, CompressionType compression)
      throws IOException {
    File file = tempFolder.resolve(filename).toFile();
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

  @BeforeClass
  public static void setupClass() throws IOException {
    tempFolder = Files.createTempDirectory("TextIOTest");
    // empty files
    emptyTxt = writeToFile(EMPTY, "empty.txt", CompressionType.UNCOMPRESSED);
    emptyGz = writeToFile(EMPTY, "empty.gz", GZIP);
    emptyBzip2 = writeToFile(EMPTY, "empty.bz2", BZIP2);
    emptyZip = writeToFile(EMPTY, "empty.zip", ZIP);
    emptyDeflate = writeToFile(EMPTY, "empty.deflate", DEFLATE);
    // tiny files
    tinyTxt = writeToFile(TINY, "tiny.txt", CompressionType.UNCOMPRESSED);
    tinyGz = writeToFile(TINY, "tiny.gz", GZIP);
    tinyBzip2 = writeToFile(TINY, "tiny.bz2", BZIP2);
    tinyZip = writeToFile(TINY, "tiny.zip", ZIP);
    tinyDeflate = writeToFile(TINY, "tiny.deflate", DEFLATE);
    // large files
    largeTxt = writeToFile(LARGE, "large.txt", CompressionType.UNCOMPRESSED);
    largeGz = writeToFile(LARGE, "large.gz", GZIP);
    largeBzip2 = writeToFile(LARGE, "large.bz2", BZIP2);
    largeZip = writeToFile(LARGE, "large.zip", ZIP);
    largeDeflate = writeToFile(LARGE, "large.deflate", DEFLATE);
  }

  @AfterClass
  public static void teardownClass() throws IOException {
    Files.walkFileTree(
        tempFolder,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
              throws IOException {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private void runTestRead(String[] expected) throws Exception {
    File tmpFile = Files.createTempFile(tempFolder, "file", "txt").toFile();
    String filename = tmpFile.getPath();

    try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
      for (String elem : expected) {
        byte[] encodedElem = CoderUtils.encodeToByteArray(StringUtf8Coder.of(), elem);
        String line = new String(encodedElem);
        writer.println(line);
      }
    }

    TextIO.Read read = TextIO.read().from(filename);

    PCollection<String> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadStrings() throws Exception {
    runTestRead(LINES_ARRAY);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadEmptyStrings() throws Exception {
    runTestRead(NO_LINES_ARRAY);
  }

  @Test
  public void testReadNamed() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    assertEquals("TextIO.Read/Read.out", p.apply(TextIO.read().from("somefile")).getName());
    assertEquals(
        "MyRead/Read.out", p.apply("MyRead", TextIO.read().from(emptyTxt.getPath())).getName());
  }

  @Test
  public void testReadDisplayData() {
    TextIO.Read read = TextIO.read().from("foo.*").withCompressionType(BZIP2);

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("compressionType", BZIP2.toString()));
  }

  @Test
  @Category(ValidatesRunner.class)
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    TextIO.Read read = TextIO.read().from("foobar");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveSourceTransforms(read);
    assertThat(
        "TextIO.Read should include the file prefix in its primitive display data",
        displayData,
        hasItem(hasDisplayItem(hasValue(startsWith("foobar")))));
  }

  /** Options for testing. */
  public interface RuntimeTestOptions extends PipelineOptions {
    ValueProvider<String> getInput();
    void setInput(ValueProvider<String> value);
  }

  @Test
  public void testRuntimeOptionsNotCalledInApply() throws Exception {
    p.enableAbandonedNodeEnforcement(false);

    RuntimeTestOptions options =
        PipelineOptionsFactory.as(RuntimeTestOptions.class);

    p.apply(TextIO.read().from(options.getInput()));
  }

  @Test
  public void testCompressionTypeIsSet() throws Exception {
    TextIO.Read read = TextIO.read().from("/tmp/test");
    assertEquals(AUTO, read.getCompressionType());
    read = TextIO.read().from("/tmp/test").withCompressionType(GZIP);
    assertEquals(GZIP, read.getCompressionType());
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

  /**
   * Helper method that runs a variety of ways to read a single file using TextIO
   * and checks that they all match the given expected output.
   *
   * <p>The transforms being verified are:
   * <ul>
   *   <li>TextIO.read().from(filename).withCompressionType(compressionType)
   *   <li>TextIO.read().from(filename).withCompressionType(compressionType)
   *     .withHintMatchesManyFiles()
   *   <li>TextIO.readAll().withCompressionType(compressionType)
   * </ul> and
   */
  private void assertReadingCompressedFileMatchesExpected(
      File file, CompressionType compressionType, List<String> expected) {

    int thisUniquifier = ++uniquifier;

    TextIO.Read read = TextIO.read().from(file.getPath()).withCompressionType(compressionType);

    PAssert.that(
            p.apply("Read_" + file + "_" + compressionType.toString() + "_" + thisUniquifier, read))
        .containsInAnyOrder(expected);

    PAssert.that(
            p.apply(
                "Read_" + file + "_" + compressionType.toString() + "_many" + "_" + thisUniquifier,
                read.withHintMatchesManyFiles()))
        .containsInAnyOrder(expected);

    TextIO.ReadAll readAll =
        TextIO.readAll().withCompressionType(compressionType).withDesiredBundleSizeBytes(10);
    PAssert.that(
            p.apply("Create_" + file + "_" + thisUniquifier, Create.of(file.getPath()))
                .apply("Read_" + compressionType.toString() + "_" + thisUniquifier, readAll))
        .containsInAnyOrder(expected);
  }

  /** Helper to make an array of compressible strings. Returns ["word"i] for i in range(0,n). */
  private static List<String> makeLines(int n) {
    List<String> ret = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      ret.add("word" + i);
    }
    return ret;
  }

  /** Tests reading from a small, gzipped file with no .gz extension but GZIP compression set. */
  @Test
  @Category(NeedsRunner.class)
  public void testSmallCompressedGzipReadNoExtension() throws Exception {
    File smallGzNoExtension = writeToFile(TINY, "tiny_gz_no_extension", GZIP);
    assertReadingCompressedFileMatchesExpected(smallGzNoExtension, GZIP, TINY);
    p.run();
  }

  /**
   * Tests reading from a small, uncompressed file with .gz extension. This must work in AUTO or
   * GZIP modes. This is needed because some network file systems / HTTP clients will transparently
   * decompress gzipped content.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testSmallCompressedGzipReadActuallyUncompressed() throws Exception {
    File smallGzNotCompressed =
        writeToFile(TINY, "tiny_uncompressed.gz", CompressionType.UNCOMPRESSED);
    // Should work with GZIP compression set.
    assertReadingCompressedFileMatchesExpected(smallGzNotCompressed, GZIP, TINY);
    // Should also work with AUTO mode set.
    assertReadingCompressedFileMatchesExpected(smallGzNotCompressed, AUTO, TINY);
    p.run();
  }

  /** Tests reading from a small, bzip2ed file with no .bz2 extension but BZIP2 compression set. */
  @Test
  @Category(NeedsRunner.class)
  public void testSmallCompressedBzip2ReadNoExtension() throws Exception {
    File smallBz2NoExtension = writeToFile(TINY, "tiny_bz2_no_extension", BZIP2);
    assertReadingCompressedFileMatchesExpected(smallBz2NoExtension, BZIP2, TINY);
    p.run();
  }

  /**
   * Create a zip file with the given lines.
   *
   * @param expected A list of expected lines, populated in the zip file.
   * @param filename Optionally zip file name (can be null).
   * @param fieldsEntries Fields to write in zip entries.
   * @return The zip filename.
   * @throws Exception In case of a failure during zip file creation.
   */
  private String createZipFile(List<String> expected, String filename, String[]... fieldsEntries)
      throws Exception {
    File tmpFile = tempFolder.resolve(filename).toFile();
    String tmpFileName = tmpFile.getPath();

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

    return tmpFileName;
  }

  @Test
  @Category(NeedsRunner.class)
  public void testTxtRead() throws Exception {
    // Files with non-compressed extensions should work in AUTO and UNCOMPRESSED modes.
    for (CompressionType type : new CompressionType[] {AUTO, UNCOMPRESSED}) {
      assertReadingCompressedFileMatchesExpected(emptyTxt, type, EMPTY);
      assertReadingCompressedFileMatchesExpected(tinyTxt, type, TINY);
      assertReadingCompressedFileMatchesExpected(largeTxt, type, LARGE);
    }
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGzipCompressedRead() throws Exception {
    // Files with the right extensions should work in AUTO and GZIP modes.
    for (CompressionType type : new CompressionType[] {AUTO, GZIP}) {
      assertReadingCompressedFileMatchesExpected(emptyGz, type, EMPTY);
      assertReadingCompressedFileMatchesExpected(tinyGz, type, TINY);
      assertReadingCompressedFileMatchesExpected(largeGz, type, LARGE);
    }

    // Sanity check that we're properly testing compression.
    assertThat(largeTxt.length(), greaterThan(largeGz.length()));

    // GZIP files with non-gz extension should work in GZIP mode.
    File gzFile = writeToFile(TINY, "tiny_gz_no_extension", GZIP);
    assertReadingCompressedFileMatchesExpected(gzFile, GZIP, TINY);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testBzip2CompressedRead() throws Exception {
    // Files with the right extensions should work in AUTO and BZIP2 modes.
    for (CompressionType type : new CompressionType[] {AUTO, BZIP2}) {
      assertReadingCompressedFileMatchesExpected(emptyBzip2, type, EMPTY);
      assertReadingCompressedFileMatchesExpected(tinyBzip2, type, TINY);
      assertReadingCompressedFileMatchesExpected(largeBzip2, type, LARGE);
    }

    // Sanity check that we're properly testing compression.
    assertThat(largeTxt.length(), greaterThan(largeBzip2.length()));

    // BZ2 files with non-bz2 extension should work in BZIP2 mode.
    File bz2File = writeToFile(TINY, "tiny_bz2_no_extension", BZIP2);
    assertReadingCompressedFileMatchesExpected(bz2File, BZIP2, TINY);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedRead() throws Exception {
    // Files with the right extensions should work in AUTO and ZIP modes.
    for (CompressionType type : new CompressionType[] {AUTO, ZIP}) {
      assertReadingCompressedFileMatchesExpected(emptyZip, type, EMPTY);
      assertReadingCompressedFileMatchesExpected(tinyZip, type, TINY);
      assertReadingCompressedFileMatchesExpected(largeZip, type, LARGE);
    }

    // Sanity check that we're properly testing compression.
    assertThat(largeTxt.length(), greaterThan(largeZip.length()));

    // Zip files with non-zip extension should work in ZIP mode.
    File zipFile = writeToFile(TINY, "tiny_zip_no_extension", ZIP);
    assertReadingCompressedFileMatchesExpected(zipFile, ZIP, TINY);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDeflateCompressedRead() throws Exception {
    // Files with the right extensions should work in AUTO and ZIP modes.
    for (CompressionType type : new CompressionType[] {AUTO, DEFLATE}) {
      assertReadingCompressedFileMatchesExpected(emptyDeflate, type, EMPTY);
      assertReadingCompressedFileMatchesExpected(tinyDeflate, type, TINY);
      assertReadingCompressedFileMatchesExpected(largeDeflate, type, LARGE);
    }

    // Sanity check that we're properly testing compression.
    assertThat(largeTxt.length(), greaterThan(largeDeflate.length()));

    // Deflate files with non-deflate extension should work in DEFLATE mode.
    File deflateFile = writeToFile(TINY, "tiny_deflate_no_extension", DEFLATE);
    assertReadingCompressedFileMatchesExpected(deflateFile, DEFLATE, TINY);
    p.run();
  }

  /**
   * Tests a zip file with no entries. This is a corner case not tested elsewhere as the default
   * test zip files have a single entry.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedReadWithNoEntries() throws Exception {
    String filename = createZipFile(new ArrayList<String>(), "empty zip file");
    assertReadingCompressedFileMatchesExpected(new File(filename), CompressionType.ZIP, EMPTY);
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

    String filename = createZipFile(expected, "multiple entries", entry0, entry1, entry2);
    assertReadingCompressedFileMatchesExpected(new File(filename), CompressionType.ZIP, expected);
    p.run();
  }

  /**
   * Read a ZIP compressed file containing data, multiple empty entries, and then more data. We
   * expect just the data back.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedReadWithComplexEmptyAndPresentEntries() throws Exception {
    String filename =
        createZipFile(
            new ArrayList<String>(),
            "complex empty and present entries",
            new String[] {"cat"},
            new String[] {},
            new String[] {},
            new String[] {"dog"});

    assertReadingCompressedFileMatchesExpected(
        new File(filename), CompressionType.ZIP, Arrays.asList("cat", "dog"));
    p.run();
  }

  @Test
  public void testTextIOGetName() {
    assertEquals("TextIO.Read", TextIO.read().from("somefile").getName());
    assertEquals("TextIO.Read", TextIO.read().from("somefile").toString());
  }

  @Test
  public void testProgressEmptyFile() throws IOException {
    try (BoundedReader<String> reader =
        prepareSource(new byte[0]).createReader(PipelineOptionsFactory.create())) {
      // Check preconditions before starting.
      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

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
    try (BoundedReader<String> reader =
        prepareSource(file.getBytes()).createReader(PipelineOptionsFactory.create())) {
      // Check preconditions before starting
      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // Line 1
      assertTrue(reader.start());
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // Line 2
      assertTrue(reader.advance());
      assertEquals(1, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

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
    BoundedSource<String> source = prepareSource(file.getBytes());
    BoundedSource<String> remainder;

    // Create the remainder, verifying properties pre- and post-splitting.
    try (BoundedReader<String> readerOrig = source.createReader(PipelineOptionsFactory.create())) {
      // Preconditions.
      assertEquals(0.0, readerOrig.getFractionConsumed(), 1e-6);
      assertEquals(0, readerOrig.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, readerOrig.getSplitPointsRemaining());

      // First record, before splitting.
      assertTrue(readerOrig.start());
      assertEquals(0, readerOrig.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, readerOrig.getSplitPointsRemaining());

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
    try (BoundedReader<String> reader = remainder.createReader(PipelineOptionsFactory.create())) {
      // Preconditions.
      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

      // First record should be line 2.
      assertTrue(reader.start());
      assertEquals(0, reader.getSplitPointsConsumed());
      assertEquals(BoundedReader.SPLIT_POINTS_UNKNOWN, reader.getSplitPointsRemaining());

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
  public void testReadEmptyLines() throws Exception {
    runTestReadWithData("\n\n\n".getBytes(StandardCharsets.UTF_8), ImmutableList.of("", "", ""));
  }

  @Test
  public void testReadFileWithLineFeedDelimiter() throws Exception {
    runTestReadWithData(
        "asdf\nhjkl\nxyz\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnDelimiter() throws Exception {
    runTestReadWithData(
        "asdf\rhjkl\rxyz\r".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnAndLineFeedDelimiter() throws Exception {
    runTestReadWithData(
        "asdf\r\nhjkl\r\nxyz\r\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithMixedDelimiters() throws Exception {
    runTestReadWithData(
        "asdf\rhjkl\r\nxyz\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithLineFeedDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData(
        "asdf\nhjkl\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData(
        "asdf\rhjkl\rxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnAndLineFeedDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    runTestReadWithData(
        "asdf\r\nhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithMixedDelimitersAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData(
        "asdf\rhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
    TextSource source = prepareSource(data);
    List<String> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertThat(actual, containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
  }

  @Test
  public void testSplittingSourceWithEmptyLines() throws Exception {
    TextSource source = prepareSource("\n\n\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithLineFeedDelimiter() throws Exception {
    TextSource source = prepareSource("asdf\nhjkl\nxyz\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnDelimiter() throws Exception {
    TextSource source = prepareSource("asdf\rhjkl\rxyz\r".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnAndLineFeedDelimiter() throws Exception {
    TextSource source = prepareSource("asdf\r\nhjkl\r\nxyz\r\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithMixedDelimiters() throws Exception {
    TextSource source = prepareSource("asdf\rhjkl\r\nxyz\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithLineFeedDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    TextSource source = prepareSource("asdf\nhjkl\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    TextSource source = prepareSource("asdf\rhjkl\rxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnAndLineFeedDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    TextSource source = prepareSource("asdf\r\nhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithMixedDelimitersAndNonEmptyBytesAtEnd() throws Exception {
    TextSource source = prepareSource("asdf\rhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  private TextSource prepareSource(byte[] data) throws IOException {
    Path path = Files.createTempFile(tempFolder, "tempfile", "ext");
    Files.write(path, data);
    return new TextSource(ValueProvider.StaticValueProvider.of(path.toString()));
  }

  @Test
  public void testInitialSplitAutoModeTxt() throws Exception {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    long desiredBundleSize = 1000;

    // Sanity check: file is at least 2 bundles long.
    assertThat(largeTxt.length(), greaterThan(2 * desiredBundleSize));

    FileBasedSource<String> source = TextIO.read().from(largeTxt.getPath()).getSource();
    List<? extends FileBasedSource<String>> splits = source.split(desiredBundleSize, options);

    // At least 2 splits and they are equal to reading the whole file.
    assertThat(splits, hasSize(greaterThan(1)));
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
  }

  @Test
  public void testInitialSplitAutoModeGz() throws Exception {
    long desiredBundleSize = 1000;
    PipelineOptions options = TestPipeline.testingPipelineOptions();

    // Sanity check: file is at least 2 bundles long.
    assertThat(largeGz.length(), greaterThan(2 * desiredBundleSize));

    FileBasedSource<String> source = TextIO.read().from(largeGz.getPath()).getSource();
    List<? extends FileBasedSource<String>> splits = source.split(desiredBundleSize, options);

    // Exactly 1 split, even in AUTO mode, since it is a gzip file.
    assertThat(splits, hasSize(equalTo(1)));
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
  }

  @Test
  public void testInitialSplitGzipModeTxt() throws Exception {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    long desiredBundleSize = 1000;

    // Sanity check: file is at least 2 bundles long.
    assertThat(largeTxt.length(), greaterThan(2 * desiredBundleSize));

    FileBasedSource<String> source =
        TextIO.read().from(largeTxt.getPath()).withCompressionType(GZIP).getSource();
    List<? extends FileBasedSource<String>> splits = source.split(desiredBundleSize, options);

    // Exactly 1 split, even though splittable text file, since using GZIP mode.
    assertThat(splits, hasSize(equalTo(1)));
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
  }

  @Test
  public void testInitialSplitGzipModeGz() throws Exception {
    PipelineOptions options = TestPipeline.testingPipelineOptions();
    long desiredBundleSize = 1000;

    // Sanity check: file is at least 2 bundles long.
    assertThat(largeGz.length(), greaterThan(2 * desiredBundleSize));

    FileBasedSource<String> source =
        TextIO.read().from(largeGz.getPath()).withCompressionType(GZIP).getSource();
    List<? extends FileBasedSource<String>> splits = source.split(desiredBundleSize, options);

    // Exactly 1 split using .gz extension and using GZIP mode.
    assertThat(splits, hasSize(equalTo(1)));
    SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadAll() throws IOException {
    writeToFile(TINY, "readAllTiny1.zip", ZIP);
    writeToFile(TINY, "readAllTiny2.zip", ZIP);
    writeToFile(LARGE, "readAllLarge1.zip", ZIP);
    writeToFile(LARGE, "readAllLarge2.zip", ZIP);
    PCollection<String> lines =
        p.apply(
                Create.of(
                    tempFolder.resolve("readAllTiny*").toString(),
                    tempFolder.resolve("readAllLarge*").toString()))
            .apply(TextIO.readAll().withCompressionType(AUTO));
    PAssert.that(lines).containsInAnyOrder(Iterables.concat(TINY, TINY, LARGE, LARGE));
    p.run();
  }
}
