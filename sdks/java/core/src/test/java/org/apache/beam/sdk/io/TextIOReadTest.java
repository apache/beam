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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;
import static org.apache.beam.sdk.io.Compression.AUTO;
import static org.apache.beam.sdk.io.Compression.BZIP2;
import static org.apache.beam.sdk.io.Compression.DEFLATE;
import static org.apache.beam.sdk.io.Compression.GZIP;
import static org.apache.beam.sdk.io.Compression.UNCOMPRESSED;
import static org.apache.beam.sdk.io.Compression.ZIP;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Charsets;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Joiner;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorOutputStream;
import org.apache.commons.lang3.SystemUtils;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

/** Tests for {@link TextIO.Read}. */
@RunWith(Enclosed.class)
public class TextIOReadTest {
  private static final int LINES_NUMBER_FOR_LARGE = 1000;
  private static final List<String> EMPTY = Collections.emptyList();
  private static final List<String> TINY =
      Arrays.asList("Irritable eagle", "Optimistic jay", "Fanciful hawk");

  private static final List<String> LARGE = makeLines(LINES_NUMBER_FOR_LARGE);

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

  /** Helper to make an array of compressible strings. Returns ["word"i] for i in range(0,n). */
  private static List<String> makeLines(int n) {
    List<String> ret = new ArrayList<>();
    for (int i = 0; i < n; ++i) {
      ret.add("word" + i);
    }
    return ret;
  }

  /**
   * Helper method that runs a variety of ways to read a single file using TextIO and checks that
   * they all match the given expected output.
   *
   * <p>The transforms being verified are:
   *
   * <ul>
   *   <li>TextIO.read().from(filename).withCompression(compressionType)
   *   <li>TextIO.read().from(filename).withCompression(compressionType) .withHintMatchesManyFiles()
   *   <li>TextIO.readFiles().withCompression(compressionType)
   *   <li>TextIO.readAll().withCompression(compressionType)
   * </ul>
   */
  private static void assertReadingCompressedFileMatchesExpected(
      File file, Compression compression, List<String> expected, Pipeline p) {

    TextIO.Read read = TextIO.read().from(file.getPath()).withCompression(compression);

    PAssert.that(p.apply("Read_" + file + "_" + compression.toString(), read))
        .containsInAnyOrder(expected);
    PAssert.that(
            p.apply(
                "Read_" + file + "_" + compression.toString() + "_many",
                read.withHintMatchesManyFiles()))
        .containsInAnyOrder(expected);

    PAssert.that(
            p.apply("Create_Paths_ReadFiles_" + file, Create.of(file.getPath()))
                .apply("Match_" + file, FileIO.matchAll())
                .apply("ReadMatches_" + file, FileIO.readMatches().withCompression(compression))
                .apply("ReadFiles_" + compression.toString(), TextIO.readFiles()))
        .containsInAnyOrder(expected);

    PAssert.that(
            p.apply("Create_Paths_ReadAll_" + file, Create.of(file.getPath()))
                .apply(
                    "ReadAll_" + compression.toString(),
                    TextIO.readAll().withCompression(compression)))
        .containsInAnyOrder(expected);
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

  private static TextSource prepareSource(
      TemporaryFolder temporaryFolder, byte[] data, @Nullable byte[] delimiter, int skipHeaderLines)
      throws IOException {
    Path path = temporaryFolder.newFile().toPath();
    Files.write(path, data);
    return getTextSource(path.toString(), delimiter, skipHeaderLines);
  }

  public static TextSource getTextSource(
      String path, @Nullable byte[] delimiter, int skipHeaderLines) {
    return new TextSource(
        ValueProvider.StaticValueProvider.of(path),
        EmptyMatchTreatment.DISALLOW,
        delimiter,
        skipHeaderLines);
  }

  public static TextSource getTextSource(String path, @Nullable byte[] delimiter) {
    return getTextSource(path, delimiter, 0);
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
      if (lines.size() == LINES_NUMBER_FOR_LARGE && !compression.equals(UNCOMPRESSED)) {
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
  public static class ReadWithDefaultDelimiterTest {
    private static final ImmutableList<String> EXPECTED = ImmutableList.of("asdf", "hjkl", "xyz");
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {"\n\n\n", ImmutableList.of("", "", "")})
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
    public void testReadLinesWithDefaultDelimiter() throws Exception {
      runTestReadWithData(line.getBytes(UTF_8), expected);
    }

    @Test
    public void testReadLinesWithDefaultDelimiterAndZeroAndOneLengthReturningChannel()
        throws Exception {
      Path path = tempFolder.newFile().toPath();
      Files.write(path, line.getBytes(UTF_8));
      Metadata metadata = FileSystems.matchSingleFileSpec(path.toString());
      FileBasedSource source =
          getTextSource(path.toString(), null, 0)
              .createForSubrangeOfFile(metadata, 0, metadata.sizeBytes());
      FileBasedReader<String> reader =
          source.createSingleFileReader(PipelineOptionsFactory.create());
      ReadableByteChannel channel =
          FileSystems.open(
              FileSystems.matchSingleFileSpec(source.getFileOrPatternSpec()).resourceId());
      InputStream stream = Channels.newInputStream(channel);
      reader.startReading(
          // Placeholder channel that only yields 0- and 1-length buffers.
          // Data is read at most one byte at a time from line parameter.
          new ReadableByteChannel() {
            int readCount = 0;

            @Override
            public int read(ByteBuffer dst) throws IOException {
              if (++readCount % 3 == 0) {
                if (dst.hasRemaining()) {
                  int value = stream.read();
                  if (value == -1) {
                    return -1;
                  }
                  dst.put((byte) value);
                  return 1;
                }
              }
              return 0;
            }

            @Override
            public boolean isOpen() {
              return channel.isOpen();
            }

            @Override
            public void close() throws IOException {
              stream.close();
            }
          });
      assertEquals(expected, SourceTestUtils.readFromStartedReader(reader));
    }

    @Test
    public void testSplittingSource() throws Exception {
      TextSource source = prepareSource(line.getBytes(UTF_8));
      SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
    }

    private TextSource prepareSource(byte[] data) throws IOException {
      return TextIOReadTest.prepareSource(tempFolder, data, null, 0);
    }

    private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
      TextSource source = prepareSource(data);
      List<String> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
      assertThat(
          actual, containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
    }
  }

  /** Tests for reading files with/without header. */
  @RunWith(Parameterized.class)
  public static class SkippingHeaderTest {
    private static final ImmutableList<String> EXPECTED = ImmutableList.of("asdf", "hjkl", "xyz");
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {"\n\n\n", ImmutableList.of("", ""), 1})
          .add(new Object[] {"\n", ImmutableList.of(), 1})
          .add(new Object[] {"header\nasdf\nhjkl\nxyz\n", EXPECTED, 1})
          .add(new Object[] {"header1\nheader2\nasdf\nhjkl\nxyz\n", EXPECTED, 2})
          .build();
    }

    @Parameterized.Parameter(0)
    public String line;

    @Parameterized.Parameter(1)
    public ImmutableList<String> expected;

    @Parameterized.Parameter(2)
    public int skipHeaderLines;

    @Test
    public void testReadLines() throws Exception {
      runTestReadWithData(line.getBytes(UTF_8), expected);
    }

    private TextSource prepareSource(byte[] data) throws IOException {
      return TextIOReadTest.prepareSource(tempFolder, data, null, skipHeaderLines);
    }

    private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
      TextSource source = prepareSource(data);
      List<String> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
      assertThat(
          actual, containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
    }
  }

  /** Tests for reading files with various delimiters. */
  @RunWith(Parameterized.class)
  public static class ReadWithCustomDelimiterTest {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return ImmutableList.<Object[]>builder()
          .add(new Object[] {"first|*second|*|*third"})
          .add(new Object[] {"first|*second|*|*third|"})
          .add(new Object[] {"first|*second|*|*third*"})
          .add(new Object[] {"first|*second|*|*third|*"})
          .add(new Object[] {"|first|*second|*|*third"})
          .add(new Object[] {"|first|*second|*|*third|"})
          .add(new Object[] {"|first|*second|*|*third*"})
          .add(new Object[] {"|first|*second|*|*third|*"})
          .add(new Object[] {"*first|*second|*|*third"})
          .add(new Object[] {"*first|*second|*|*third|"})
          .add(new Object[] {"*first|*second|*|*third*"})
          .add(new Object[] {"*first|*second|*|*third|*"})
          .add(new Object[] {"|*first|*second|*|*third"})
          .add(new Object[] {"|*first|*second|*|*third|"})
          .add(new Object[] {"|*first|*second|*|*third*"})
          .add(new Object[] {"|*first|*second|*|*third|*"})
          .build();
    }

    @Parameterized.Parameter(0)
    public String testCase;

    @Test
    public void testReadLinesWithCustomDelimiter() throws Exception {
      SourceTestUtils.assertSplitAtFractionExhaustive(
          TextIOReadTest.prepareSource(
              tempFolder, testCase.getBytes(UTF_8), new byte[] {'|', '*'}, 0),
          PipelineOptionsFactory.create());
    }

    @Test
    public void testReadLinesWithCustomDelimiterAndZeroAndOneLengthReturningChannel()
        throws Exception {
      byte[] delimiter = new byte[] {'|', '*'};
      Path path = tempFolder.newFile().toPath();
      Files.write(path, testCase.getBytes(UTF_8));
      Metadata metadata = FileSystems.matchSingleFileSpec(path.toString());
      FileBasedSource source =
          getTextSource(path.toString(), delimiter, 0)
              .createForSubrangeOfFile(metadata, 0, metadata.sizeBytes());
      FileBasedReader<String> reader =
          source.createSingleFileReader(PipelineOptionsFactory.create());
      ReadableByteChannel channel =
          FileSystems.open(
              FileSystems.matchSingleFileSpec(source.getFileOrPatternSpec()).resourceId());
      InputStream stream = Channels.newInputStream(channel);
      reader.startReading(
          // Placeholder channel that only yields 0- and 1-length buffers.
          // Data is read at most one byte at a time from testCase parameter.
          new ReadableByteChannel() {
            int readCount = 0;

            @Override
            public int read(ByteBuffer dst) throws IOException {
              if (++readCount % 3 == 0) {
                if (dst.hasRemaining()) {
                  int value = stream.read();
                  if (value == -1) {
                    return -1;
                  }
                  dst.put((byte) value);
                  return 1;
                }
              }
              return 0;
            }

            @Override
            public boolean isOpen() {
              return channel.isOpen();
            }

            @Override
            public void close() throws IOException {
              stream.close();
            }
          });
      assertEquals(
          SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create()),
          SourceTestUtils.readFromStartedReader(reader));
    }
  }

  /** Tests for some basic operations in {@link TextIO.Read}. */
  @RunWith(JUnit4.class)
  public static class BasicIOTest {
    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
    @Rule public TestPipeline p = TestPipeline.create();

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

      TextIO.Read read = TextIO.read().from(filename);
      PCollection<String> output = p.apply(read);

      PAssert.that(output).containsInAnyOrder(expected);
      p.run();
    }

    @Test
    public void testDelimiterSelfOverlaps() {
      assertFalse(TextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c'}));
      assertFalse(TextIO.Read.isSelfOverlapping(new byte[] {'c', 'a', 'b', 'd', 'a', 'b'}));
      assertFalse(TextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c', 'a', 'b', 'd'}));
      assertTrue(TextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'a'}));
      assertTrue(TextIO.Read.isSelfOverlapping(new byte[] {'a', 'b', 'c', 'a', 'b'}));
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

      PAssert.that(p.apply(TextIO.read().from(filename).withDelimiter(new byte[] {'|', '*'})))
          .containsInAnyOrder(
              "To be, or not to be: that |is the question: To be, or not to be: "
                  + "that *is the question: Whether 'tis nobler in the mind to suffer ",
              "The slings and arrows of outrageous fortune,|");
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
      File emptyFile = tempFolder.newFile();
      p.enableAbandonedNodeEnforcement(false);

      assertThat(p.apply(TextIO.read().from("somefile")).getName(), startsWith("TextIO.Read/Read"));
      assertThat(
          p.apply("MyRead", TextIO.read().from(emptyFile.getPath())).getName(),
          startsWith("MyRead/Read"));
    }

    @Test
    public void testReadDisplayData() {
      TextIO.Read read = TextIO.read().from("foo.*").withCompression(BZIP2);

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

      p.apply(TextIO.read().from(options.getInput()));
    }

    @Test
    public void testCompressionIsSet() throws Exception {
      TextIO.Read read = TextIO.read().from("/tmp/test");
      assertEquals(AUTO, read.getCompression());
      read = TextIO.read().from("/tmp/test").withCompression(GZIP);
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
    public void testTextIOGetName() {
      assertEquals("TextIO.Read", TextIO.read().from("somefile").getName());
      assertEquals("TextIO.Read", TextIO.read().from("somefile").toString());
    }

    private TextSource prepareSource(byte[] data) throws IOException {
      return TextIOReadTest.prepareSource(tempFolder, data, null, 0);
    }

    @Test
    public void testProgressEmptyFile() throws IOException {
      try (BoundedSource.BoundedReader<String> reader =
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
      try (BoundedSource.BoundedReader<String> reader =
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
      BoundedSource<String> source = prepareSource(file.getBytes(Charsets.UTF_8));
      BoundedSource<String> remainder;

      // Create the remainder, verifying properties pre- and post-splitting.
      try (BoundedSource.BoundedReader<String> readerOrig =
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
      try (BoundedSource.BoundedReader<String> reader =
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
    public void testInitialSplitAutoModeTxt() throws Exception {
      PipelineOptions options = TestPipeline.testingPipelineOptions();
      long desiredBundleSize = 1000;
      File largeTxt = writeToFile(LARGE, tempFolder, "large.txt", UNCOMPRESSED);

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
      // TODO: Java core test failing on windows, https://github.com/apache/beam/issues/20470
      assumeFalse(SystemUtils.IS_OS_WINDOWS);
      PipelineOptions options = TestPipeline.testingPipelineOptions();
      long desiredBundleSize = 1000;
      File largeGz = writeToFile(LARGE, tempFolder, "large.gz", GZIP);
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
      File largeTxt = writeToFile(LARGE, tempFolder, "large.txt", UNCOMPRESSED);
      // Sanity check: file is at least 2 bundles long.
      assertThat(largeTxt.length(), greaterThan(2 * desiredBundleSize));

      FileBasedSource<String> source =
          TextIO.read().from(largeTxt.getPath()).withCompression(GZIP).getSource();
      List<? extends FileBasedSource<String>> splits = source.split(desiredBundleSize, options);

      // Exactly 1 split, even though splittable text file, since using GZIP mode.
      assertThat(splits, hasSize(equalTo(1)));
      SourceTestUtils.assertSourcesEqualReferenceSource(source, splits, options);
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadAll() throws IOException {
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
              .apply(TextIO.readAll().withCompression(AUTO));
      PAssert.that(lines).containsInAnyOrder(Iterables.concat(TINY, TINY, LARGE, LARGE));
      p.run();
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
              .apply(TextIO.readFiles().withDesiredBundleSizeBytes(10));
      PAssert.that(lines).containsInAnyOrder(Iterables.concat(TINY, TINY, LARGE, LARGE));
      p.run();
    }

    private List<KV<String, String>> filenameKV(Path path, String fn, List<String> input) {
      return input.stream()
          .map(l -> KV.of(path.resolve(fn).toString(), l))
          .collect(Collectors.toList());
    }

    @Test
    @Category(NeedsRunner.class)
    public void testReadFilesWithFilename() throws IOException {
      Path tempFolderPath = tempFolder.getRoot().toPath();
      writeToFile(TINY, tempFolder, "readAllTiny1.zip", ZIP);
      writeToFile(TINY, tempFolder, "readAllTiny2.txt", UNCOMPRESSED);
      writeToFile(LARGE, tempFolder, "readAllLarge1.zip", ZIP);
      writeToFile(LARGE, tempFolder, "readAllLarge2.txt", UNCOMPRESSED);

      SerializableFunction<String, ? extends FileBasedSource<String>> createSource =
          input ->
              new TextSource(
                  ValueProvider.StaticValueProvider.of(input),
                  EmptyMatchTreatment.DISALLOW,
                  new byte[] {'\n'},
                  0);

      PCollection<KV<String, String>> lines =
          p.apply(
                  Create.of(
                      tempFolderPath.resolve("readAllTiny*").toString(),
                      tempFolderPath.resolve("readAllLarge*").toString()))
              .apply(FileIO.matchAll())
              .apply(FileIO.readMatches().withCompression(AUTO))
              .apply(
                  new ReadAllViaFileBasedSourceWithFilename<>(
                      10, createSource, KvCoder.of(StringUtf8Coder.of(), StringUtf8Coder.of())));

      PAssert.that(lines)
          .containsInAnyOrder(
              Iterables.concat(
                  filenameKV(tempFolderPath, "readAllTiny1.zip", TINY),
                  filenameKV(tempFolderPath, "readAllTiny2.txt", TINY),
                  filenameKV(tempFolderPath, "readAllLarge1.zip", LARGE),
                  filenameKV(tempFolderPath, "readAllLarge2.txt", LARGE)));
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
              TextIO.read()
                  .from(basePath.resolve("*").toString())
                  .watchForNewFiles(
                      Duration.millis(100),
                      Watch.Growth.afterTimeSinceNewOutput(Duration.standardSeconds(3))));

      PAssert.that(lines).containsInAnyOrder("0", "1", "2", "3", "4", "5", "6", "7", "8", "9");
      p.run();
    }
  }

  /** Tests for TextSource class. */
  @RunWith(JUnit4.class)
  public static class TextSourceTest {
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
              .apply("Read file", new TextIOReadTest.TextSourceTest.TextFileReadTransform());
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
              .apply("Read file", new TextIOReadTest.TextSourceTest.TextFileReadTransform());

      PAssert.that(contents).containsInAnyOrder("\uFEFE1,p1テスト", "\uFEFE2,p1テスト");

      pipeline.run();
    }

    private static class FileReadDoFn extends DoFn<FileIO.ReadableFile, String> {

      @ProcessElement
      public void processElement(ProcessContext c) {
        FileIO.ReadableFile file = c.element();
        ValueProvider<String> filenameProvider =
            ValueProvider.StaticValueProvider.of(file.getMetadata().resourceId().getFilename());
        // Create a TextSource, passing null as the delimiter to use the default
        // delimiters ('\n', '\r', or '\r\n').
        TextSource textSource = new TextSource(filenameProvider, null, null, 0);
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
            .apply("Read lines", ParDo.of(new TextIOReadTest.TextSourceTest.FileReadDoFn()));
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
}
