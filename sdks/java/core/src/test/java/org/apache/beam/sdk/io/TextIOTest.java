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

import static org.apache.beam.sdk.TestUtils.INTS_ARRAY;
import static org.apache.beam.sdk.TestUtils.LINES_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_INTS_ARRAY;
import static org.apache.beam.sdk.TestUtils.NO_LINES_ARRAY;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static org.apache.beam.sdk.transforms.display.DisplayDataMatchers.hasValue;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.TextualIntegerCoder;
import org.apache.beam.sdk.coders.VoidCoder;
import org.apache.beam.sdk.io.BoundedSource.BoundedReader;
import org.apache.beam.sdk.io.TextIO.CompressionType;
import org.apache.beam.sdk.io.TextIO.TextSource;
import org.apache.beam.sdk.options.GcsOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.RunnableOnService;
import org.apache.beam.sdk.testing.SourceTestUtils;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.DisplayDataEvaluator;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.PCollection;

import com.google.common.collect.ImmutableList;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nullable;

/**
 * Tests for TextIO Read and Write transforms.
 */
// TODO: Change the tests to use RunnableOnService instead of NeedsRunner
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class TextIOTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setupClass() {
    IOChannelUtils.registerStandardIOFactories(TestPipeline.testingPipelineOptions());
  }

  <T> void runTestRead(T[] expected, Coder<T> coder) throws Exception {
    File tmpFile = tmpFolder.newFile("file.txt");
    String filename = tmpFile.getPath();

    try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
      for (T elem : expected) {
        byte[] encodedElem = CoderUtils.encodeToByteArray(coder, elem);
        String line = new String(encodedElem);
        writer.println(line);
      }
    }

    Pipeline p = TestPipeline.create();

    TextIO.Read.Bound<T> read;
    if (coder.equals(StringUtf8Coder.of())) {
      TextIO.Read.Bound<String> readStrings = TextIO.Read.from(filename);
      // T==String
      read = (TextIO.Read.Bound<T>) readStrings;
    } else {
      read = TextIO.Read.from(filename).withCoder(coder);
    }

    PCollection<T> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadStrings() throws Exception {
    runTestRead(LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadEmptyStrings() throws Exception {
    runTestRead(NO_LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadInts() throws Exception {
    runTestRead(INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadEmptyInts() throws Exception {
    runTestRead(NO_INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testReadNulls() throws Exception {
    runTestRead(new Void[]{ null, null, null }, VoidCoder.of());
  }

  @Test
  public void testReadNamed() throws Exception {
    String file = tmpFolder.newFile().getAbsolutePath();
    Pipeline p = TestPipeline.create();

    {
      PCollection<String> output1 =
          p.apply(TextIO.Read.from(file));
      assertEquals("TextIO.Read/Read.out", output1.getName());
    }

    {
      PCollection<String> output2 = p.apply("MyRead", TextIO.Read.from(file));
      assertEquals("MyRead/Read.out", output2.getName());
    }
  }

  @Test
  public void testReadDisplayData() {
    TextIO.Read.Bound<?> read = TextIO.Read
        .from("foo.*")
        .withCompressionType(CompressionType.BZIP2)
        .withoutValidation();

    DisplayData displayData = DisplayData.from(read);

    assertThat(displayData, hasDisplayItem("filePattern", "foo.*"));
    assertThat(displayData, hasDisplayItem("compressionType", CompressionType.BZIP2.toString()));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  @Category(RunnableOnService.class)
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    TextIO.Read.Bound<String> read = TextIO.Read
        .from("foobar")
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(read);
    assertThat("TextIO.Read should include the file prefix in its primitive display data",
        displayData, hasItem(hasDisplayItem(hasValue(startsWith("foobar")))));
  }

  <T> void runTestWrite(T[] elems, Coder<T> coder) throws Exception {
    runTestWrite(elems, coder, 1);
  }

  <T> void runTestWrite(T[] elems, Coder<T> coder, int numShards) throws Exception {
    String outputName = "file.txt";
    String baseFilename = tmpFolder.newFile(outputName).getPath();

    Pipeline p = TestPipeline.create();

    PCollection<T> input = p.apply(Create.of(Arrays.asList(elems)).withCoder(coder));

    TextIO.Write.Bound<T> write;
    if (coder.equals(StringUtf8Coder.of())) {
      TextIO.Write.Bound<String> writeStrings = TextIO.Write.to(baseFilename);
      // T==String
      write = (TextIO.Write.Bound<T>) writeStrings;
    } else {
      write = TextIO.Write.to(baseFilename).withCoder(coder);
    }
    if (numShards == 1) {
      write = write.withoutSharding();
    } else if (numShards > 0) {
      write = write.withNumShards(numShards).withShardNameTemplate(ShardNameTemplate.INDEX_OF_MAX);
    }
    int numOutputShards = (numShards > 0) ? numShards : 1;

    input.apply(write);

    p.run();

    assertOutputFiles(elems, coder, numOutputShards, tmpFolder, outputName,
        write.getShardNameTemplate());
  }

  public static <T> void assertOutputFiles(
      T[] elems,
      Coder<T> coder,
      int numShards,
      TemporaryFolder rootLocation,
      String outputName,
      String shardNameTemplate)
      throws Exception {
    List<File> expectedFiles = new ArrayList<>();
    for (int i = 0; i < numShards; i++) {
      expectedFiles.add(
          new File(
              rootLocation.getRoot(),
              IOChannelUtils.constructName(outputName, shardNameTemplate, "", i, numShards)));
    }

    List<String> actual = new ArrayList<>();
    for (File tmpFile : expectedFiles) {
      try (BufferedReader reader = new BufferedReader(new FileReader(tmpFile))) {
        for (;;) {
          String line = reader.readLine();
          if (line == null) {
            break;
          }
          actual.add(line);
        }
      }
    }

    String[] expected = new String[elems.length];
    for (int i = 0; i < elems.length; i++) {
      T elem = elems[i];
      byte[] encodedElem = CoderUtils.encodeToByteArray(coder, elem);
      String line = new String(encodedElem);
      expected[i] = line;
    }

    assertThat(actual, containsInAnyOrder(expected));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteStrings() throws Exception {
    runTestWrite(LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteEmptyStringsNoSharding() throws Exception {
    runTestWrite(NO_LINES_ARRAY, StringUtf8Coder.of(), 0);
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteEmptyStrings() throws Exception {
    runTestWrite(NO_LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteInts() throws Exception {
    runTestWrite(INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testWriteEmptyInts() throws Exception {
    runTestWrite(NO_INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testShardedWrite() throws Exception {
    runTestWrite(LINES_ARRAY, StringUtf8Coder.of(), 5);
  }

  @Test
  public void testWriteDisplayData() {
    TextIO.Write.Bound<?> write = TextIO.Write
        .to("foo")
        .withSuffix("bar")
        .withShardNameTemplate("-SS-of-NN-")
        .withNumShards(100)
        .withoutValidation();

    DisplayData displayData = DisplayData.from(write);

    assertThat(displayData, hasDisplayItem("filePrefix", "foo"));
    assertThat(displayData, hasDisplayItem("fileSuffix", "bar"));
    assertThat(displayData, hasDisplayItem("shardNameTemplate", "-SS-of-NN-"));
    assertThat(displayData, hasDisplayItem("numShards", 100));
    assertThat(displayData, hasDisplayItem("validation", false));
  }

  @Test
  @Category(RunnableOnService.class)
  @Ignore("[BEAM-436] DirectRunner RunnableOnService tempLocation configuration insufficient")
  public void testPrimitiveWriteDisplayData() throws IOException {
    PipelineOptions options = DisplayDataEvaluator.getDefaultOptions();
    String tempRoot = options.as(TestPipelineOptions.class).getTempRoot();
    String outputPath = IOChannelUtils.getFactory(tempRoot).resolve(tempRoot, "foobar");

    DisplayDataEvaluator evaluator = DisplayDataEvaluator.create();

    TextIO.Write.Bound<?> write = TextIO.Write.to(outputPath);

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("TextIO.Write should include the file prefix in its primitive display data",
        displayData, hasItem(hasDisplayItem(hasValue(startsWith(outputPath)))));
  }

  @Test
  public void testUnsupportedFilePattern() throws IOException {
    File outFolder = tmpFolder.newFolder();
    // Windows doesn't like resolving paths with * in them.
    String filename = outFolder.toPath().resolve("output@5").toString();

    Pipeline p = TestPipeline.create();

    PCollection<String> input =
        p.apply(Create.of(Arrays.asList(LINES_ARRAY))
            .withCoder(StringUtf8Coder.of()));

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Output name components are not allowed to contain");
    input.apply(TextIO.Write.to(filename));
  }

  /**
   * Recursive wildcards are not supported.
   * This tests "**".
   */
  @Test
  public void testBadWildcardRecursive() throws Exception {
    Pipeline pipeline = TestPipeline.create();

    // Check that applying does fail.
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("wildcard");

    pipeline.apply(TextIO.Read.from("gs://bucket/foo**/baz"));
  }

  @Test
  public void testReadWithoutValidationFlag() throws Exception {
    TextIO.Read.Bound<String> read = TextIO.Read.from("gs://bucket/foo*/baz");
    assertTrue(read.needsValidation());
    assertFalse(read.withoutValidation().needsValidation());
  }

  @Test
  public void testWriteWithoutValidationFlag() throws Exception {
    TextIO.Write.Bound<String> write = TextIO.Write.to("gs://bucket/foo/baz");
    assertTrue(write.needsValidation());
    assertFalse(write.withoutValidation().needsValidation());
  }

  @Test
  public void testCompressionTypeIsSet() throws Exception {
    TextIO.Read.Bound<String> read = TextIO.Read.from("gs://bucket/test");
    assertEquals(CompressionType.AUTO, read.getCompressionType());
    read = TextIO.Read.from("gs://bucket/test").withCompressionType(CompressionType.GZIP);
    assertEquals(CompressionType.GZIP, read.getCompressionType());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testCompressedRead() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    File tmpFile = tmpFolder.newFile();
    String filename = tmpFile.getPath();

    List<String> expected = new ArrayList<>();
    try (PrintStream writer =
        new PrintStream(new GZIPOutputStream(new FileOutputStream(tmpFile)))) {
      for (String line : lines) {
        writer.println(line);
        expected.add(line);
      }
    }

    Pipeline p = TestPipeline.create();

    TextIO.Read.Bound<String> read =
        TextIO.Read.from(filename).withCompressionType(CompressionType.GZIP);
    PCollection<String> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
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
  private String createZipFile(List<String> expected, @Nullable String filename, String[]
      ...
      fieldsEntries)
      throws Exception {
    File tmpFile;
    if (filename != null) {
      tmpFile = tmpFolder.newFile(filename);
    } else {
      tmpFile = tmpFolder.newFile();
    }
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

  /**
   * Read a zip compressed file. The user provides the ZIP compression type.
   * We expect a PCollection with the lines.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedRead() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    List<String> expected = new ArrayList<>();

    String filename = createZipFile(expected, null, lines);

    Pipeline p = TestPipeline.create();

    TextIO.Read.Bound<String> read =
        TextIO.Read.from(filename).withCompressionType(CompressionType.ZIP);
    PCollection<String> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  /**
   * Read a zip compressed file. The ZIP compression type is auto-detected based on the
   * file extension. We expect a PCollection with the lines.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedAutoDetected() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    List<String> expected = new ArrayList<>();

    String filename = createZipFile(expected, "testZipCompressedAutoDetected.zip", lines);

    // test with auto-detect ZIP based on extension.
    Pipeline p = TestPipeline.create();

    TextIO.Read.Bound<String> read = TextIO.Read.from(filename);
    PCollection<String> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  /**
   * Read a ZIP compressed empty file. We expect an empty PCollection.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedReadWithEmptyFile() throws Exception {
    String filename = createZipFile(new ArrayList<String>(), null);

    Pipeline p = TestPipeline.create();

    PCollection<String> output = p.apply(TextIO.Read.from(filename).withCompressionType
        (CompressionType.ZIP));
    PAssert.that(output).empty();

    p.run();
  }

  /**
   * Read a ZIP compressed file containing an unique empty entry. We expect an empty PCollection.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedReadWithEmptyEntry() throws Exception {
    String filename = createZipFile(new ArrayList<String>(), null, new String[]{ });

    Pipeline p = TestPipeline.create();

    PCollection<String> output = p.apply(TextIO.Read.from(filename).withCompressionType
        (CompressionType.ZIP));
    PAssert.that(output).empty();

    p.run();
  }

  /**
   * Read a ZIP compressed file with multiple entries. We expect a PCollection containing
   * lines from all entries.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedReadWithMultiEntriesFile() throws Exception {
    String[] entry0 = new String[]{ "first", "second", "three" };
    String[] entry1 = new String[]{ "four", "five", "six" };
    String[] entry2 = new String[]{ "seven", "eight", "nine" };

    List<String> expected = new ArrayList<>();

    String filename = createZipFile(expected, null, entry0, entry1, entry2);

    Pipeline p = TestPipeline.create();

    TextIO.Read.Bound<String> read =
        TextIO.Read.from(filename).withCompressionType(CompressionType.ZIP);
    PCollection<String> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  /**
   * Read a ZIP compressed file containing data, multiple empty entries, and then more data. We
   * expect just the data back.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testZipCompressedReadWithComplexEmptyAndPresentEntries() throws Exception {
    String filename = createZipFile(
        new ArrayList<String>(),
        null,
        new String[] {"cat"},
        new String[] {},
        new String[] {},
        new String[] {"dog"});
    List<String> expected = ImmutableList.of("cat", "dog");

    Pipeline p = TestPipeline.create();

    PCollection<String> output =
        p.apply(TextIO.Read.from(filename).withCompressionType(CompressionType.ZIP));
    PAssert.that(output).containsInAnyOrder(expected);

    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testGZIPReadWhenUncompressed() throws Exception {
    String[] lines = {"Meritorious condor", "Obnoxious duck"};
    File tmpFile = tmpFolder.newFile();
    String filename = tmpFile.getPath();

    List<String> expected = new ArrayList<>();
    try (PrintStream writer = new PrintStream(new FileOutputStream(tmpFile))) {
      for (String line : lines) {
        writer.println(line);
        expected.add(line);
      }
    }

    Pipeline p = TestPipeline.create();
    TextIO.Read.Bound<String> read =
        TextIO.Read.from(filename).withCompressionType(CompressionType.GZIP);
    PCollection<String> output = p.apply(read);

    PAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testTextIOGetName() {
    assertEquals("TextIO.Read", TextIO.Read.from("somefile").getName());
    assertEquals("TextIO.Write", TextIO.Write.to("somefile").getName());

    assertEquals("TextIO.Read", TextIO.Read.from("somefile").toString());
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
    BoundedSource source = prepareSource(file.getBytes());
    BoundedSource remainder;

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
    runTestReadWithData("\n\n\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("", "", ""));
  }

  @Test
  public void testReadFileWithLineFeedDelimiter() throws Exception {
    runTestReadWithData("asdf\nhjkl\nxyz\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnDelimiter() throws Exception {
    runTestReadWithData("asdf\rhjkl\rxyz\r".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnAndLineFeedDelimiter() throws Exception {
    runTestReadWithData("asdf\r\nhjkl\r\nxyz\r\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithMixedDelimiters() throws Exception {
    runTestReadWithData("asdf\rhjkl\r\nxyz\n".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithLineFeedDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData("asdf\nhjkl\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData("asdf\rhjkl\rxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithCarriageReturnAndLineFeedDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    runTestReadWithData("asdf\r\nhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  @Test
  public void testReadFileWithMixedDelimitersAndNonEmptyBytesAtEnd() throws Exception {
    runTestReadWithData("asdf\rhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8),
        ImmutableList.of("asdf", "hjkl", "xyz"));
  }

  private void runTestReadWithData(byte[] data, List<String> expectedResults) throws Exception {
    TextSource<String> source = prepareSource(data);
    List<String> actual = SourceTestUtils.readFromSource(source, PipelineOptionsFactory.create());
    assertThat(actual, containsInAnyOrder(new ArrayList<>(expectedResults).toArray(new String[0])));
  }

  @Test
  public void testSplittingSourceWithEmptyLines() throws Exception {
    TextSource<String> source = prepareSource("\n\n\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithLineFeedDelimiter() throws Exception {
    TextSource<String> source = prepareSource("asdf\nhjkl\nxyz\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnDelimiter() throws Exception {
    TextSource<String> source = prepareSource("asdf\rhjkl\rxyz\r".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnAndLineFeedDelimiter() throws Exception {
    TextSource<String> source = prepareSource(
        "asdf\r\nhjkl\r\nxyz\r\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithMixedDelimiters() throws Exception {
    TextSource<String> source = prepareSource(
        "asdf\rhjkl\r\nxyz\n".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithLineFeedDelimiterAndNonEmptyBytesAtEnd() throws Exception {
    TextSource<String> source = prepareSource("asdf\nhjkl\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    TextSource<String> source = prepareSource("asdf\rhjkl\rxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithCarriageReturnAndLineFeedDelimiterAndNonEmptyBytesAtEnd()
      throws Exception {
    TextSource<String> source = prepareSource(
        "asdf\r\nhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  @Test
  public void testSplittingSourceWithMixedDelimitersAndNonEmptyBytesAtEnd() throws Exception {
    TextSource<String> source = prepareSource("asdf\rhjkl\r\nxyz".getBytes(StandardCharsets.UTF_8));
    SourceTestUtils.assertSplitAtFractionExhaustive(source, PipelineOptionsFactory.create());
  }

  private TextSource<String> prepareSource(byte[] data) throws IOException {
    File file = tmpFolder.newFile();
    Files.write(file.toPath(), data);

    TextSource<String> source = new TextSource<>(file.toPath().toString(), StringUtf8Coder.of());

    return source;
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////
  // Test "gs://" paths

  private GcsUtil buildMockGcsUtil() throws IOException {
    GcsUtil mockGcsUtil = Mockito.mock(GcsUtil.class);

    // Any request to open gets a new bogus channel
    Mockito
        .when(mockGcsUtil.open(Mockito.any(GcsPath.class)))
        .then(new Answer<SeekableByteChannel>() {
          @Override
          public SeekableByteChannel answer(InvocationOnMock invocation) throws Throwable {
            return FileChannel.open(
                Files.createTempFile("channel-", ".tmp"),
                StandardOpenOption.CREATE, StandardOpenOption.DELETE_ON_CLOSE);
          }
        });

    // Any request for expansion returns a list containing the original GcsPath
    // This is required to pass validation that occurs in TextIO during apply()
    Mockito
        .when(mockGcsUtil.expand(Mockito.any(GcsPath.class)))
        .then(new Answer<List<GcsPath>>() {
          @Override
          public List<GcsPath> answer(InvocationOnMock invocation) throws Throwable {
            return ImmutableList.of((GcsPath) invocation.getArguments()[0]);
          }
        });

    return mockGcsUtil;
  }

  /**
   * This tests a few corner cases that should not crash.
   */
  @Test
  @Category(NeedsRunner.class)
  public void testGoodWildcards() throws Exception {
    GcsOptions options = TestPipeline.testingPipelineOptions().as(GcsOptions.class);
    options.setGcsUtil(buildMockGcsUtil());

    Pipeline pipeline = Pipeline.create(options);

    applyRead(pipeline, "gs://bucket/foo");
    applyRead(pipeline, "gs://bucket/foo/");
    applyRead(pipeline, "gs://bucket/foo/*");
    applyRead(pipeline, "gs://bucket/foo/?");
    applyRead(pipeline, "gs://bucket/foo/[0-9]");
    applyRead(pipeline, "gs://bucket/foo/*baz*");
    applyRead(pipeline, "gs://bucket/foo/*baz?");
    applyRead(pipeline, "gs://bucket/foo/[0-9]baz?");
    applyRead(pipeline, "gs://bucket/foo/baz/*");
    applyRead(pipeline, "gs://bucket/foo/baz/*wonka*");
    applyRead(pipeline, "gs://bucket/foo/*baz/wonka*");
    applyRead(pipeline, "gs://bucket/foo*/baz");
    applyRead(pipeline, "gs://bucket/foo?/baz");
    applyRead(pipeline, "gs://bucket/foo[0-9]/baz");

    // Check that running doesn't fail.
    pipeline.run();
  }

  private void applyRead(Pipeline pipeline, String path) {
    pipeline.apply("Read(" + path + ")", TextIO.Read.from(path));
  }
}
