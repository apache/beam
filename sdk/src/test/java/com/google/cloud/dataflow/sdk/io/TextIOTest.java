/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static com.google.cloud.dataflow.sdk.TestUtils.INTS_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.LINES_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_INTS_ARRAY;
import static com.google.cloud.dataflow.sdk.TestUtils.NO_LINES_ARRAY;
import static com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers.hasDisplayItem;
import static com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers.hasValue;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.VoidCoder;
import com.google.cloud.dataflow.sdk.io.BoundedSource.BoundedReader;
import com.google.cloud.dataflow.sdk.io.TextIO.CompressionType;
import com.google.cloud.dataflow.sdk.io.TextIO.TextSource;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.testing.DataflowAssert;
import com.google.cloud.dataflow.sdk.testing.SourceTestUtils;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.testing.TestPipeline;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.display.DataflowDisplayDataEvaluator;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayData;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayDataEvaluator;
import com.google.cloud.dataflow.sdk.transforms.display.DisplayDataMatchers;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.GcsUtil;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PDone;
import com.google.common.collect.ImmutableList;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.junit.Rule;
import org.junit.Test;
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
import java.io.OutputStream;
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

/**
 * Tests for TextIO Read and Write transforms.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class TextIOTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule public ExpectedException expectedException = ExpectedException.none();

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

  private TestDataflowPipelineOptions buildTestPipelineOptions() {
    TestDataflowPipelineOptions options =
        PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());
    return options;
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

    DataflowAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testReadStrings() throws Exception {
    runTestRead(LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  public void testReadEmptyStrings() throws Exception {
    runTestRead(NO_LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  public void testReadInts() throws Exception {
    runTestRead(INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  public void testReadEmptyInts() throws Exception {
    runTestRead(NO_INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
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
      PCollection<String> output2 =
          p.apply(TextIO.Read.named("MyRead").from(file));
      assertEquals("MyRead/Read.out", output2.getName());
    }

    {
      PCollection<String> output3 =
          p.apply(TextIO.Read.from(file).named("HerRead"));
      assertEquals("HerRead/Read.out", output3.getName());
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
    } else {
      write = write.withNumShards(numShards).withShardNameTemplate(ShardNameTemplate.INDEX_OF_MAX);
    }

    input.apply(write);

    p.run();

    assertOutputFiles(elems, coder, numShards, tmpFolder, outputName, write.getShardNameTemplate());
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
  public void testWriteStrings() throws Exception {
    runTestWrite(LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  public void testWriteEmptyStrings() throws Exception {
    runTestWrite(NO_LINES_ARRAY, StringUtf8Coder.of());
  }

  @Test
  public void testWriteInts() throws Exception {
    runTestWrite(INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  public void testWriteEmptyInts() throws Exception {
    runTestWrite(NO_INTS_ARRAY, TextualIntegerCoder.of());
  }

  @Test
  public void testWriteNamed() {
    {
      PTransform<PCollection<String>, PDone> transform1 =
        TextIO.Write.to("/tmp/file.txt");
      assertEquals("TextIO.Write", transform1.getName());
    }

    {
      PTransform<PCollection<String>, PDone> transform2 =
          TextIO.Write.named("MyWrite").to("/tmp/file.txt");
      assertEquals("MyWrite", transform2.getName());
    }

    {
      PTransform<PCollection<String>, PDone> transform3 =
          TextIO.Write.to("/tmp/file.txt").named("HerWrite");
      assertEquals("HerWrite", transform3.getName());
    }
  }

  @Test
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
  public void testPrimitiveWriteDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();

    TextIO.Write.Bound<?> write = TextIO.Write.to("foobar");

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(write);
    assertThat("TextIO.Write should include the file prefix in its primitive display data",
        displayData, hasItem(hasDisplayItem(hasValue(startsWith("foobar")))));
  }

  @Test
  public void testPrimitiveReadDisplayData() {
    DisplayDataEvaluator evaluator = DataflowDisplayDataEvaluator.create();

    TextIO.Read.Bound<String> read = TextIO.Read
        .from("foobar")
        .withoutValidation();

    Set<DisplayData> displayData = evaluator.displayDataForPrimitiveTransforms(read);
    assertThat("TextIO.Read should include the file prefix in its primitive display data",
        displayData,
        hasItem(hasDisplayItem(DisplayDataMatchers.hasValue(startsWith("foobar")))));
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
   * This tests a few corner cases that should not crash.
   */
  @Test
  public void testGoodWildcards() throws Exception {
    TestDataflowPipelineOptions options = buildTestPipelineOptions();
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

  /**
   * Helper that writes the given lines (adding a newline in between) to a stream, then closes the
   * stream.
   */
  private static void writeToStreamAndClose(String[] lines, OutputStream outputStream) {
    try (PrintStream writer = new PrintStream(outputStream)) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

  /**
   * Helper method that runs TextIO.Read.from(filename).withCompressionType(compressionType)
   * and asserts that the results match the given expected output.
   */
  private static void assertReadingCompressedFileMatchesExpected(
      String filename, CompressionType compressionType, String[] expected) {
    Pipeline p = TestPipeline.create();
    TextIO.Read.Bound<String> read =
        TextIO.Read.from(filename).withCompressionType(compressionType);
    PCollection<String> output = p.apply(read);

    DataflowAssert.that(output).containsInAnyOrder(expected);
    p.run();
  }

  /** Helper to make an array of compressible strings. Returns ["word"+i] for i in range(0,n). */
  private static String[] makeLines(int n) {
    String[] ret = new String[n];
    for (int i = 0; i < n; ++i) {
      ret[i] = "word" + i;
    }
    return ret;
  }

  /** Tests reading from a small, gzipped file with no .gz extension but GZIP compression set. */
  @Test
  public void testSmallCompressedGzipReadNoExtension() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    File tmpFile = tmpFolder.newFile(); // no GZ extension
    String filename = tmpFile.getPath();

    writeToStreamAndClose(lines, new GZIPOutputStream(new FileOutputStream(tmpFile)));
    assertReadingCompressedFileMatchesExpected(filename, CompressionType.GZIP, lines);
  }

  /**
   * Tests reading from a small, gzipped file with .gz extension and AUTO or GZIP compression set.
   */
  @Test
  public void testSmallCompressedGzipRead() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    File tmpFile = tmpFolder.newFile("small_gzip.gz");
    String filename = tmpFile.getPath();

    writeToStreamAndClose(lines, new GZIPOutputStream(new FileOutputStream(tmpFile)));
    // Should work in AUTO mode.
    assertReadingCompressedFileMatchesExpected(filename, CompressionType.AUTO, lines);
    // Should work in GZIP mode.
    assertReadingCompressedFileMatchesExpected(filename, CompressionType.GZIP, lines);
  }

  /**
   * Tests reading from a small, uncompressed file with .gz extension.
   * This must work in AUTO or GZIP modes. This is needed because some network file systems / HTTP
   * clients will transparently decompress gzipped content.
   */
  @Test
  public void testSmallCompressedGzipReadActuallyUncompressed() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    File tmpFile = tmpFolder.newFile("not_really_gzipped.gz"); // GZ file extension lies
    String filename = tmpFile.getPath();

    writeToStreamAndClose(lines, new FileOutputStream(tmpFile));
    // Should work with GZIP compression set.
    assertReadingCompressedFileMatchesExpected(filename, CompressionType.GZIP, lines);
    // Should also work with AUTO mode set.
    assertReadingCompressedFileMatchesExpected(filename, CompressionType.AUTO, lines);
  }

  /** Tests reading from a small, bzip2ed file with no .bz2 extension but BZIP2 compression set. */
  @Test
  public void testSmallCompressedBzip2ReadNoExtension() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    File tmpFile = tmpFolder.newFile(); // no BZ2 extension
    String filename = tmpFile.getPath();

    writeToStreamAndClose(lines, new BZip2CompressorOutputStream(new FileOutputStream(tmpFile)));
    assertReadingCompressedFileMatchesExpected(filename, CompressionType.BZIP2, lines);
  }

  /**
   * Tests reading from a small, bzip2ed file with .bz2 extension and AUTO or BZIP2 compression set.
   */
  @Test
  public void testSmallCompressedBzipRead() throws Exception {
    String[] lines = {"Irritable eagle", "Optimistic jay", "Fanciful hawk"};
    File tmpFile = tmpFolder.newFile("small_bzip2.bz2");
    String filename = tmpFile.getPath();

    writeToStreamAndClose(lines, new BZip2CompressorOutputStream(new FileOutputStream(tmpFile)));
    // Should work in AUTO mode.
    assertReadingCompressedFileMatchesExpected(filename, CompressionType.AUTO, lines);
    // Should work in BZIP2 mode.
    assertReadingCompressedFileMatchesExpected(filename, CompressionType.BZIP2, lines);
  }

  /**
   * Tests reading from a large, bzip2ed file with .bz2 extension and AUTO or BZIP2 compression set.
   * It is important to test a large compressible file because using only small files may mask bugs
   * from range tracking that can only occur if the file compression ratio is high -- small
   * compressed files are usually as big as the uncompressed ones or bigger.
   */
  @Test
  public void testLargeCompressedBzipRead() throws Exception {
    String[] lines = makeLines(5000);
    File bz2File = tmpFolder.newFile("large_bzip2.bz2");
    String bz2Filename = bz2File.getPath();

    writeToStreamAndClose(lines, new BZip2CompressorOutputStream(new FileOutputStream(bz2File)));
    // Should work in AUTO mode.
    assertReadingCompressedFileMatchesExpected(bz2Filename, CompressionType.AUTO, lines);
    // Should work in BZIP2 mode.
    assertReadingCompressedFileMatchesExpected(bz2Filename, CompressionType.BZIP2, lines);

    // Confirm that the compressed file is smaller than the uncompressed file.
    File txtFile = tmpFolder.newFile("large_bzip2.txt");
    writeToStreamAndClose(lines, new FileOutputStream(txtFile));
    assertThat(Files.size(txtFile.toPath()), greaterThan(Files.size(bz2File.toPath())));
  }

  /**
   * Tests reading from a large, gzipped file with .gz extension and AUTO or GZIP compression set.
   * It is important to test a large compressible file because using only small files may mask bugs
   * from range tracking that can only occur if the file compression ratio is high -- small
   * compressed files are usually as big as the uncompressed ones or bigger.
   */
  @Test
  public void testLargeCompressedGzipRead() throws Exception {
    String[] lines = makeLines(5000);
    File gzFile = tmpFolder.newFile("large_gzip.gz");
    String gzFilename = gzFile.getPath();

    writeToStreamAndClose(lines, new GZIPOutputStream(new FileOutputStream(gzFile)));
    // Should work in AUTO mode.
    assertReadingCompressedFileMatchesExpected(gzFilename, CompressionType.AUTO, lines);
    // Should work in BZIP2 mode.
    assertReadingCompressedFileMatchesExpected(gzFilename, CompressionType.GZIP, lines);

    // Confirm that the compressed file is smaller than the uncompressed file.
    File txtFile = tmpFolder.newFile("large_gzip.txt");
    writeToStreamAndClose(lines, new FileOutputStream(txtFile));
    assertThat(Files.size(txtFile.toPath()), greaterThan(Files.size(gzFile.toPath())));
  }

  /**
   * Tests reading from a large, uncompressed file.
   */
  @Test
  public void testLargeUncompressedReadTxt() throws Exception {
    String[] lines = makeLines(5000);
    File txtFile = tmpFolder.newFile("large_file.txt");
    String txtFilename = txtFile.getPath();

    writeToStreamAndClose(lines, new FileOutputStream(txtFile));
    // Should work in AUTO mode.
    assertReadingCompressedFileMatchesExpected(txtFilename, CompressionType.AUTO, lines);
  }

  /**
   * Tests reading from a large, uncompressed file with a weird file extension.
   */
  @Test
  public void testLargeUncompressedReadWeirdExtension() throws Exception {
    String[] lines = makeLines(5000);
    File txtFile = tmpFolder.newFile("large_file.bin.data.foo");
    String txtFilename = txtFile.getPath();

    writeToStreamAndClose(lines, new FileOutputStream(txtFile));
    // Should work in AUTO mode.
    assertReadingCompressedFileMatchesExpected(txtFilename, CompressionType.AUTO, lines);
  }

  @Test
  public void testTextIOGetName() {
    assertEquals("TextIO.Read", TextIO.Read.from("somefile").getName());
    assertEquals("TextIO.Write", TextIO.Write.to("somefile").getName());
    assertEquals("ReadMyFile", TextIO.Read.named("ReadMyFile").from("somefile").getName());
    assertEquals("WriteMyFile", TextIO.Write.named("WriteMyFile").to("somefile").getName());

    assertEquals("TextIO.Read", TextIO.Read.from("somefile").toString());
    assertEquals(
        "ReadMyFile [TextIO.Read]", TextIO.Read.named("ReadMyFile").from("somefile").toString());
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
}
