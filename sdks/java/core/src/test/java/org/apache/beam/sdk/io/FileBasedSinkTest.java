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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.beam.sdk.io.FileBasedSink.CompressionType;
import org.apache.beam.sdk.io.FileBasedSink.FileBasedWriteOperation;
import org.apache.beam.sdk.io.FileBasedSink.FileBasedWriter;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.Context;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for FileBasedSink.
 */
@RunWith(JUnit4.class)
public class FileBasedSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private String baseOutputFilename = "output";
  private String tempDirectory = "temp";

  private String appendToTempFolder(String filename) {
    return Paths.get(tmpFolder.getRoot().getPath(), filename).toString();
  }

  private String getBaseOutputFilename() {
    return appendToTempFolder(baseOutputFilename);
  }

  private String getBaseTempDirectory() {
    return appendToTempFolder(tempDirectory);
  }

  /**
   * FileBasedWriter opens the correct file, writes the header, footer, and elements in the
   * correct order, and returns the correct filename.
   */
  @Test
  public void testWriter() throws Exception {
    String testUid = "testId";
    String expectedFilename = IOChannelUtils.resolve(getBaseTempDirectory(), testUid);
    SimpleSink.SimpleWriter writer = buildWriter();

    List<String> values = Arrays.asList("sympathetic vulture", "boresome hummingbird");
    List<String> expected = new ArrayList<>();
    expected.add(SimpleSink.SimpleWriter.HEADER);
    expected.addAll(values);
    expected.add(SimpleSink.SimpleWriter.FOOTER);

    writer.openUnwindowed(testUid, -1, -1);
    for (String value : values) {
      writer.write(value);
    }
    FileResult result = writer.close();

    assertEquals(expectedFilename, result.getFilename());
    assertFileContains(expected, expectedFilename);
  }

  /**
   * Assert that a file contains the lines provided, in the same order as expected.
   */
  private void assertFileContains(List<String> expected, String filename) throws Exception {
    try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
      List<String> actual = new ArrayList<>();
      for (;;) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        actual.add(line);
      }
      assertEquals(expected, actual);
    }
  }

  /**
   * Write lines to a file.
   */
  private void writeFile(List<String> lines, File file) throws Exception {
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(file))) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

  /**
   * Removes temporary files when temporary and output filenames differ.
   */
  @Test
  public void testRemoveWithTempFilename() throws Exception {
    testRemoveTemporaryFiles(3, tempDirectory);
  }

  /**
   * Removes only temporary files, even if temporary and output files share the same base filename.
   */
  @Test
  public void testRemoveWithSameFilename() throws Exception {
    testRemoveTemporaryFiles(3, baseOutputFilename);
  }

  /**
   * Finalize copies temporary files to output files and removes any temporary files.
   */
  @Test
  public void testFinalize() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    runFinalize(buildWriteOperation(), files);
  }

  /**
   * Finalize can be called repeatedly.
   */
  @Test
  public void testFinalizeMultipleCalls() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();
    runFinalize(writeOp, files);
    runFinalize(writeOp, files);
  }

  /**
   * Finalize can be called when some temporary files do not exist and output files exist.
   */
  @Test
  public void testFinalizeWithIntermediateState() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();
    runFinalize(writeOp, files);

    // create a temporary file
    tmpFolder.newFolder(tempDirectory);
    tmpFolder.newFile(tempDirectory + "/1");

    runFinalize(writeOp, files);
  }

  /**
   * Generate n temporary files using the temporary file pattern of FileBasedWriter.
   */
  private List<File> generateTemporaryFilesForFinalize(int numFiles) throws Exception {
    List<File> temporaryFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String temporaryFilename =
          FileBasedWriteOperation.buildTemporaryFilename(tempDirectory, "" + i);
      File tmpFile = new File(tmpFolder.getRoot(), temporaryFilename);
      tmpFile.getParentFile().mkdirs();
      assertTrue(tmpFile.createNewFile());
      temporaryFiles.add(tmpFile);
    }

    return temporaryFiles;
  }

  /**
   * Finalize and verify that files are copied and temporary files are optionally removed.
   */
  private void runFinalize(SimpleSink.SimpleWriteOperation writeOp, List<File> temporaryFiles)
      throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();

    int numFiles = temporaryFiles.size();

    List<FileResult> fileResults = new ArrayList<>();
    // Create temporary output bundles and output File objects.
    for (int i = 0; i < numFiles; i++) {
      fileResults.add(new FileResult(temporaryFiles.get(i).toString(), null));
    }

    writeOp.finalize(fileResults, options);

    for (int i = 0; i < numFiles; i++) {
      String outputFilename = writeOp.getSink().getFileNamePolicy().unwindowedFilename(
          new Context(i, numFiles));
      assertTrue(new File(outputFilename).exists());
      assertFalse(temporaryFiles.get(i).exists());
    }

    assertFalse(new File(writeOp.tempDirectory.get()).exists());
    // Test that repeated requests of the temp directory return a stable result.
    assertEquals(writeOp.tempDirectory.get(), writeOp.tempDirectory.get());
  }

  /**
   * Create n temporary and output files and verify that removeTemporaryFiles only
   * removes temporary files.
   */
  private void testRemoveTemporaryFiles(int numFiles, String baseTemporaryFilename)
      throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation(baseTemporaryFilename);

    List<File> temporaryFiles = new ArrayList<>();
    List<File> outputFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      File tmpFile = new File(tmpFolder.getRoot(),
          FileBasedWriteOperation.buildTemporaryFilename(baseTemporaryFilename, "" + i));
      tmpFile.getParentFile().mkdirs();
      assertTrue(tmpFile.createNewFile());
      temporaryFiles.add(tmpFile);
      File outputFile = tmpFolder.newFile(baseOutputFilename + i);
      outputFiles.add(outputFile);
    }

    writeOp.removeTemporaryFiles(Collections.<String>emptySet(), true, options);

    for (int i = 0; i < numFiles; i++) {
      assertFalse(temporaryFiles.get(i).exists());
      assertTrue(outputFiles.get(i).exists());
    }
  }

  /**
   * Output files are copied to the destination location with the correct names and contents.
   */
  @Test
  public void testCopyToOutputFiles() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();

    List<String> inputFilenames = Arrays.asList("input-1", "input-2", "input-3");
    List<String> inputContents = Arrays.asList("1", "2", "3");
    List<String> expectedOutputFilenames = Arrays.asList(
        "output-00000-of-00003.test", "output-00001-of-00003.test", "output-00002-of-00003.test");

    Map<String, String> inputFilePaths = new HashMap<>();
    List<String> expectedOutputPaths = new ArrayList<>();

    for (int i = 0; i < inputFilenames.size(); i++) {
      // Generate output paths.
      File outputFile = tmpFolder.newFile(expectedOutputFilenames.get(i));
      expectedOutputPaths.add(outputFile.toString());

      // Generate and write to input paths.
      File inputTmpFile = tmpFolder.newFile(inputFilenames.get(i));
      List<String> lines = Arrays.asList(inputContents.get(i));
      writeFile(lines, inputTmpFile);
      inputFilePaths.put(inputTmpFile.toString(),
          writeOp.getSink().getFileNamePolicy().unwindowedFilename(
              new Context(i, inputFilenames.size())));
    }

    // Copy input files to output files.
    writeOp.copyToOutputFiles(inputFilePaths, options);

    // Assert that the contents were copied.
    for (int i = 0; i < expectedOutputPaths.size(); i++) {
      assertFileContains(Arrays.asList(inputContents.get(i)), expectedOutputPaths.get(i));
    }
  }

  public List<String> generateDestinationFilenames(FilenamePolicy policy, int numFiles) {
    List<String> filenames = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      filenames.add(policy.unwindowedFilename(new Context(i, numFiles)));
    }
    return filenames;
  }

  /**
   * Output filenames use the supplied naming template.
   */
  @Test
  public void testGenerateOutputFilenamesWithTemplate() {
    List<String> expected;
    List<String> actual;
    SimpleSink sink = new SimpleSink(getBaseOutputFilename(), "test", ".SS.of.NN");
    FilenamePolicy policy = sink.getFileNamePolicy();

    expected = Arrays.asList(appendToTempFolder("output.00.of.03.test"),
        appendToTempFolder("output.01.of.03.test"), appendToTempFolder("output.02.of.03.test"));
    actual = generateDestinationFilenames(policy, 3);
    assertEquals(expected, actual);

    expected = Arrays.asList(appendToTempFolder("output.00.of.01.test"));
    actual = generateDestinationFilenames(policy, 1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = generateDestinationFilenames(policy, 0);
    assertEquals(expected, actual);

    // Also validate that we handle the case where the user specified "." that we do
    // not prefix an additional "." making "..test"
    sink = new SimpleSink(getBaseOutputFilename(), ".test", ".SS.of.NN");
    expected = Arrays.asList(appendToTempFolder("output.00.of.03.test"),
        appendToTempFolder("output.01.of.03.test"), appendToTempFolder("output.02.of.03.test"));
    actual = generateDestinationFilenames(policy, 3);
    assertEquals(expected, actual);

    expected = Arrays.asList(appendToTempFolder("output.00.of.01.test"));
    actual = generateDestinationFilenames(policy, 1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = generateDestinationFilenames(policy, 0);
    assertEquals(expected, actual);
  }

  /**
   * Output filenames are generated correctly when an extension is supplied.
   */
  @Test
  public void testGenerateOutputFilenamesWithExtension() {
    List<String> expected;
    List<String> actual;
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();
    FilenamePolicy policy = writeOp.getSink().getFileNamePolicy();

    expected = Arrays.asList(
        appendToTempFolder("output-00000-of-00003.test"),
        appendToTempFolder("output-00001-of-00003.test"),
        appendToTempFolder("output-00002-of-00003.test"));
    actual = generateDestinationFilenames(policy, 3);
    assertEquals(expected, actual);

    expected = Arrays.asList(appendToTempFolder("output-00000-of-00001.test"));
    actual = generateDestinationFilenames(policy, 1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = generateDestinationFilenames(policy, 0);
    assertEquals(expected, actual);
  }

  /**
   * Reject non-distinct output filenames.
   */
  @Test
  public void testCollidingOutputFilenames() {
    SimpleSink sink = new SimpleSink("output", "test", "-NN");
    SimpleSink.SimpleWriteOperation writeOp = new SimpleSink.SimpleWriteOperation(sink);

    // More than one shard does.
    try {
      Iterable<FileResult> results = Lists.newArrayList(
          new FileResult("temp1", "file1"),
          new FileResult("temp2", "file1"),
          new FileResult("temp3", "file1"));

      writeOp.buildOutputFilenames(results);
      fail("Should have failed.");
    } catch (IllegalStateException exn) {
      assertEquals("Only generated 1 distinct file names for 3 files.",
                   exn.getMessage());
    }
  }

  /**
   * Output filenames are generated correctly when an extension is not supplied.
   */
  @Test
  public void testGenerateOutputFilenamesWithoutExtension() {
    List<String> expected;
    List<String> actual;
    SimpleSink sink = new SimpleSink(appendToTempFolder(baseOutputFilename), "");
    FilenamePolicy policy = sink.getFileNamePolicy();

    expected = Arrays.asList(appendToTempFolder("output-00000-of-00003"),
        appendToTempFolder("output-00001-of-00003"), appendToTempFolder("output-00002-of-00003"));
    actual = generateDestinationFilenames(policy, 3);
    assertEquals(expected, actual);

    expected = Arrays.asList(appendToTempFolder("output-00000-of-00001"));
    actual = generateDestinationFilenames(policy, 1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = generateDestinationFilenames(policy, 0);
    assertEquals(expected, actual);
  }

  /**
   * {@link CompressionType#BZIP2} correctly writes BZip2 data.
   */
  @Test
  public void testCompressionTypeBZIP2() throws FileNotFoundException, IOException {
    final File file =
        writeValuesWithWritableByteChannelFactory(CompressionType.BZIP2, "abc", "123");
    // Read Bzip2ed data back in using Apache commons API (de facto standard).
    assertReadValues(new BufferedReader(new InputStreamReader(
        new BZip2CompressorInputStream(new FileInputStream(file)), StandardCharsets.UTF_8.name())),
        "abc", "123");
  }

  /**
   * {@link CompressionType#GZIP} correctly writes Gzipped data.
   */
  @Test
  public void testCompressionTypeGZIP() throws FileNotFoundException, IOException {
    final File file = writeValuesWithWritableByteChannelFactory(CompressionType.GZIP, "abc", "123");
    // Read Gzipped data back in using standard API.
    assertReadValues(new BufferedReader(new InputStreamReader(
        new GZIPInputStream(new FileInputStream(file)), StandardCharsets.UTF_8.name())), "abc",
        "123");
  }

  /**
   * {@link CompressionType#DEFLATE} correctly writes deflate data.
   */
  @Test
  public void testCompressionTypeDEFLATE() throws FileNotFoundException, IOException {
    final File file = writeValuesWithWritableByteChannelFactory(
        CompressionType.DEFLATE, "abc", "123");
    // Read Gzipped data back in using standard API.
    assertReadValues(new BufferedReader(new InputStreamReader(new DeflateCompressorInputStream(
        new FileInputStream(file)), StandardCharsets.UTF_8.name())), "abc", "123");
  }

  /**
   * {@link CompressionType#UNCOMPRESSED} correctly writes uncompressed data.
   */
  @Test
  public void testCompressionTypeUNCOMPRESSED() throws FileNotFoundException, IOException {
    final File file =
        writeValuesWithWritableByteChannelFactory(CompressionType.UNCOMPRESSED, "abc", "123");
    // Read uncompressed data back in using standard API.
    assertReadValues(new BufferedReader(new InputStreamReader(
        new FileInputStream(file), StandardCharsets.UTF_8.name())), "abc",
        "123");
  }

  private void assertReadValues(final BufferedReader br, String... values) throws IOException {
    try (final BufferedReader _br = br) {
      for (String value : values) {
        assertEquals(String.format("Line should read '%s'", value), value, _br.readLine());
      }
    }
  }

  private File writeValuesWithWritableByteChannelFactory(final WritableByteChannelFactory factory,
      String... values)
      throws IOException, FileNotFoundException {
    final File file = tmpFolder.newFile("test.gz");
    final WritableByteChannel channel =
        factory.create(Channels.newChannel(new FileOutputStream(file)));
    for (String value : values) {
      channel.write(ByteBuffer.wrap((value + "\n").getBytes(StandardCharsets.UTF_8)));
    }
    channel.close();
    return file;
  }

  /**
   * {@link FileBasedWriter} writes to the {@link WritableByteChannel} provided by
   * {@link DrunkWritableByteChannelFactory}.
   */
  @Test
  public void testFileBasedWriterWithWritableByteChannelFactory() throws Exception {
    final String testUid = "testId";
    SimpleSink.SimpleWriteOperation writeOp =
        new SimpleSink(getBaseOutputFilename(), "txt", new DrunkWritableByteChannelFactory())
            .createWriteOperation(null);
    final FileBasedWriter<String> writer =
        writeOp.createWriter(null);
    final String expectedFilename = IOChannelUtils.resolve(writeOp.tempDirectory.get(), testUid);

    final List<String> expected = new ArrayList<>();
    expected.add("header");
    expected.add("header");
    expected.add("a");
    expected.add("a");
    expected.add("b");
    expected.add("b");
    expected.add("footer");
    expected.add("footer");

    writer.openUnwindowed(testUid, -1, -1);
    writer.write("a");
    writer.write("b");
    final FileResult result = writer.close();

    assertEquals(expectedFilename, result.getFilename());
    assertFileContains(expected, expectedFilename);
  }

  /**
   * A simple FileBasedSink that writes String values as lines with header and footer lines.
   */
  private static final class SimpleSink extends FileBasedSink<String> {
    public SimpleSink(String baseOutputFilename, String extension) {
      super(baseOutputFilename, extension);
    }

    public SimpleSink(String baseOutputFilename, String extension,
        WritableByteChannelFactory writableByteChannelFactory) {
      super(baseOutputFilename, extension, writableByteChannelFactory);
    }

    public SimpleSink(String baseOutputFilename, String extension, String fileNamingTemplate) {
      super(baseOutputFilename, extension, fileNamingTemplate);
    }

    @Override
    public SimpleWriteOperation createWriteOperation(PipelineOptions options) {
      return new SimpleWriteOperation(this);
    }

    private static final class SimpleWriteOperation extends FileBasedWriteOperation<String> {
      public SimpleWriteOperation(SimpleSink sink, String tempOutputFilename) {
        super(sink, tempOutputFilename);
      }

      public SimpleWriteOperation(SimpleSink sink) {
        super(sink);
      }

      @Override
      public SimpleWriter createWriter(PipelineOptions options) throws Exception {
        return new SimpleWriter(this);
      }
    }

    private static final class SimpleWriter extends FileBasedWriter<String> {
      static final String HEADER = "header";
      static final String FOOTER = "footer";

      private WritableByteChannel channel;

      public SimpleWriter(SimpleWriteOperation writeOperation) {
        super(writeOperation);
      }

      private static ByteBuffer wrap(String value) throws Exception {
        return ByteBuffer.wrap((value + "\n").getBytes("UTF-8"));
      }

      @Override
      protected void prepareWrite(WritableByteChannel channel) throws Exception {
        this.channel = channel;
      }

      @Override
      protected void writeHeader() throws Exception {
        channel.write(wrap(HEADER));
      }

      @Override
      protected void writeFooter() throws Exception {
        channel.write(wrap(FOOTER));
      }

      @Override
      public void write(String value) throws Exception {
        channel.write(wrap(value));
      }
    }
  }

  /**
   * Build a SimpleSink with default options.
   */
  private SimpleSink buildSink() {
    return new SimpleSink(getBaseOutputFilename(), "test");
  }

  /**
   * Build a SimpleWriteOperation with default options and the given base temporary filename.
   */
  private SimpleSink.SimpleWriteOperation buildWriteOperation(String baseTemporaryFilename) {
    SimpleSink sink = buildSink();
    return new SimpleSink.SimpleWriteOperation(sink, appendToTempFolder(baseTemporaryFilename));
  }

  /**
   * Build a write operation with the default options for it and its parent sink.
   */
  private SimpleSink.SimpleWriteOperation buildWriteOperation() {
    SimpleSink sink = buildSink();
    return new SimpleSink.SimpleWriteOperation(sink, getBaseTempDirectory());
  }

  /**
   * Build a writer with the default options for its parent write operation and sink.
   */
  private SimpleSink.SimpleWriter buildWriter() {
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();
    return new SimpleSink.SimpleWriter(writeOp);
  }
}
