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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.io.FileBasedSink.CompressionType;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy.Context;
import org.apache.beam.sdk.io.FileBasedSink.WritableByteChannelFactory;
import org.apache.beam.sdk.io.FileBasedSink.WriteOperation;
import org.apache.beam.sdk.io.FileBasedSink.Writer;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link FileBasedSink}.
 */
@RunWith(JUnit4.class)
public class FileBasedSinkTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  private final String tempDirectoryName = "temp";

  private ResourceId getTemporaryFolder() {
    return LocalResources.fromFile(tmpFolder.getRoot(), /* isDirectory */ true);
  }

  private ResourceId getBaseOutputDirectory() {
    String baseOutputDirname = "output";
    return getTemporaryFolder()
        .resolve(baseOutputDirname, StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  private ResourceId getBaseTempDirectory() {
    return getTemporaryFolder()
        .resolve(tempDirectoryName, StandardResolveOptions.RESOLVE_DIRECTORY);
  }

  /**
   * Writer opens the correct file, writes the header, footer, and elements in the correct
   * order, and returns the correct filename.
   */
  @Test
  public void testWriter() throws Exception {
    String testUid = "testId";
    ResourceId expectedTempFile = getBaseTempDirectory()
        .resolve(testUid, StandardResolveOptions.RESOLVE_FILE);
    List<String> values = Arrays.asList("sympathetic vulture", "boresome hummingbird");
    List<String> expected = new ArrayList<>();
    expected.add(SimpleSink.SimpleWriter.HEADER);
    expected.addAll(values);
    expected.add(SimpleSink.SimpleWriter.FOOTER);

    SimpleSink.SimpleWriter writer =
        buildWriteOperationWithTempDir(getBaseTempDirectory()).createWriter();
    writer.openUnwindowed(testUid, -1, null);
    for (String value : values) {
      writer.write(value);
    }
    FileResult result = writer.close();

    FileBasedSink sink = writer.getWriteOperation().getSink();
    assertEquals(expectedTempFile, result.getTempFilename());
    assertFileContains(expected, expectedTempFile);
  }

  /**
   * Assert that a file contains the lines provided, in the same order as expected.
   */
  private void assertFileContains(List<String> expected, ResourceId file) throws Exception {
    try (BufferedReader reader = new BufferedReader(new FileReader(file.toString()))) {
      List<String> actual = new ArrayList<>();
      for (;;) {
        String line = reader.readLine();
        if (line == null) {
          break;
        }
        actual.add(line);
      }
      assertEquals("contents for " + file, expected, actual);
    }
  }

  /** Write lines to a file. */
  private void writeFile(List<String> lines, File file) throws Exception {
    try (PrintWriter writer = new PrintWriter(new FileOutputStream(file))) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

  /**
   * Removes temporary files when temporary and output directories differ.
   */
  @Test
  public void testRemoveWithTempFilename() throws Exception {
    testRemoveTemporaryFiles(3, getBaseTempDirectory());
  }

  /** Finalize copies temporary files to output files and removes any temporary files. */
  @Test
  public void testFinalize() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    runFinalize(buildWriteOperation(), files);
  }

  /** Finalize can be called repeatedly. */
  @Test
  public void testFinalizeMultipleCalls() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();
    runFinalize(writeOp, files);
    runFinalize(writeOp, files);
  }

  /** Finalize can be called when some temporary files do not exist and output files exist. */
  @Test
  public void testFinalizeWithIntermediateState() throws Exception {
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();
    List<File> files = generateTemporaryFilesForFinalize(3);
    runFinalize(writeOp, files);

    // create a temporary file and then rerun finalize
    tmpFolder.newFolder(tempDirectoryName);
    tmpFolder.newFile(tempDirectoryName + "/1");

    runFinalize(writeOp, files);
  }

  /** Generate n temporary files using the temporary file pattern of Writer. */
  private List<File> generateTemporaryFilesForFinalize(int numFiles) throws Exception {
    List<File> temporaryFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      ResourceId temporaryFile =
          WriteOperation.buildTemporaryFilename(getBaseTempDirectory(), "" + i);
      File tmpFile = new File(tmpFolder.getRoot(), temporaryFile.toString());
      tmpFile.getParentFile().mkdirs();
      assertTrue(tmpFile.createNewFile());
      temporaryFiles.add(tmpFile);
    }

    return temporaryFiles;
  }

  /** Finalize and verify that files are copied and temporary files are optionally removed. */
  private void runFinalize(SimpleSink.SimpleWriteOperation writeOp, List<File> temporaryFiles)
      throws Exception {
    int numFiles = temporaryFiles.size();

    List<FileResult<Void>> fileResults = new ArrayList<>();
    // Create temporary output bundles and output File objects.
    for (int i = 0; i < numFiles; i++) {
      fileResults.add(
          new FileResult<Void>(
              LocalResources.fromFile(temporaryFiles.get(i), false),
              WriteFiles.UNKNOWN_SHARDNUM,
              null,
              null,
              null));
    }

    writeOp.finalize(fileResults);

    for (int i = 0; i < numFiles; i++) {
      ResourceId outputFilename = writeOp.getDynamicDestinations().getFilenamePolicy(null)
          .unwindowedFilename(new Context(i, numFiles), CompressionType.UNCOMPRESSED);
      assertTrue(new File(outputFilename.toString()).exists());
      assertFalse(temporaryFiles.get(i).exists());
    }

    assertFalse(new File(writeOp.tempDirectory.get().toString()).exists());
    // Test that repeated requests of the temp directory return a stable result.
    assertEquals(writeOp.tempDirectory.get(), writeOp.tempDirectory.get());
  }

  /**
   * Create n temporary and output files and verify that removeTemporaryFiles only removes temporary
   * files.
   */
  private void testRemoveTemporaryFiles(int numFiles, ResourceId tempDirectory)
      throws Exception {
    String prefix = "file";
    SimpleSink sink =
        new SimpleSink(getBaseOutputDirectory(), prefix, "", "");

    WriteOperation<String, Void> writeOp =
        new SimpleSink.SimpleWriteOperation(sink, tempDirectory);

    List<File> temporaryFiles = new ArrayList<>();
    List<File> outputFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      ResourceId tempResource =
          WriteOperation.buildTemporaryFilename(tempDirectory, prefix + i);
      File tmpFile = new File(tempResource.toString());
      tmpFile.getParentFile().mkdirs();
      assertTrue("not able to create new temp file", tmpFile.createNewFile());
      temporaryFiles.add(tmpFile);
      ResourceId outputFileId =
          getBaseOutputDirectory().resolve(prefix + i, StandardResolveOptions.RESOLVE_FILE);
      File outputFile = new File(outputFileId.toString());
      outputFile.getParentFile().mkdirs();
      assertTrue("not able to create new output file", outputFile.createNewFile());
      outputFiles.add(outputFile);
    }

    writeOp.removeTemporaryFiles(Collections.<ResourceId>emptySet(), true);

    for (int i = 0; i < numFiles; i++) {
      File temporaryFile = temporaryFiles.get(i);
      assertThat(
          String.format("temp file %s exists", temporaryFile),
          temporaryFile.exists(), is(false));
      File outputFile = outputFiles.get(i);
      assertThat(
          String.format("output file %s exists", outputFile),
          outputFile.exists(), is(true));
    }
  }

  /** Output files are copied to the destination location with the correct names and contents. */
  @Test
  public void testCopyToOutputFiles() throws Exception {
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();
    List<String> inputFilenames = Arrays.asList("input-1", "input-2", "input-3");
    List<String> inputContents = Arrays.asList("1", "2", "3");
    List<String> expectedOutputFilenames = Arrays.asList(
        "file-00-of-03.test", "file-01-of-03.test", "file-02-of-03.test");

    Map<ResourceId, ResourceId> inputFilePaths = new HashMap<>();
    List<ResourceId> expectedOutputPaths = new ArrayList<>();

    for (int i = 0; i < inputFilenames.size(); i++) {
      // Generate output paths.
      expectedOutputPaths.add(
          getBaseOutputDirectory()
              .resolve(expectedOutputFilenames.get(i), StandardResolveOptions.RESOLVE_FILE));

      // Generate and write to input paths.
      File inputTmpFile = tmpFolder.newFile(inputFilenames.get(i));
      List<String> lines = Collections.singletonList(inputContents.get(i));
      writeFile(lines, inputTmpFile);
      inputFilePaths.put(LocalResources.fromFile(inputTmpFile, false),
          writeOp.getDynamicDestinations().getFilenamePolicy(null)
              .unwindowedFilename(new Context(i, inputFilenames.size()),
              CompressionType.UNCOMPRESSED));
    }

    // Copy input files to output files.
    writeOp.copyToOutputFiles(inputFilePaths);

    // Assert that the contents were copied.
    for (int i = 0; i < expectedOutputPaths.size(); i++) {
      assertFileContains(
          Collections.singletonList(inputContents.get(i)), expectedOutputPaths.get(i));
    }
  }

  public List<ResourceId> generateDestinationFilenames(
      ResourceId outputDirectory, FilenamePolicy policy, int numFiles) {
    List<ResourceId> filenames = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      filenames.add(policy.unwindowedFilename(new Context(i, numFiles),
      CompressionType.UNCOMPRESSED));
    }
    return filenames;
  }

  /**
   * Output filenames are generated correctly when an extension is supplied.
   */

  @Test
  public void testGenerateOutputFilenames() {
    List<ResourceId> expected;
    List<ResourceId> actual;
    ResourceId root = getBaseOutputDirectory();

    SimpleSink sink = new SimpleSink(root, "file", ".SSSSS.of.NNNNN", ".test");
    FilenamePolicy policy = sink.getDynamicDestinations().getFilenamePolicy(null);

    expected = Arrays.asList(
        root.resolve("file.00000.of.00003.test", StandardResolveOptions.RESOLVE_FILE),
        root.resolve("file.00001.of.00003.test", StandardResolveOptions.RESOLVE_FILE),
        root.resolve("file.00002.of.00003.test", StandardResolveOptions.RESOLVE_FILE)
    );
    actual = generateDestinationFilenames(root, policy, 3);
    assertEquals(expected, actual);

    expected = Collections.singletonList(
        root.resolve("file.00000.of.00001.test", StandardResolveOptions.RESOLVE_FILE)
    );
    actual = generateDestinationFilenames(root, policy, 1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = generateDestinationFilenames(root, policy, 0);
    assertEquals(expected, actual);
  }

  /** Reject non-distinct output filenames. */
  @Test
  public void testCollidingOutputFilenames() throws IOException {
    ResourceId root = getBaseOutputDirectory();
    SimpleSink sink = new SimpleSink(root, "file", "-NN", "test");
    SimpleSink.SimpleWriteOperation writeOp = new SimpleSink.SimpleWriteOperation(sink);

    ResourceId temp1 = root.resolve("temp1", StandardResolveOptions.RESOLVE_FILE);
    ResourceId temp2 = root.resolve("temp2", StandardResolveOptions.RESOLVE_FILE);
    ResourceId temp3 = root.resolve("temp3", StandardResolveOptions.RESOLVE_FILE);
    ResourceId output = root.resolve("file-03.test", StandardResolveOptions.RESOLVE_FILE);
    // More than one shard does.
    try {
      Iterable<FileResult<Void>> results =
          Lists.newArrayList(
              new FileResult<Void>(temp1, 1, null, null, null),
              new FileResult<Void>(temp2, 1, null, null, null),
              new FileResult<Void>(temp3, 1, null, null, null));
      writeOp.buildOutputFilenames(results);
      fail("Should have failed.");
    } catch (IllegalStateException exn) {
      assertEquals("Only generated 1 distinct file names for 3 files.", exn.getMessage());
    }
  }

  /** Output filenames are generated correctly when an extension is not supplied. */
  @Test
  public void testGenerateOutputFilenamesWithoutExtension() {
    List<ResourceId> expected;
    List<ResourceId> actual;
    ResourceId root = getBaseOutputDirectory();
    SimpleSink sink = new SimpleSink(root, "file", "-SSSSS-of-NNNNN", "");
    FilenamePolicy policy = sink.getDynamicDestinations().getFilenamePolicy(null);

    expected = Arrays.asList(
        root.resolve("file-00000-of-00003", StandardResolveOptions.RESOLVE_FILE),
        root.resolve("file-00001-of-00003", StandardResolveOptions.RESOLVE_FILE),
        root.resolve("file-00002-of-00003", StandardResolveOptions.RESOLVE_FILE)
    );
    actual = generateDestinationFilenames(root, policy, 3);
    assertEquals(expected, actual);

    expected = Collections.singletonList(
        root.resolve("file-00000-of-00001", StandardResolveOptions.RESOLVE_FILE)
    );
    actual = generateDestinationFilenames(root, policy, 1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = generateDestinationFilenames(root, policy, 0);
    assertEquals(expected, actual);
  }

  /** {@link CompressionType#BZIP2} correctly writes BZip2 data. */
  @Test
  public void testCompressionTypeBZIP2() throws FileNotFoundException, IOException {
    final File file =
        writeValuesWithWritableByteChannelFactory(CompressionType.BZIP2, "abc", "123");
    // Read Bzip2ed data back in using Apache commons API (de facto standard).
    assertReadValues(
        new BufferedReader(
            new InputStreamReader(
                new BZip2CompressorInputStream(new FileInputStream(file)),
                StandardCharsets.UTF_8.name())),
        "abc",
        "123");
  }

  /** {@link CompressionType#GZIP} correctly writes Gzipped data. */
  @Test
  public void testCompressionTypeGZIP() throws FileNotFoundException, IOException {
    final File file = writeValuesWithWritableByteChannelFactory(CompressionType.GZIP, "abc", "123");
    // Read Gzipped data back in using standard API.
    assertReadValues(
        new BufferedReader(
            new InputStreamReader(
                new GZIPInputStream(new FileInputStream(file)), StandardCharsets.UTF_8.name())),
        "abc",
        "123");
  }

  /** {@link CompressionType#DEFLATE} correctly writes deflate data. */
  @Test
  public void testCompressionTypeDEFLATE() throws FileNotFoundException, IOException {
    final File file =
        writeValuesWithWritableByteChannelFactory(CompressionType.DEFLATE, "abc", "123");
    // Read Gzipped data back in using standard API.
    assertReadValues(
        new BufferedReader(
            new InputStreamReader(
                new DeflateCompressorInputStream(new FileInputStream(file)),
                StandardCharsets.UTF_8.name())),
        "abc",
        "123");
  }

  /** {@link CompressionType#UNCOMPRESSED} correctly writes uncompressed data. */
  @Test
  public void testCompressionTypeUNCOMPRESSED() throws FileNotFoundException, IOException {
    final File file =
        writeValuesWithWritableByteChannelFactory(CompressionType.UNCOMPRESSED, "abc", "123");
    // Read uncompressed data back in using standard API.
    assertReadValues(
        new BufferedReader(
            new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8.name())),
        "abc",
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
      throws IOException {
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
   * {@link Writer} writes to the {@link WritableByteChannel} provided by {@link
   * DrunkWritableByteChannelFactory}.
   */
  @Test
  public void testFileBasedWriterWithWritableByteChannelFactory() throws Exception {
    final String testUid = "testId";
    ResourceId root = getBaseOutputDirectory();
    WriteOperation<String, Void> writeOp =
        new SimpleSink(root, "file", "-SS-of-NN", "txt", new DrunkWritableByteChannelFactory())
            .createWriteOperation();
    final Writer<String, Void> writer = writeOp.createWriter();
    final ResourceId expectedFile =
        writeOp.tempDirectory.get().resolve(testUid, StandardResolveOptions.RESOLVE_FILE);

    final List<String> expected = new ArrayList<>();
    expected.add("header");
    expected.add("header");
    expected.add("a");
    expected.add("a");
    expected.add("b");
    expected.add("b");
    expected.add("footer");
    expected.add("footer");

    writer.openUnwindowed(testUid, -1, null);
    writer.write("a");
    writer.write("b");
    final FileResult result = writer.close();

    assertEquals(expectedFile, result.getTempFilename());
    assertFileContains(expected, expectedFile);
  }

  /** Build a SimpleSink with default options. */
  private SimpleSink buildSink() {
    return new SimpleSink(getBaseOutputDirectory(), "file", "-SS-of-NN", ".test");
  }

  /**
   * Build a SimpleWriteOperation with default options and the given temporary directory.
   */
  private SimpleSink.SimpleWriteOperation buildWriteOperationWithTempDir(ResourceId tempDirectory) {
    SimpleSink sink = buildSink();
    return new SimpleSink.SimpleWriteOperation(sink, tempDirectory);
  }

  /** Build a write operation with the default options for it and its parent sink. */
  private SimpleSink.SimpleWriteOperation buildWriteOperation() {
    return buildSink().createWriteOperation();
  }
}
