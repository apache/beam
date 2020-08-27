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

import static org.apache.beam.sdk.io.WriteFiles.UNKNOWN_SHARDNUM;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets.UTF_8;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeFalse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import org.apache.beam.sdk.io.FileBasedSink.CompressionType;
import org.apache.beam.sdk.io.FileBasedSink.FileResult;
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy;
import org.apache.beam.sdk.io.FileBasedSink.WriteOperation;
import org.apache.beam.sdk.io.FileBasedSink.Writer;
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.deflate.DeflateCompressorInputStream;
import org.apache.commons.lang3.SystemUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FileBasedSink}. */
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
   * Writer opens the correct file, writes the header, footer, and elements in the correct order,
   * and returns the correct filename.
   */
  @Test
  public void testWriter() throws Exception {
    String testUid = "testId";
    ResourceId expectedTempFile =
        getBaseTempDirectory().resolve(testUid, StandardResolveOptions.RESOLVE_FILE);
    List<String> values = Arrays.asList("sympathetic vulture", "boresome hummingbird");
    List<String> expected = new ArrayList<>();
    expected.add(SimpleSink.SimpleWriter.HEADER);
    expected.addAll(values);
    expected.add(SimpleSink.SimpleWriter.FOOTER);

    SimpleSink.SimpleWriter<Void> writer =
        buildWriteOperationWithTempDir(getBaseTempDirectory()).createWriter();
    writer.open(testUid);
    for (String value : values) {
      writer.write(value);
    }
    writer.close();
    assertEquals(expectedTempFile, writer.getOutputFile());
    assertFileContains(expected, expectedTempFile);
  }

  /** Assert that a file contains the lines provided, in the same order as expected. */
  private void assertFileContains(List<String> expected, ResourceId file) throws Exception {
    try (BufferedReader reader = Files.newBufferedReader(Paths.get(file.toString()), UTF_8)) {
      List<String> actual = new ArrayList<>();
      for (; ; ) {
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
    try (PrintWriter writer =
        new PrintWriter(
            new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file), UTF_8)))) {
      for (String line : lines) {
        writer.println(line);
      }
    }
  }

  /** Test whether WriteOperation can create a unique temporary directory. */
  @Test
  public void testTemporaryDirectoryUniqueness() {
    List<SimpleSink.SimpleWriteOperation<Void>> writeOps = Lists.newArrayListWithCapacity(1000);
    for (int i = 0; i < 1000; i++) {
      writeOps.add(buildWriteOperation());
    }
    Set<String> tempDirectorySet = Sets.newHashSetWithExpectedSize(1000);
    for (SimpleSink.SimpleWriteOperation<Void> op : writeOps) {
      tempDirectorySet.add(op.getTempDirectory().toString());
    }
    assertEquals(1000, tempDirectorySet.size());
  }

  /** Removes temporary files when temporary and output directories differ. */
  @Test
  public void testRemoveWithTempFilename() throws Exception {
    testRemoveTemporaryFiles(3, getBaseTempDirectory());
  }

  /** Finalize copies temporary files to output files and removes any temporary files. */
  @Test
  public void testFinalize() throws Exception {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10743
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    List<File> files = generateTemporaryFilesForFinalize(3);
    runFinalize(buildWriteOperation(), files);
  }

  /** Finalize can be called repeatedly. */
  @Test
  public void testFinalizeMultipleCalls() throws Exception {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10744
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
    List<File> files = generateTemporaryFilesForFinalize(3);
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation();
    runFinalize(writeOp, files);
    runFinalize(writeOp, files);
  }

  /** Finalize can be called when some temporary files do not exist and output files exist. */
  @Test
  public void testFinalizeWithIntermediateState() throws Exception {
    // TODO: Java core test failing on windows, https://issues.apache.org/jira/browse/BEAM-10745
    assumeFalse(SystemUtils.IS_OS_WINDOWS);
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
  private void runFinalize(SimpleSink.SimpleWriteOperation<Void> writeOp, List<File> temporaryFiles)
      throws Exception {
    int numFiles = temporaryFiles.size();

    List<FileResult<Void>> fileResults = new ArrayList<>();
    // Create temporary output bundles and output File objects.
    for (File temporaryFile : temporaryFiles) {
      fileResults.add(
          new FileResult<>(
              LocalResources.fromFile(temporaryFile, false),
              UNKNOWN_SHARDNUM,
              GlobalWindow.INSTANCE,
              PaneInfo.ON_TIME_AND_ONLY_FIRING,
              null));
    }

    // TODO: test with null first argument?
    List<KV<FileResult<Void>, ResourceId>> resultsToFinalFilenames =
        writeOp.finalizeDestination(null, GlobalWindow.INSTANCE, null, fileResults);
    writeOp.moveToOutputFiles(resultsToFinalFilenames);

    for (int i = 0; i < numFiles; i++) {
      ResourceId outputFilename =
          writeOp
              .getSink()
              .getDynamicDestinations()
              .getFilenamePolicy(null)
              .unwindowedFilename(i, numFiles, CompressionType.UNCOMPRESSED);
      assertTrue(outputFilename.toString(), new File(outputFilename.toString()).exists());
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
  private void testRemoveTemporaryFiles(int numFiles, ResourceId tempDirectory) throws Exception {
    String prefix = "file";
    SimpleSink<Void> sink =
        SimpleSink.makeSimpleSink(
            getBaseOutputDirectory(), prefix, "", "", Compression.UNCOMPRESSED);

    WriteOperation<Void, String> writeOp =
        new SimpleSink.SimpleWriteOperation<>(sink, tempDirectory);

    List<File> temporaryFiles = new ArrayList<>();
    List<File> outputFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      ResourceId tempResource = WriteOperation.buildTemporaryFilename(tempDirectory, prefix + i);
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

    writeOp.removeTemporaryFiles(Collections.emptySet(), true);

    for (int i = 0; i < numFiles; i++) {
      File temporaryFile = temporaryFiles.get(i);
      assertThat(
          String.format("temp file %s exists", temporaryFile), temporaryFile.exists(), is(false));
      File outputFile = outputFiles.get(i);
      assertThat(String.format("output file %s exists", outputFile), outputFile.exists(), is(true));
    }
  }

  /** Output files are copied to the destination location with the correct names and contents. */
  @Test
  public void testCopyToOutputFiles() throws Exception {
    SimpleSink.SimpleWriteOperation<Void> writeOp = buildWriteOperation();
    List<String> inputFilenames = Arrays.asList("input-1", "input-2", "input-3");
    List<String> inputContents = Arrays.asList("1", "2", "3");
    List<String> expectedOutputFilenames =
        Arrays.asList("file-00-of-03.test", "file-01-of-03.test", "file-02-of-03.test");

    List<KV<FileResult<Void>, ResourceId>> resultsToFinalFilenames = Lists.newArrayList();
    List<ResourceId> expectedOutputPaths = Lists.newArrayList();

    for (int i = 0; i < inputFilenames.size(); i++) {
      // Generate output paths.
      expectedOutputPaths.add(
          getBaseOutputDirectory()
              .resolve(expectedOutputFilenames.get(i), StandardResolveOptions.RESOLVE_FILE));

      // Generate and write to input paths.
      File inputTmpFile = tmpFolder.newFile(inputFilenames.get(i));
      List<String> lines = Collections.singletonList(inputContents.get(i));
      writeFile(lines, inputTmpFile);
      ResourceId finalFilename =
          writeOp
              .getSink()
              .getDynamicDestinations()
              .getFilenamePolicy(null)
              .unwindowedFilename(i, inputFilenames.size(), CompressionType.UNCOMPRESSED);
      resultsToFinalFilenames.add(
          KV.of(
              new FileResult<>(
                  LocalResources.fromFile(inputTmpFile, false),
                  UNKNOWN_SHARDNUM,
                  GlobalWindow.INSTANCE,
                  PaneInfo.ON_TIME_AND_ONLY_FIRING,
                  null),
              finalFilename));
    }

    // Copy input files to output files.
    writeOp.moveToOutputFiles(resultsToFinalFilenames);

    // Assert that the contents were copied.
    for (int i = 0; i < expectedOutputPaths.size(); i++) {
      assertFileContains(
          Collections.singletonList(inputContents.get(i)), expectedOutputPaths.get(i));
    }
  }

  public List<ResourceId> generateDestinationFilenames(FilenamePolicy policy, int numFiles) {
    List<ResourceId> filenames = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      filenames.add(policy.unwindowedFilename(i, numFiles, CompressionType.UNCOMPRESSED));
    }
    return filenames;
  }

  /** Output filenames are generated correctly when an extension is supplied. */
  @Test
  public void testGenerateOutputFilenames() {
    List<ResourceId> expected;
    List<ResourceId> actual;
    ResourceId root = getBaseOutputDirectory();

    SimpleSink<Void> sink =
        SimpleSink.makeSimpleSink(
            root, "file", ".SSSSS.of.NNNNN", ".test", Compression.UNCOMPRESSED);
    FilenamePolicy policy = sink.getDynamicDestinations().getFilenamePolicy(null);

    expected =
        Arrays.asList(
            root.resolve("file.00000.of.00003.test", StandardResolveOptions.RESOLVE_FILE),
            root.resolve("file.00001.of.00003.test", StandardResolveOptions.RESOLVE_FILE),
            root.resolve("file.00002.of.00003.test", StandardResolveOptions.RESOLVE_FILE));
    actual = generateDestinationFilenames(policy, 3);
    assertEquals(expected, actual);

    expected =
        Collections.singletonList(
            root.resolve("file.00000.of.00001.test", StandardResolveOptions.RESOLVE_FILE));
    actual = generateDestinationFilenames(policy, 1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = generateDestinationFilenames(policy, 0);
    assertEquals(expected, actual);
  }

  /** Reject non-distinct output filenames. */
  @Test
  public void testCollidingOutputFilenames() throws Exception {
    ResourceId root = getBaseOutputDirectory();
    SimpleSink<Void> sink =
        SimpleSink.makeSimpleSink(root, "file", "-NN", "test", Compression.UNCOMPRESSED);
    SimpleSink.SimpleWriteOperation<Void> writeOp = new SimpleSink.SimpleWriteOperation<>(sink);

    try {
      List<FileResult<Void>> results = Lists.newArrayList();
      for (int i = 0; i < 3; ++i) {
        results.add(
            new FileResult<>(
                root.resolve("temp" + i, StandardResolveOptions.RESOLVE_FILE),
                1 /* shard - should be different, but is the same */,
                GlobalWindow.INSTANCE,
                PaneInfo.ON_TIME_AND_ONLY_FIRING,
                null));
      }
      writeOp.finalizeDestination(null, GlobalWindow.INSTANCE, 5 /* numShards */, results);
      fail("Should have failed.");
    } catch (IllegalArgumentException exn) {
      assertThat(exn.getMessage(), containsString("generated the same name"));
      assertThat(exn.getMessage(), containsString("temp0"));
      assertThat(exn.getMessage(), containsString("temp1"));
    }
  }

  /** Output filenames are generated correctly when an extension is not supplied. */
  @Test
  public void testGenerateOutputFilenamesWithoutExtension() {
    List<ResourceId> expected;
    List<ResourceId> actual;
    ResourceId root = getBaseOutputDirectory();
    SimpleSink<Void> sink =
        SimpleSink.makeSimpleSink(root, "file", "-SSSSS-of-NNNNN", "", Compression.UNCOMPRESSED);
    FilenamePolicy policy = sink.getDynamicDestinations().getFilenamePolicy(null);

    expected =
        Arrays.asList(
            root.resolve("file-00000-of-00003", StandardResolveOptions.RESOLVE_FILE),
            root.resolve("file-00001-of-00003", StandardResolveOptions.RESOLVE_FILE),
            root.resolve("file-00002-of-00003", StandardResolveOptions.RESOLVE_FILE));
    actual = generateDestinationFilenames(policy, 3);
    assertEquals(expected, actual);

    expected =
        Collections.singletonList(
            root.resolve("file-00000-of-00001", StandardResolveOptions.RESOLVE_FILE));
    actual = generateDestinationFilenames(policy, 1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = generateDestinationFilenames(policy, 0);
    assertEquals(expected, actual);
  }

  /** {@link Compression#BZIP2} correctly writes BZip2 data. */
  @Test
  public void testCompressionBZIP2() throws FileNotFoundException, IOException {
    final File file = writeValuesWithCompression(Compression.BZIP2, "abc", "123");
    // Read Bzip2ed data back in using Apache commons API (de facto standard).
    assertReadValues(
        new BufferedReader(
            new InputStreamReader(
                new BZip2CompressorInputStream(new FileInputStream(file)), StandardCharsets.UTF_8)),
        "abc",
        "123");
  }

  /** {@link Compression#GZIP} correctly writes Gzipped data. */
  @Test
  public void testCompressionGZIP() throws FileNotFoundException, IOException {
    final File file = writeValuesWithCompression(Compression.GZIP, "abc", "123");
    // Read Gzipped data back in using standard API.
    assertReadValues(
        new BufferedReader(
            new InputStreamReader(
                new GZIPInputStream(new FileInputStream(file)), StandardCharsets.UTF_8)),
        "abc",
        "123");
  }

  /** {@link Compression#DEFLATE} correctly writes deflate data. */
  @Test
  public void testCompressionDEFLATE() throws FileNotFoundException, IOException {
    final File file = writeValuesWithCompression(Compression.DEFLATE, "abc", "123");
    // Read Gzipped data back in using standard API.
    assertReadValues(
        new BufferedReader(
            new InputStreamReader(
                new DeflateCompressorInputStream(new FileInputStream(file)),
                StandardCharsets.UTF_8)),
        "abc",
        "123");
  }

  /** {@link Compression#UNCOMPRESSED} correctly writes uncompressed data. */
  @Test
  public void testCompressionUNCOMPRESSED() throws FileNotFoundException, IOException {
    final File file = writeValuesWithCompression(Compression.UNCOMPRESSED, "abc", "123");
    // Read uncompressed data back in using standard API.
    assertReadValues(
        new BufferedReader(
            new InputStreamReader(new FileInputStream(file), StandardCharsets.UTF_8)),
        "abc",
        "123");
  }

  private void assertReadValues(final BufferedReader br, String... values) throws IOException {
    try (final BufferedReader lbr = br) {
      for (String value : values) {
        assertEquals(String.format("Line should read '%s'", value), value, lbr.readLine());
      }
    }
  }

  private File writeValuesWithCompression(Compression compression, String... values)
      throws IOException {
    final File file = tmpFolder.newFile("test.gz");
    final WritableByteChannel channel =
        compression.writeCompressed(Channels.newChannel(new FileOutputStream(file)));
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
    WriteOperation<Void, String> writeOp =
        SimpleSink.makeSimpleSink(
                root, "file", "-SS-of-NN", "txt", new DrunkWritableByteChannelFactory())
            .createWriteOperation();
    final Writer<Void, String> writer = writeOp.createWriter();
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

    writer.open(testUid);
    writer.write("a");
    writer.write("b");
    writer.close();
    assertEquals(expectedFile, writer.getOutputFile());
    assertFileContains(expected, expectedFile);
  }

  /** Build a SimpleSink with default options. */
  private SimpleSink<Void> buildSink() {
    return SimpleSink.makeSimpleSink(
        getBaseOutputDirectory(), "file", "-SS-of-NN", ".test", Compression.UNCOMPRESSED);
  }

  /** Build a SimpleWriteOperation with default options and the given temporary directory. */
  private SimpleSink.SimpleWriteOperation<Void> buildWriteOperationWithTempDir(
      ResourceId tempDirectory) {
    SimpleSink<Void> sink = buildSink();
    return new SimpleSink.SimpleWriteOperation<>(sink, tempDirectory);
  }

  /** Build a write operation with the default options for it and its parent sink. */
  private SimpleSink.SimpleWriteOperation<Void> buildWriteOperation() {
    return buildSink().createWriteOperation();
  }
}
