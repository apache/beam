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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.io.FileBasedSink.FileBasedWriteOperation;
import com.google.cloud.dataflow.sdk.io.FileBasedSink.FileBasedWriteOperation.TemporaryFileRetention;
import com.google.cloud.dataflow.sdk.io.FileBasedSink.FileResult;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for FileBasedSink.
 */
@RunWith(JUnit4.class)
public class FileBasedSinkTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private String baseOutputFilename = "output";
  private String baseTemporaryFilename = "temp";

  private String appendToTempFolder(String filename) {
    return Paths.get(tmpFolder.getRoot().getPath(), filename).toString();
  }

  private String getBaseOutputFilename() {
    return appendToTempFolder(baseOutputFilename);
  }

  private String getBaseTempFilename() {
    return appendToTempFolder(baseTemporaryFilename);
  }

  /**
   * FileBasedWriter opens the correct file, writes the header, footer, and elements in the
   * correct order, and returns the correct filename.
   */
  @Test
  public void testWriter() throws Exception {
    String testUid = "testId";
    String expectedFilename =
        getBaseTempFilename() + FileBasedWriteOperation.TEMPORARY_FILENAME_SEPARATOR + testUid;
    SimpleSink.SimpleWriter writer = buildWriter();

    List<String> values = Arrays.asList("sympathetic vulture", "boresome hummingbird");
    List<String> expected = new ArrayList<>();
    expected.add(SimpleSink.SimpleWriter.HEADER);
    expected.addAll(values);
    expected.add(SimpleSink.SimpleWriter.FOOTER);

    writer.open(testUid);
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
    testRemoveTemporaryFiles(3, baseTemporaryFilename);
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
  public void testFinalizeWithNoRetention() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    boolean retainTemporaryFiles = false;
    runFinalize(buildWriteOperationForFinalize(retainTemporaryFiles), files, retainTemporaryFiles);
  }

  /**
   * Finalize retains temporary files when requested.
   */
  @Test
  public void testFinalizeWithRetention() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    boolean retainTemporaryFiles = true;
    runFinalize(buildWriteOperationForFinalize(retainTemporaryFiles), files, retainTemporaryFiles);
  }

  /**
   * Finalize can be called repeatedly.
   */
  @Test
  public void testFinalizeMultipleCalls() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperationForFinalize(false);
    runFinalize(writeOp, files, false);
    runFinalize(writeOp, files, false);
  }

  /**
   * Finalize can be called when some temporary files do not exist and output files exist.
   */
  @Test
  public void testFinalizeWithIntermediateState() throws Exception {
    List<File> files = generateTemporaryFilesForFinalize(3);
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperationForFinalize(false);
    runFinalize(writeOp, files, false);

    // create a temporary file
    tmpFolder.newFile(
        baseTemporaryFilename + FileBasedWriteOperation.TEMPORARY_FILENAME_SEPARATOR + "1");

    runFinalize(writeOp, files, false);
  }

  /**
   * Build a SimpleWriteOperation with default values and the specified retention policy.
   */
  private SimpleSink.SimpleWriteOperation buildWriteOperationForFinalize(
      boolean retainTemporaryFiles) throws Exception {
    TemporaryFileRetention retentionPolicy =
        retainTemporaryFiles ? TemporaryFileRetention.KEEP : TemporaryFileRetention.REMOVE;
    return buildWriteOperation(retentionPolicy);
  }

  /**
   * Generate n temporary files using the temporary file pattern of FileBasedWriter.
   */
  private List<File> generateTemporaryFilesForFinalize(int numFiles) throws Exception {
    List<File> temporaryFiles = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      String temporaryFilename =
          FileBasedWriteOperation.buildTemporaryFilename(baseTemporaryFilename, "" + i);
      File tmpFile = tmpFolder.newFile(temporaryFilename);
      temporaryFiles.add(tmpFile);
    }

    return temporaryFiles;
  }

  /**
   * Finalize and verify that files are copied and temporary files are optionally removed.
   */
  private void runFinalize(SimpleSink.SimpleWriteOperation writeOp, List<File> temporaryFiles,
      boolean retainTemporaryFiles) throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();

    int numFiles = temporaryFiles.size();

    List<File> outputFiles = new ArrayList<>();
    List<FileResult> fileResults = new ArrayList<>();
    List<String> outputFilenames = writeOp.generateDestinationFilenames(numFiles);

    // Create temporary output bundles and output File objects
    for (int i = 0; i < numFiles; i++) {
      fileResults.add(new FileResult(temporaryFiles.get(i).toString()));
      outputFiles.add(new File(outputFilenames.get(i)));
    }

    writeOp.finalize(fileResults, options);

    for (int i = 0; i < numFiles; i++) {
      assertTrue(outputFiles.get(i).exists());
      assertEquals(retainTemporaryFiles, temporaryFiles.get(i).exists());
    }
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
      File tmpFile = tmpFolder.newFile(
          FileBasedWriteOperation.buildTemporaryFilename(baseTemporaryFilename, "" + i));
      temporaryFiles.add(tmpFile);
      File outputFile = tmpFolder.newFile(baseOutputFilename + i);
      outputFiles.add(outputFile);
    }

    writeOp.removeTemporaryFiles(options);

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

    List<String> inputFilenames = Arrays.asList("input-3", "input-2", "input-1");
    List<String> inputContents = Arrays.asList("3", "2", "1");
    List<String> expectedOutputFilenames = Arrays.asList(
        "output-00002-of-00003.test", "output-00001-of-00003.test", "output-00000-of-00003.test");

    List<String> inputFilePaths = new ArrayList<>();
    List<String> expectedOutputPaths = new ArrayList<>();

    for (int i = 0; i < inputFilenames.size(); i++) {
      // Generate output paths.
      File outputFile = tmpFolder.newFile(expectedOutputFilenames.get(i));
      expectedOutputPaths.add(outputFile.toString());

      // Generate and write to input paths.
      File inputTmpFile = tmpFolder.newFile(inputFilenames.get(i));
      List<String> lines = Arrays.asList(inputContents.get(i));
      writeFile(lines, inputTmpFile);
      inputFilePaths.add(inputTmpFile.toString());
    }

    // Copy input files to output files.
    List<String> actual = writeOp.copyToOutputFiles(inputFilePaths, options);

    // Assert that the expected paths are returned.
    assertThat(expectedOutputPaths, containsInAnyOrder(actual.toArray()));

    // Assert that the contents were copied.
    for (int i = 0; i < expectedOutputPaths.size(); i++) {
      assertFileContains(Arrays.asList(inputContents.get(i)), expectedOutputPaths.get(i));
    }
  }

  /**
   * Output filenames use the supplied naming template.
   */
  @Test
  public void testGenerateOutputFilenamesWithTemplate() {
    List<String> expected;
    List<String> actual;
    SimpleSink sink = new SimpleSink(getBaseOutputFilename(), "test", ".SS.of.NN");
    SimpleSink.SimpleWriteOperation writeOp = new SimpleSink.SimpleWriteOperation(sink);

    expected = Arrays.asList(appendToTempFolder("output.00.of.03.test"),
        appendToTempFolder("output.01.of.03.test"), appendToTempFolder("output.02.of.03.test"));
    actual = writeOp.generateDestinationFilenames(3);
    assertEquals(expected, actual);

    expected = Arrays.asList(appendToTempFolder("output.00.of.01.test"));
    actual = writeOp.generateDestinationFilenames(1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = writeOp.generateDestinationFilenames(0);
    assertEquals(expected, actual);

    // Also validate that we handle the case where the user specified "." that we do
    // not prefix an additional "." making "..test"
    sink = new SimpleSink(getBaseOutputFilename(), ".test", ".SS.of.NN");
    writeOp = new SimpleSink.SimpleWriteOperation(sink);
    expected = Arrays.asList(appendToTempFolder("output.00.of.03.test"),
        appendToTempFolder("output.01.of.03.test"), appendToTempFolder("output.02.of.03.test"));
    actual = writeOp.generateDestinationFilenames(3);
    assertEquals(expected, actual);

    expected = Arrays.asList(appendToTempFolder("output.00.of.01.test"));
    actual = writeOp.generateDestinationFilenames(1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = writeOp.generateDestinationFilenames(0);
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

    expected = Arrays.asList(
        appendToTempFolder("output-00000-of-00003.test"),
        appendToTempFolder("output-00001-of-00003.test"),
        appendToTempFolder("output-00002-of-00003.test"));
    actual = writeOp.generateDestinationFilenames(3);
    assertEquals(expected, actual);

    expected = Arrays.asList(appendToTempFolder("output-00000-of-00001.test"));
    actual = writeOp.generateDestinationFilenames(1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = writeOp.generateDestinationFilenames(0);
    assertEquals(expected, actual);
  }

  /**
   * Output filenames are generated correctly when an extension is not supplied.
   */
  @Test
  public void testGenerateOutputFilenamesWithoutExtension() {
    List<String> expected;
    List<String> actual;
    SimpleSink sink = new SimpleSink(appendToTempFolder(baseOutputFilename), "");
    SimpleSink.SimpleWriteOperation writeOp = new SimpleSink.SimpleWriteOperation(sink);

    expected = Arrays.asList(appendToTempFolder("output-00000-of-00003"),
        appendToTempFolder("output-00001-of-00003"), appendToTempFolder("output-00002-of-00003"));
    actual = writeOp.generateDestinationFilenames(3);
    assertEquals(expected, actual);

    expected = Arrays.asList(appendToTempFolder("output-00000-of-00001"));
    actual = writeOp.generateDestinationFilenames(1);
    assertEquals(expected, actual);

    expected = new ArrayList<>();
    actual = writeOp.generateDestinationFilenames(0);
    assertEquals(expected, actual);
  }

  /**
   * A simple FileBasedSink that writes String values as lines with header and footer lines.
   */
  private static final class SimpleSink extends FileBasedSink<String> {
    public SimpleSink(String baseOutputFilename, String extension) {
      super(baseOutputFilename, extension);
    }

    public SimpleSink(String baseOutputFilename, String extension, String fileNamingTemplate) {
      super(baseOutputFilename, extension, fileNamingTemplate);
    }

    @Override
    public SimpleWriteOperation createWriteOperation(PipelineOptions options) {
      return new SimpleWriteOperation(this);
    }

    private static final class SimpleWriteOperation extends FileBasedWriteOperation<String> {
      public SimpleWriteOperation(
          SimpleSink sink, String tempOutputFilename, TemporaryFileRetention retentionPolicy) {
        super(sink, tempOutputFilename, retentionPolicy);
      }

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
   * Build a SimpleWriteOperation with default options and the given file retention policy.
   */
  private SimpleSink.SimpleWriteOperation buildWriteOperation(
      TemporaryFileRetention fileRetention) {
    SimpleSink sink = buildSink();
    return new SimpleSink.SimpleWriteOperation(sink, getBaseTempFilename(), fileRetention);
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
    return new SimpleSink.SimpleWriteOperation(
        sink, getBaseTempFilename(), TemporaryFileRetention.REMOVE);
  }

  /**
   * Build a writer with the default options for its parent write operation and sink.
   */
  private SimpleSink.SimpleWriter buildWriter() {
    SimpleSink.SimpleWriteOperation writeOp = buildWriteOperation(TemporaryFileRetention.REMOVE);
    return new SimpleSink.SimpleWriter(writeOp);
  }
}
