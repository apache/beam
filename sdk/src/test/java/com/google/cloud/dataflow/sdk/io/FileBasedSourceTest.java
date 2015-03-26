/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.io.FileBasedSource.FileBasedReader;
import com.google.cloud.dataflow.sdk.io.FileBasedSource.Mode;
import com.google.cloud.dataflow.sdk.io.Source.Reader;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.runners.DirectPipeline;
import com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner.EvaluationResults;
import com.google.cloud.dataflow.sdk.testing.TestDataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.IOChannelFactory;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.TestCredential;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.ImmutableList;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Tests code common to all file-based sources.
 */
@RunWith(JUnit4.class)
public class FileBasedSourceTest {

  Random random = new Random(System.currentTimeMillis());

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  /**
   * If {@code splitHeader} is null, this is just a simple line-based reader. Otherwise, the file is
   * considered to consist of blocks beginning with {@code splitHeader}. The header itself is not
   * returned as a record. The first record after the header is considered to be a split point.
   *
   * <p>E.g., if {@code splitHeader} is "h" and the lines of the file are: h, a, b, h, h, c, then
   * the records in this source are a,b,c, and records a and c are split points.
   */
  class TestFileBasedSource extends FileBasedSource<String> {

    private static final long serialVersionUID = 85539251;

    ReadableByteChannel channel = null;
    final String splitHeader;

    public TestFileBasedSource(boolean isFilePattern, String fileOrPattern, long minBundleSize,
        String splitHeader) {
      super(isFilePattern, fileOrPattern, minBundleSize, 0L, Long.MAX_VALUE);
      this.splitHeader = splitHeader;
    }

    public TestFileBasedSource(String fileOrPattern, long minBundleSize, long startOffset,
        long endOffset, String splitHeader) {
      super(false, fileOrPattern, minBundleSize, startOffset, endOffset);
      this.splitHeader = splitHeader;
    }

    @Override
    public boolean producesSortedKeys(PipelineOptions options) throws Exception {
      return false;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    public FileBasedSource<String> createForSubrangeOfFile(String fileName, long start, long end) {
      return new TestFileBasedSource(fileName, getMinBundleSize(), start, end, splitHeader);
    }

    @Override
    public FileBasedReader<String> createSingleFileReader(PipelineOptions options,
                                                          ExecutionContext executionContext) {
      if (splitHeader == null) {
        return new TestReader(this);
      } else {
        return new TestReaderWithSplits(this);
      }
    }
  }

  /**
   * A reader that can read lines of text from a {@link TestFileBasedSource}. This reader does not
   * consider {@code splitHeader} defined by {@code TestFileBasedSource} hence every line can be the
   * first line of a split.
   */
  class TestReader extends FileBasedReader<String> {
    private ReadableByteChannel channel = null;
    private final byte boundary;
    private long nextOffset = 0;
    private long currentOffset = 0;
    private boolean isAtSplitPoint = false;
    private final ByteBuffer buf;
    private static final int BUF_SIZE = 1024;
    private String currentValue = null;
    private boolean emptyBundle = false;

    public TestReader(TestFileBasedSource source) {
      super(source);
      boundary = '\n';
      buf = ByteBuffer.allocate(BUF_SIZE);
      buf.flip();
    }

    private int readNextLine(ByteArrayOutputStream out) throws IOException {
      int byteCount = 0;
      while (true) {
        if (!buf.hasRemaining()) {
          buf.clear();
          int read = channel.read(buf);
          if (read < 0) {
            break;
          }
          buf.flip();
        }
        byte b = buf.get();
        byteCount++;
        if (b == boundary) {
          break;
        }
        out.write(b);
      }
      return byteCount;
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      boolean removeLine = false;
      if (getSource().getMode() == Mode.SUBRANGE_OF_SINGLE_FILE) {
        SeekableByteChannel seekChannel = (SeekableByteChannel) channel;
        // If we are not at the beginning of a line, we should ignore the current line.
        if (seekChannel.position() > 0) {
          // Start from one character back and read till we find a new line.
          seekChannel.position(seekChannel.position() - 1);
          removeLine = true;
        }
        nextOffset = seekChannel.position();
      }
      this.channel = channel;
      if (removeLine) {
        nextOffset += readNextLine(new ByteArrayOutputStream());
      }
      if (nextOffset >= getSource().getEndOffset()) {
        emptyBundle = true;
      }
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      if (emptyBundle) {
        return false;
      }

      currentOffset = nextOffset;

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      int offsetAdjustment = readNextLine(buf);
      if (offsetAdjustment == 0) {
        // EOF
        return false;
      }
      nextOffset += offsetAdjustment;
      isAtSplitPoint = true;
      // When running on Windows, each line obtained from 'readNextLine()' will end with a '\r'
      // since we use '\n' as the line boundary of the reader. So we trim it off here.
      currentValue = CoderUtils.decodeFromByteArray(StringUtf8Coder.of(), buf.toByteArray()).trim();
      return true;
    }

    @Override
    protected boolean isAtSplitPoint() {
      return isAtSplitPoint;
    }

    @Override
    protected long getCurrentOffset() {
      return currentOffset;
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      return currentValue;
    }
  }

  /**
   * A reader that can read lines of text from a {@link TestFileBasedSource}. This reader considers
   * {@code splitHeader} defined by {@code TestFileBasedSource} hence only lines that immediately
   * follow a {@code splitHeader} are split points.
   */
  class TestReaderWithSplits extends TestReader {
    private final String splitHeader;
    private boolean isAtSplitPoint = false;
    private long currentOffset;
    private boolean emptyBundle = false;

    public TestReaderWithSplits(TestFileBasedSource source) {
      super(source);
      this.splitHeader = source.splitHeader;
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      super.startReading(channel);

      // Ignore all lines until next header.
      if (!super.readNextRecord()) {
        return;
      }
      String current = super.getCurrent();

      currentOffset = super.getCurrentOffset();
      while (current == null || !current.equals(splitHeader)) {
        // Offset of a split point should be the offset of the header. Hence marking current
        // offset before reading the record.
        currentOffset = super.getCurrentOffset();
        if (!super.readNextRecord()) {
          return;
        }
        current = super.getCurrent();
      }
      if (currentOffset >= getSource().getEndOffset()) {
        emptyBundle = true;
      }
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      // Get next record. If next record is a header read up to the next non-header record (ignoring
      // any empty splits that does not have any records).

      if (emptyBundle) {
        return false;
      }

      isAtSplitPoint = false;
      while (true) {
        long previousOffset = super.getCurrentOffset();
        if (!super.readNextRecord()) {
          return false;
        }
        String current = super.getCurrent();
        if (current == null || !current.equals(splitHeader)) {
          if (isAtSplitPoint) {
            // Offset of a split point should be the offset of the header.
            currentOffset = previousOffset;
          } else {
            currentOffset = super.getCurrentOffset();
          }
          return true;
        }
        isAtSplitPoint = true;
      }
    }

    @Override
    protected boolean isAtSplitPoint() {
      return isAtSplitPoint;
    }

    @Override
    protected long getCurrentOffset() {
      return currentOffset;
    }
  }

  public File createFileWithData(String fileName, List<String> data) throws IOException {
    File file = tempFolder.newFile(fileName);
    Files.write(file.toPath(), data, StandardCharsets.UTF_8);
    return file;
  }

  private String createRandomString(int length) {
    char[] chars = "abcdefghijklmnopqrstuvwxyz".toCharArray();
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < length; i++) {
      builder.append(chars[random.nextInt(chars.length)]);
    }
    return builder.toString();
  }

  public List<String> createStringDataset(int dataItemLength, int numItems) {
    List<String> list = new ArrayList<String>();
    for (int i = 0; i < numItems; i++) {
      list.add(createRandomString(dataItemLength));
    }
    return list;
  }

  private List<String> readEverythingFromReader(Reader<String> reader) throws IOException {
    List<String> results = new ArrayList<String>();
    for (boolean available = reader.start(); available; available = reader.advance()) {
      results.add(reader.getCurrent());
    }
    return results;
  }

  @Test
  public void testFullyReadSingleFile() throws IOException {
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(false, file.getPath(), 64, null);
    assertThat(data, containsInAnyOrder(
        readEverythingFromReader(source.createReader(null, null)).toArray()));
  }

  @Test
  public void testFullyReadFilePattern() throws IOException {
    List<String> data1 = createStringDataset(3, 50);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source =
        new TestFileBasedSource(true, new File(file1.getParent(), "file*").getPath(), 64, null);
    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data1);
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);
    assertThat(expectedResults, containsInAnyOrder(
        readEverythingFromReader(source.createReader(null, null)).toArray()));
  }

  @Test
  public void testFullyReadFilePatternFirstRecordEmpty() throws IOException {
    File file1 = createFileWithData("file1", new ArrayList<String>());

    IOChannelFactory mockIOFactory = Mockito.mock(IOChannelFactory.class);
    String parent = file1.getParent();
    String pattern = "mocked://test";
    when(mockIOFactory.match(pattern)).thenReturn(ImmutableList.of(
        new File(parent, "file1").getPath(), new File(parent, "file2").getPath(),
        new File(parent, "file3").getPath()));
    IOChannelUtils.setIOFactory("mocked", mockIOFactory);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source = new TestFileBasedSource(true, pattern, 64, null);

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);
    assertThat(expectedResults, containsInAnyOrder(
        readEverythingFromReader(source.createReader(null, null)).toArray()));
  }

  @Test
  public void testReadRangeAtStart() throws IOException {
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 25, null);
    TestFileBasedSource source2 =
        new TestFileBasedSource(file.getPath(), 64, 25, Long.MAX_VALUE, null);

    List<String> results = new ArrayList<String>();
    results.addAll(readEverythingFromReader(source1.createReader(null, null)));
    results.addAll(readEverythingFromReader(source2.createReader(null, null)));
    assertThat(data, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadEverythingFromFileWithSplits() throws IOException {
    String header = "<h>";
    List<String> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      data.add(header);
      data.addAll(createStringDataset(3, 9));
    }
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source =
        new TestFileBasedSource(file.getPath(), 64, 0, Long.MAX_VALUE, header);

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data);
    // Remove all occurrences of header from expected results.
    expectedResults.removeAll(Arrays.asList(header));

    assertThat(expectedResults, containsInAnyOrder(
        readEverythingFromReader(source.createReader(null, null)).toArray()));
  }

  @Test
  public void testReadRangeFromFileWithSplitsFromStart() throws IOException {
    String header = "<h>";
    List<String> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      data.add(header);
      data.addAll(createStringDataset(3, 9));
    }
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 60, header);
    TestFileBasedSource source2 =
        new TestFileBasedSource(file.getPath(), 64, 60, Long.MAX_VALUE, header);

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data);
    // Remove all occurrences of header from expected results.
    expectedResults.removeAll(Arrays.asList(header));

    List<String> results = new ArrayList<>();
    results.addAll(readEverythingFromReader(source1.createReader(null, null)));
    results.addAll(readEverythingFromReader(source2.createReader(null, null)));

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadRangeFromFileWithSplitsFromMiddle() throws IOException {
    String header = "<h>";
    List<String> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      data.add(header);
      data.addAll(createStringDataset(3, 9));
    }
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 42, header);
    TestFileBasedSource source2 = new TestFileBasedSource(file.getPath(), 64, 42, 112, header);
    TestFileBasedSource source3 =
        new TestFileBasedSource(file.getPath(), 64, 112, Long.MAX_VALUE, header);

    List<String> expectedResults = new ArrayList<String>();

    expectedResults.addAll(data);
    // Remove all occurrences of header from expected results.
    expectedResults.removeAll(Arrays.asList(header));

    List<String> results = new ArrayList<>();
    results.addAll(readEverythingFromReader(source1.createReader(null, null)));
    results.addAll(readEverythingFromReader(source2.createReader(null, null)));
    results.addAll(readEverythingFromReader(source3.createReader(null, null)));

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadFileWithSplitsWithEmptyRange() throws IOException {
    String header = "<h>";
    List<String> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      data.add(header);
      data.addAll(createStringDataset(3, 9));
    }
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 42, header);
    TestFileBasedSource source2 = new TestFileBasedSource(file.getPath(), 64, 42, 62, header);
    TestFileBasedSource source3 =
        new TestFileBasedSource(file.getPath(), 64, 62, Long.MAX_VALUE, header);

    List<String> expectedResults = new ArrayList<String>();

    expectedResults.addAll(data);
    // Remove all occurrences of header from expected results.
    expectedResults.removeAll(Arrays.asList(header));

    List<String> results = new ArrayList<>();
    results.addAll(readEverythingFromReader(source1.createReader(null, null)));
    results.addAll(readEverythingFromReader(source2.createReader(null, null)));
    results.addAll(readEverythingFromReader(source3.createReader(null, null)));

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadRangeFromFileWithSplitsFromMiddleOfHeader() throws IOException {
    String header = "<h>";
    List<String> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      data.add(header);
      data.addAll(createStringDataset(3, 9));
    }
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data.subList(10, data.size()));
    // Remove all occurrences of header from expected results.
    expectedResults.removeAll(Arrays.asList(header));

    // Split starts after "<" of the header
    TestFileBasedSource source =
        new TestFileBasedSource(file.getPath(), 64, 1, Long.MAX_VALUE, header);
    assertThat(expectedResults, containsInAnyOrder(
        readEverythingFromReader(source.createReader(null, null)).toArray()));

    // Split starts after "<h" of the header
    source = new TestFileBasedSource(file.getPath(), 64, 2, Long.MAX_VALUE, header);
    assertThat(expectedResults, containsInAnyOrder(
        readEverythingFromReader(source.createReader(null, null)).toArray()));

    // Split starts after "<h>" of the header
    source = new TestFileBasedSource(file.getPath(), 64, 3, Long.MAX_VALUE, header);
    assertThat(expectedResults, containsInAnyOrder(
        readEverythingFromReader(source.createReader(null, null)).toArray()));
  }

  @Test
  public void testReadRangeAtMiddle() throws IOException {
    List<String> data = createStringDataset(3, 50);
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 52, null);
    TestFileBasedSource source2 = new TestFileBasedSource(file.getPath(), 64, 52, 72, null);
    TestFileBasedSource source3 =
        new TestFileBasedSource(file.getPath(), 64, 72, Long.MAX_VALUE, null);

    List<String> results = new ArrayList<>();
    results.addAll(readEverythingFromReader(source1.createReader(null, null)));
    results.addAll(readEverythingFromReader(source2.createReader(null, null)));
    results.addAll(readEverythingFromReader(source3.createReader(null, null)));

    assertThat(data, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadRangeAtEnd() throws IOException {
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 162, null);
    TestFileBasedSource source2 =
        new TestFileBasedSource(file.getPath(), 1024, 162, Long.MAX_VALUE, null);

    List<String> results = new ArrayList<>();
    results.addAll(readEverythingFromReader(source1.createReader(null, null)));
    results.addAll(readEverythingFromReader(source2.createReader(null, null)));
    assertThat(data, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadAllSplitsOfSingleFile() throws Exception {
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(false, file.getPath(), 16, null);

    List<? extends Source<String>> sources = source.splitIntoBundles(32, null);

    // Not a trivial split.
    assertTrue(sources.size() > 1);

    List<String> results = new ArrayList<String>();
    for (Source<String> split : sources) {
      results.addAll(readEverythingFromReader(split.createReader(null, null)));
    }

    assertThat(data, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testDataflowFile() throws IOException {
    TestDataflowPipelineOptions options =
        PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    DirectPipeline p = DirectPipeline.createForTest();
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(false, file.getPath(), 64, null);

    PCollection<String> output = p.apply(ReadSource.from(source).named("ReadFileData"));

    EvaluationResults results = p.run();
    List<String> readData = results.getPCollection(output);

    // Need to sort here since we have no control over the order of files returned from a file
    // pattern expansion.
    Collections.sort(data);
    Collections.sort(readData);

    assertThat(data, containsInAnyOrder(readData.toArray()));
  }

  @Test
  public void testDataflowFilePattern() throws IOException {
    TestDataflowPipelineOptions options =
        PipelineOptionsFactory.as(TestDataflowPipelineOptions.class);
    options.setGcpCredential(new TestCredential());

    DirectPipeline p = DirectPipeline.createForTest();

    List<String> data1 = createStringDataset(3, 50);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source =
        new TestFileBasedSource(true, new File(file1.getParent(), "file*").getPath(), 64, null);

    PCollection<String> output = p.apply(ReadSource.from(source).named("ReadFileData"));

    EvaluationResults pipelineResults = p.run();
    List<String> results = pipelineResults.getPCollection(output);

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data1);
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);

    // Need to sort here since we have no control over the order of files returned from a file
    // pattern expansion.
    Collections.sort(expectedResults);
    Collections.sort(results);

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testEstimatedSizeOfFile() throws Exception {
    List<String> data = createStringDataset(3, 50);
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(false, file.getPath(), 64, null);
    assertEquals(file.length(), source.getEstimatedSizeBytes(null));
  }

  @Test
  public void testEstimatedSizeOfFilePattern() throws Exception {
    List<String> data1 = createStringDataset(3, 20);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 40);
    File file2 = createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 30);
    File file3 = createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 45);
    createFileWithData("otherfile", data4);

    List<String> data5 = createStringDataset(3, 53);
    createFileWithData("anotherfile", data5);

    TestFileBasedSource source =
        new TestFileBasedSource(true, new File(file1.getParent(), "file*").getPath(), 64, null);

    // Estimated size of the file pattern based source should be the total size of files that the
    // corresponding pattern is expanded into.
    assertEquals(file1.length() + file2.length() + file3.length(),
        source.getEstimatedSizeBytes(null));
  }

  @Test
  public void testReadAllSplitsOfFilePattern() throws Exception {
    List<String> data1 = createStringDataset(3, 50);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source =
        new TestFileBasedSource(true, new File(file1.getParent(), "file*").getPath(), 64, null);
    List<? extends Source<String>> sources = source.splitIntoBundles(512, null);

    // Not a trivial split.
    assertTrue(sources.size() > 1);

    List<String> results = new ArrayList<String>();
    for (Source<String> split : sources) {
      results.addAll(readEverythingFromReader(split.createReader(null, null)));
    }

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data1);
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }
}
