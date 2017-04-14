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

import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionExhaustive;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionFails;
import static org.apache.beam.sdk.testing.SourceTestUtils.assertSplitAtFractionSucceedsAndConsistent;
import static org.apache.beam.sdk.testing.SourceTestUtils.readFromSource;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
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
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader;
import org.apache.beam.sdk.io.Source.Reader;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.IOChannelFactory;
import org.apache.beam.sdk.util.IOChannelUtils;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/**
 * Tests code common to all file-based sources.
 */
@RunWith(JUnit4.class)
public class FileBasedSourceTest {

  Random random = new Random(0L);

  @Rule public final TestPipeline p = TestPipeline.create();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();

  /**
   * If {@code splitHeader} is null, this is just a simple line-based reader. Otherwise, the file is
   * considered to consist of blocks beginning with {@code splitHeader}. The header itself is not
   * returned as a record. The first record after the header is considered to be a split point.
   *
   * <p>E.g., if {@code splitHeader} is "h" and the lines of the file are: h, a, b, h, h, c, then
   * the records in this source are a,b,c, and records a and c are split points.
   */
  static class TestFileBasedSource extends FileBasedSource<String> {

    final String splitHeader;

    public TestFileBasedSource(String fileOrPattern, long minBundleSize, String splitHeader) {
      super(fileOrPattern, minBundleSize);
      this.splitHeader = splitHeader;
    }

    public TestFileBasedSource(
        String fileOrPattern,
        long minBundleSize,
        long startOffset,
        long endOffset,
        String splitHeader) {
      super(fileOrPattern, minBundleSize, startOffset, endOffset);
      this.splitHeader = splitHeader;
    }

    @Override
    public void validate() {}

    @Override
    public Coder<String> getDefaultOutputCoder() {
      return StringUtf8Coder.of();
    }

    @Override
    protected FileBasedSource<String> createForSubrangeOfFile(
        String fileName, long start, long end) {
      return new TestFileBasedSource(fileName, getMinBundleSize(), start, end, splitHeader);
    }

    @Override
    protected FileBasedReader<String> createSingleFileReader(PipelineOptions options) {
      if (splitHeader == null) {
        return new TestReader(this);
      } else {
        return new TestReaderWithSplits(this);
      }
    }
  }

  /**
   * A utility class that starts reading lines from a given offset in a file until EOF.
   */
  private static class LineReader {
    private ReadableByteChannel channel = null;
    private long nextLineStart = 0;
    private long currentLineStart = 0;
    private final ByteBuffer buf;
    private static final int BUF_SIZE = 1024;
    private String currentValue = null;

    public LineReader(ReadableByteChannel channel) throws IOException {
      buf = ByteBuffer.allocate(BUF_SIZE);
      buf.flip();

      boolean removeLine = false;
      // If we are not at the beginning of a line, we should ignore the current line.
      if (channel instanceof SeekableByteChannel) {
        SeekableByteChannel seekChannel = (SeekableByteChannel) channel;
        if (seekChannel.position() > 0) {
          // Start from one character back and read till we find a new line.
          seekChannel.position(seekChannel.position() - 1);
          removeLine = true;
        }
        nextLineStart = seekChannel.position();
      }
      this.channel = channel;
      if (removeLine) {
        nextLineStart += readNextLine(new ByteArrayOutputStream());
      }
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
        if (b == '\n') {
          break;
        }
        out.write(b);
      }
      return byteCount;
    }

    public boolean readNextLine() throws IOException {
      currentLineStart = nextLineStart;

      ByteArrayOutputStream buf = new ByteArrayOutputStream();
      int offsetAdjustment = readNextLine(buf);
      if (offsetAdjustment == 0) {
        // EOF
        return false;
      }
      nextLineStart += offsetAdjustment;
      // When running on Windows, each line obtained from 'readNextLine()' will end with a '\r'
      // since we use '\n' as the line boundary of the reader. So we trim it off here.
      currentValue = CoderUtils.decodeFromByteArray(StringUtf8Coder.of(), buf.toByteArray()).trim();
      return true;
    }

    public String getCurrent() {
      return currentValue;
    }

    public long getCurrentLineStart() {
      return currentLineStart;
    }
  }

  /**
   * A reader that can read lines of text from a {@link TestFileBasedSource}. This reader does not
   * consider {@code splitHeader} defined by {@code TestFileBasedSource} hence every line can be the
   * first line of a split.
   */
  private static class TestReader extends FileBasedReader<String> {
    private LineReader lineReader = null;

    public TestReader(TestFileBasedSource source) {
      super(source);
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      this.lineReader = new LineReader(channel);
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      return lineReader.readNextLine();
    }

    @Override
    protected boolean isAtSplitPoint() {
      return true;
    }

    @Override
    protected long getCurrentOffset() {
      return lineReader.getCurrentLineStart();
    }

    @Override
    public String getCurrent() throws NoSuchElementException {
      return lineReader.getCurrent();
    }
  }

  /**
   * A reader that can read lines of text from a {@link TestFileBasedSource}. This reader considers
   * {@code splitHeader} defined by {@code TestFileBasedSource} hence only lines that immediately
   * follow a {@code splitHeader} are split points.
   */
  private static class TestReaderWithSplits extends FileBasedReader<String> {
    private LineReader lineReader;
    private final String splitHeader;
    private boolean foundFirstSplitPoint = false;
    private boolean isAtSplitPoint = false;
    private long currentOffset;

    public TestReaderWithSplits(TestFileBasedSource source) {
      super(source);
      this.splitHeader = source.splitHeader;
    }

    @Override
    protected void startReading(ReadableByteChannel channel) throws IOException {
      this.lineReader = new LineReader(channel);
    }

    @Override
    protected boolean readNextRecord() throws IOException {
      if (!foundFirstSplitPoint) {
        while (!isAtSplitPoint) {
          if (!readNextRecordInternal()) {
            return false;
          }
        }
        foundFirstSplitPoint = true;
        return true;
      }
      return readNextRecordInternal();
    }

    private boolean readNextRecordInternal() throws IOException {
      isAtSplitPoint = false;
      if (!lineReader.readNextLine()) {
        return false;
      }
      currentOffset = lineReader.getCurrentLineStart();
      while (getCurrent().equals(splitHeader)) {
        currentOffset = lineReader.getCurrentLineStart();
        if (!lineReader.readNextLine()) {
          return false;
        }
        isAtSplitPoint = true;
      }
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
      return lineReader.getCurrent();
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

  @Test
  public void testFullyReadSingleFile() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(file.getPath(), 64, null);
    assertEquals(data, readFromSource(source, options));
  }

  @Test
  public void testFullyReadFilePattern() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<String> data1 = createStringDataset(3, 50);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source =
        new TestFileBasedSource(new File(file1.getParent(), "file*").getPath(), 64, null);
    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data1);
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);
    assertThat(expectedResults, containsInAnyOrder(readFromSource(source, options).toArray()));
  }

  @Test
  public void testCloseUnstartedFilePatternReader() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<String> data1 = createStringDataset(3, 50);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source =
        new TestFileBasedSource(new File(file1.getParent(), "file*").getPath(), 64, null);
    Reader<String> reader = source.createReader(options);
    // Closing an unstarted FilePatternReader should not throw an exception.
    try {
      reader.close();
    } catch (Exception e) {
      fail("Closing an unstarted FilePatternReader should not throw an exception");
    }
  }

  @Test
  public void testSplittingUsingFullThreadPool() throws Exception {
    int numFiles = FileBasedSource.THREAD_POOL_SIZE * 5;
    File file0 = null;
    for (int i = 0; i < numFiles; i++) {
      List<String> data = createStringDataset(3, 1000);
      File file = createFileWithData("file" + i, data);
      if (i == 0) {
        file0 = file;
      }
    }

    TestFileBasedSource source =
        new TestFileBasedSource(file0.getParent() + "/" + "file*", Long.MAX_VALUE, null);
    List<? extends BoundedSource<String>> splits = source.split(Long.MAX_VALUE, null);
    assertEquals(numFiles, splits.size());
  }

  @Test
  public void testSplittingFailsOnEmptyFileExpansion() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    String missingFilePath = tempFolder.newFolder().getAbsolutePath() + "/missing.txt";
    TestFileBasedSource source = new TestFileBasedSource(missingFilePath, Long.MAX_VALUE, null);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(String.format("Unable to find any files matching %s", missingFilePath));
    source.split(1234, options);
  }

  @Test
  public void testFractionConsumedWhenReadingFilepattern() throws IOException {
    List<String> data1 = createStringDataset(3, 1000);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 1000);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 1000);
    createFileWithData("file3", data3);

    TestFileBasedSource source =
        new TestFileBasedSource(file1.getParent() + "/" + "file*", 1024, null);
    try (BoundedSource.BoundedReader<String> reader = source.createReader(null)) {
      double lastFractionConsumed = 0.0;
      assertEquals(0.0, reader.getFractionConsumed(), 1e-6);
      assertTrue(reader.start());
      assertTrue(reader.advance());
      assertTrue(reader.advance());
      // We're inside the first file. Should be in [0, 1/3).
      assertTrue(reader.getFractionConsumed() > 0.0);
      assertTrue(reader.getFractionConsumed() < 1.0 / 3.0);
      while (reader.advance()) {
        double fractionConsumed = reader.getFractionConsumed();
        assertTrue(fractionConsumed > lastFractionConsumed);
        lastFractionConsumed = fractionConsumed;
      }
      assertEquals(1.0, reader.getFractionConsumed(), 1e-6);
    }
  }

  @Test
  public void testFullyReadFilePatternFirstRecordEmpty() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    File file1 = createFileWithData("file1", new ArrayList<String>());

    IOChannelFactory mockIOFactory = Mockito.mock(IOChannelFactory.class);
    String parent = file1.getParent();
    String pattern = "mocked://test";
    when(mockIOFactory.match(pattern))
        .thenReturn(
            ImmutableList.of(
                new File(parent, "file1").getPath(),
                new File(parent, "file2").getPath(),
                new File(parent, "file3").getPath()));
    IOChannelUtils.setIOFactoryInternal("mocked", mockIOFactory, true /* override */);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source = new TestFileBasedSource(pattern, 64, null);

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);
    assertThat(expectedResults, containsInAnyOrder(readFromSource(source, options).toArray()));
  }

  @Test
  public void testReadRangeAtStart() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 25, null);
    TestFileBasedSource source2 =
        new TestFileBasedSource(file.getPath(), 64, 25, Long.MAX_VALUE, null);

    List<String> results = new ArrayList<String>();
    results.addAll(readFromSource(source1, options));
    results.addAll(readFromSource(source2, options));
    assertThat(data, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadEverythingFromFileWithSplits() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    String header = "<h>";
    List<String> data = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      data.add(header);
      data.addAll(createStringDataset(3, 9));
    }
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(file.getPath(), 64, header);

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data);
    // Remove all occurrences of header from expected results.
    expectedResults.removeAll(Arrays.asList(header));

    assertEquals(expectedResults, readFromSource(source, options));
  }

  @Test
  public void testReadRangeFromFileWithSplitsFromStart() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
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
    results.addAll(readFromSource(source1, options));
    results.addAll(readFromSource(source2, options));

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadRangeFromFileWithSplitsFromMiddle() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
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
    results.addAll(readFromSource(source1, options));
    results.addAll(readFromSource(source2, options));
    results.addAll(readFromSource(source3, options));

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadFileWithSplitsWithEmptyRange() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
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
    results.addAll(readFromSource(source1, options));
    results.addAll(readFromSource(source2, options));
    results.addAll(readFromSource(source3, options));

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadRangeFromFileWithSplitsFromMiddleOfHeader() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
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
    assertThat(expectedResults, containsInAnyOrder(readFromSource(source, options).toArray()));

    // Split starts after "<h" of the header
    source = new TestFileBasedSource(file.getPath(), 64, 2, Long.MAX_VALUE, header);
    assertThat(expectedResults, containsInAnyOrder(readFromSource(source, options).toArray()));

    // Split starts after "<h>" of the header
    source = new TestFileBasedSource(file.getPath(), 64, 3, Long.MAX_VALUE, header);
    assertThat(expectedResults, containsInAnyOrder(readFromSource(source, options).toArray()));
  }

  @Test
  public void testReadRangeAtMiddle() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<String> data = createStringDataset(3, 50);
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 52, null);
    TestFileBasedSource source2 = new TestFileBasedSource(file.getPath(), 64, 52, 72, null);
    TestFileBasedSource source3 =
        new TestFileBasedSource(file.getPath(), 64, 72, Long.MAX_VALUE, null);

    List<String> results = new ArrayList<>();
    results.addAll(readFromSource(source1, options));
    results.addAll(readFromSource(source2, options));
    results.addAll(readFromSource(source3, options));

    assertThat(data, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadRangeAtEnd() throws IOException {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source1 = new TestFileBasedSource(file.getPath(), 64, 0, 162, null);
    TestFileBasedSource source2 =
        new TestFileBasedSource(file.getPath(), 1024, 162, Long.MAX_VALUE, null);

    List<String> results = new ArrayList<>();
    results.addAll(readFromSource(source1, options));
    results.addAll(readFromSource(source2, options));

    assertThat(data, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testReadAllSplitsOfSingleFile() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(file.getPath(), 16, null);

    List<? extends BoundedSource<String>> sources = source.split(32, null);

    // Not a trivial split.
    assertTrue(sources.size() > 1);

    List<String> results = new ArrayList<String>();
    for (BoundedSource<String> split : sources) {
      results.addAll(readFromSource(split, options));
    }

    assertThat(data, containsInAnyOrder(results.toArray()));
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDataflowFile() throws IOException {
    List<String> data = createStringDataset(3, 50);

    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(file.getPath(), 64, null);
    PCollection<String> output = p.apply("ReadFileData", Read.from(source));

    PAssert.that(output).containsInAnyOrder(data);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testDataflowFilePattern() throws IOException {

    List<String> data1 = createStringDataset(3, 50);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source =
        new TestFileBasedSource(new File(file1.getParent(), "file*").getPath(), 64, null);

    PCollection<String> output = p.apply("ReadFileData", Read.from(source));

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data1);
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);

    PAssert.that(output).containsInAnyOrder(expectedResults);
    p.run();
  }

  @Test
  public void testEstimatedSizeOfFile() throws Exception {
    List<String> data = createStringDataset(3, 50);
    String fileName = "file";
    File file = createFileWithData(fileName, data);

    TestFileBasedSource source = new TestFileBasedSource(file.getPath(), 64, null);
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
        new TestFileBasedSource(new File(file1.getParent(), "file*").getPath(), 64, null);

    // Estimated size of the file pattern based source should be the total size of files that the
    // corresponding pattern is expanded into.
    assertEquals(
        file1.length() + file2.length() + file3.length(), source.getEstimatedSizeBytes(null));
  }

  @Test
  public void testEstimatedSizeOfFilePatternAllThreads() throws Exception {
    File file0 = null;
    int numFiles = FileBasedSource.THREAD_POOL_SIZE * 5;
    long totalSize = 0;
    for (int i = 0; i < numFiles; i++) {
      List<String> data = createStringDataset(3, 20);
      File file = createFileWithData("file" + i, data);
      if (i == 0) {
        file0 = file;
      }
      totalSize += file.length();
    }

    TestFileBasedSource source =
        new TestFileBasedSource(new File(file0.getParent(), "file*").getPath(), 64, null);

    // Since all files are of equal size, sampling should produce the exact result.
    assertEquals(totalSize, source.getEstimatedSizeBytes(null));
  }

  @Test
  public void testEstimatedSizeOfFilePatternThroughSamplingEqualSize() throws Exception {
    // When all files are of equal size, we should get the exact size.
    int numFilesToTest = FileBasedSource.MAX_NUMBER_OF_FILES_FOR_AN_EXACT_STAT * 2;
    File file0 = null;
    for (int i = 0; i < numFilesToTest; i++) {
      List<String> data = createStringDataset(3, 20);
      File file = createFileWithData("file" + i, data);
      if (i == 0) {
        file0 = file;
      }
    }

    long actualTotalSize = file0.length() * numFilesToTest;
    TestFileBasedSource source =
        new TestFileBasedSource(new File(file0.getParent(), "file*").getPath(), 64, null);
    assertEquals(actualTotalSize, source.getEstimatedSizeBytes(null));
  }

  @Test
  public void testEstimatedSizeOfFilePatternThroughSamplingDifferentSizes() throws Exception {
    float tolerableError = 0.2f;
    int numFilesToTest = FileBasedSource.MAX_NUMBER_OF_FILES_FOR_AN_EXACT_STAT * 2;
    File file0 = null;

    // Keeping sizes of files close to each other to make sure that the test passes reliably.
    Random rand = new Random(System.currentTimeMillis());
    int dataSizeBase = 100;
    int dataSizeDelta = 10;

    long actualTotalSize = 0;
    for (int i = 0; i < numFilesToTest; i++) {
      List<String> data = createStringDataset(
          3, (int) (dataSizeBase + rand.nextFloat() * dataSizeDelta * 2 - dataSizeDelta));
      File file = createFileWithData("file" + i, data);
      if (i == 0) {
        file0 = file;
      }
      actualTotalSize += file.length();
    }

    TestFileBasedSource source =
        new TestFileBasedSource(new File(file0.getParent(), "file*").getPath(), 64, null);
    assertEquals((double) actualTotalSize, (double) source.getEstimatedSizeBytes(null),
        actualTotalSize * tolerableError);
  }

  @Test
  public void testReadAllSplitsOfFilePattern() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    List<String> data1 = createStringDataset(3, 50);
    File file1 = createFileWithData("file1", data1);

    List<String> data2 = createStringDataset(3, 50);
    createFileWithData("file2", data2);

    List<String> data3 = createStringDataset(3, 50);
    createFileWithData("file3", data3);

    List<String> data4 = createStringDataset(3, 50);
    createFileWithData("otherfile", data4);

    TestFileBasedSource source =
        new TestFileBasedSource(new File(file1.getParent(), "file*").getPath(), 64, null);
    List<? extends BoundedSource<String>> sources = source.split(512, null);

    // Not a trivial split.
    assertTrue(sources.size() > 1);

    List<String> results = new ArrayList<String>();
    for (BoundedSource<String> split : sources) {
      results.addAll(readFromSource(split, options));
    }

    List<String> expectedResults = new ArrayList<String>();
    expectedResults.addAll(data1);
    expectedResults.addAll(data2);
    expectedResults.addAll(data3);

    assertThat(expectedResults, containsInAnyOrder(results.toArray()));
  }

  @Test
  public void testSplitAtFraction() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    File file = createFileWithData("file", createStringDataset(3, 100));

    TestFileBasedSource source = new TestFileBasedSource(file.getPath(), 1, 0, file.length(), null);
    // Shouldn't be able to split while unstarted.
    assertSplitAtFractionFails(source, 0, 0.7, options);
    assertSplitAtFractionSucceedsAndConsistent(source, 1, 0.7, options);
    assertSplitAtFractionSucceedsAndConsistent(source, 30, 0.7, options);
    assertSplitAtFractionFails(source, 0, 0.0, options);
    assertSplitAtFractionFails(source, 70, 0.3, options);
    assertSplitAtFractionFails(source, 100, 1.0, options);
    assertSplitAtFractionFails(source, 100, 0.99, options);
    assertSplitAtFractionSucceedsAndConsistent(source, 100, 0.995, options);
  }

  @Test
  public void testSplitAtFractionExhaustive() throws Exception {
    PipelineOptions options = PipelineOptionsFactory.create();
    // Smaller file for exhaustive testing.
    File file = createFileWithData("file", createStringDataset(3, 20));

    TestFileBasedSource source = new TestFileBasedSource(file.getPath(), 1, 0, file.length(), null);
    assertSplitAtFractionExhaustive(source, options);
  }

  @Test
  public void testToStringFile() throws Exception {
    String path = "/tmp/foo";
    TestFileBasedSource source = new TestFileBasedSource(path, 1, 0, 10, null);
    assertEquals(String.format("%s range [0, 10)", path), source.toString());
  }

  @Test
  public void testToStringPattern() throws Exception {
    String path = "/tmp/foo/*";
    TestFileBasedSource source = new TestFileBasedSource(path, 1, 0, 10, null);
    assertEquals(String.format("%s range [0, 10)", path), source.toString());
  }
}
