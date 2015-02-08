/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.forkRequestAtByteOffset;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.forkRequestAtPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionFromForkResult;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.Position;
import com.google.cloud.dataflow.sdk.TestUtils;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.io.TextIO.CompressionType;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

/**
 * Tests for TextReader.
 */
@RunWith(JUnit4.class)
public class TextReaderTest {
  private static final String[] fileContent = {"First line\n", "Second line\r\n", "Third line"};
  private static final long TOTAL_BYTES_COUNT;

  static {
    long sumLen = 0L;

    for (String s : fileContent) {
      sumLen += s.length();
    }
    TOTAL_BYTES_COUNT = sumLen;
  }

  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  private File initTestFile() throws IOException {
    File tmpFile = tmpFolder.newFile();
    FileOutputStream output = new FileOutputStream(tmpFile);
    for (String s : fileContent) {
      output.write(s.getBytes());
    }
    output.close();

    return tmpFile;
  }

  @Test
  public void testReadEmptyFile() throws Exception {
    TextReader<String> textReader = new TextReader<>(tmpFolder.newFile().getPath(), true, null,
        null, StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
    try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testStrippedNewlines() throws Exception {
    testNewlineHandling("\r", true);
    testNewlineHandling("\r\n", true);
    testNewlineHandling("\n", true);
  }

  @Test
  public void testStrippedNewlinesAtEndOfReadBuffer() throws Exception {
    boolean stripNewLines = true;
    StringBuilder payload = new StringBuilder();
    for (int i = 0; i < TextReader.BUF_SIZE - 2; ++i) {
      payload.append('a');
    }
    String[] lines = {payload.toString(), payload.toString()};
    testStringPayload(lines, "\r", stripNewLines);
    testStringPayload(lines, "\r\n", stripNewLines);
    testStringPayload(lines, "\n", stripNewLines);
  }

  @Test
  public void testUnstrippedNewlines() throws Exception {
    testNewlineHandling("\r", false);
    testNewlineHandling("\r\n", false);
    testNewlineHandling("\n", false);
  }

  @Test
  public void testUnstrippedNewlinesAtEndOfReadBuffer() throws Exception {
    boolean stripNewLines = false;
    StringBuilder payload = new StringBuilder();
    for (int i = 0; i < TextReader.BUF_SIZE - 2; ++i) {
      payload.append('a');
    }
    String[] lines = {payload.toString(), payload.toString()};
    testStringPayload(lines, "\r", stripNewLines);
    testStringPayload(lines, "\r\n", stripNewLines);
    testStringPayload(lines, "\n", stripNewLines);
  }

  @Test
  public void testStartPosition() throws Exception {
    File tmpFile = initTestFile();

    {
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), false, 11L, null,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
        assertEquals("Second line\r\n", iterator.next());
        assertEquals("Third line", iterator.next());
        assertFalse(iterator.hasNext());
        // The first '1' in the array represents the reading of '\n' between first and
        // second line, to confirm that we are reading from the beginning of a record.
        assertEquals(Arrays.asList(1, 13, 10), observer.getActualSizes());
      }
    }

    {
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), false, 20L, null,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
        assertEquals("Third line", iterator.next());
        assertFalse(iterator.hasNext());
        // The first '5' in the array represents the reading of a portion of the second
        // line, which had to be read to find the beginning of the third line.
        assertEquals(Arrays.asList(5, 10), observer.getActualSizes());
      }
    }

    {
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), true, 0L, 20L,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
        assertEquals("First line", iterator.next());
        assertEquals("Second line", iterator.next());
        assertFalse(iterator.hasNext());
        assertEquals(Arrays.asList(11, 13), observer.getActualSizes());
      }
    }

    {
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), true, 1L, 20L,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
        assertEquals("Second line", iterator.next());
        assertFalse(iterator.hasNext());
        // The first '11' in the array represents the reading of the entire first
        // line, which had to be read to find the beginning of the second line.
        assertEquals(Arrays.asList(11, 13), observer.getActualSizes());
      }
    }
  }

  @Test
  public void testUtf8Handling() throws Exception {
    File tmpFile = tmpFolder.newFile();
    FileOutputStream output = new FileOutputStream(tmpFile);
    // first line:  €\n
    // second line: ¢\n
    output.write(
        new byte[] {(byte) 0xE2, (byte) 0x82, (byte) 0xAC, '\n', (byte) 0xC2, (byte) 0xA2, '\n'});
    output.close();

    {
      // 3L is after the first line if counting codepoints, but within
      // the first line if counting chars.  So correct behavior is to return
      // just one line, since offsets are in chars, not codepoints.
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), true, 0L, 3L,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
        assertArrayEquals("€".getBytes("UTF-8"), iterator.next().getBytes("UTF-8"));
        assertFalse(iterator.hasNext());
        assertEquals(Arrays.asList(4), observer.getActualSizes());
      }
    }

    {
      // Starting location is mid-way into a codepoint.
      // Ensures we don't fail when skipping over an incomplete codepoint.
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), true, 2L, null,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
        assertArrayEquals("¢".getBytes("UTF-8"), iterator.next().getBytes("UTF-8"));
        assertFalse(iterator.hasNext());
        // The first '3' in the array represents the reading of a portion of the first
        // line, which had to be read to find the beginning of the second line.
        assertEquals(Arrays.asList(3, 3), observer.getActualSizes());
      }
    }
  }

  private void testNewlineHandling(String separator, boolean stripNewlines) throws Exception {
    File tmpFile = tmpFolder.newFile();
    PrintStream writer = new PrintStream(new FileOutputStream(tmpFile));
    List<String> expected = Arrays.asList("", "  hi there  ", "bob", "", "  ", "--zowie!--", "");
    List<Integer> expectedSizes = new ArrayList<>();
    for (String line : expected) {
      writer.print(line);
      writer.print(separator);
      expectedSizes.add(line.length() + separator.length());
    }
    writer.close();

    TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), stripNewlines, null, null,
        StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
    ExecutorTestUtils.TestReaderObserver observer =
        new ExecutorTestUtils.TestReaderObserver(textReader);

    List<String> actual = new ArrayList<>();
    try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
      while (iterator.hasNext()) {
        actual.add(iterator.next());
      }
    }

    if (stripNewlines) {
      assertEquals(expected, actual);
    } else {
      List<String> unstripped = new LinkedList<>();
      for (String s : expected) {
        unstripped.add(s + separator);
      }
      assertEquals(unstripped, actual);
    }

    assertEquals(expectedSizes, observer.getActualSizes());
  }

  private void testStringPayload(String[] lines, String separator, boolean stripNewlines)
      throws Exception {
    File tmpFile = tmpFolder.newFile();
    List<String> expected = new ArrayList<>();
    PrintStream writer = new PrintStream(new FileOutputStream(tmpFile));
    for (String line : lines) {
      writer.print(line);
      writer.print(separator);
      expected.add(stripNewlines ? line : line + separator);
    }
    writer.close();

    TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), stripNewlines, null, null,
        StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
    List<String> actual = new ArrayList<>();
    try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
      while (iterator.hasNext()) {
        actual.add(iterator.next());
      }
    }
    assertEquals(expected, actual);
  }

  @Test
  public void testCloneIteratorWithEndPositionAndFinalBytesInBuffer() throws Exception {
    String line = "a\n";
    boolean stripNewlines = false;
    File tmpFile = tmpFolder.newFile();
    List<String> expected = new ArrayList<>();
    PrintStream writer = new PrintStream(new FileOutputStream(tmpFile));
    // Write 5x the size of the buffer and 10 extra trailing bytes
    for (long bytesWritten = 0; bytesWritten < TextReader.BUF_SIZE * 3 + 10;) {
      writer.print(line);
      expected.add(line);
      bytesWritten += line.length();
    }
    writer.close();
    Long fileSize = tmpFile.length();

    TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), stripNewlines, null,
        fileSize, StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);

    List<String> actual = new ArrayList<>();
    Reader.ReaderIterator<String> iterator = textReader.iterator();
    while (iterator.hasNext()) {
      actual.add(iterator.next());
      iterator = iterator.copy();
    }
    assertEquals(expected, actual);
  }

  @Test
  public void testNonStringCoders() throws Exception {
    File tmpFile = tmpFolder.newFile();
    PrintStream writer = new PrintStream(new FileOutputStream(tmpFile));
    List<Integer> expected = TestUtils.INTS;
    List<Integer> expectedSizes = new ArrayList<>();
    for (Integer elem : expected) {
      byte[] encodedElem = CoderUtils.encodeToByteArray(TextualIntegerCoder.of(), elem);
      writer.print(elem);
      writer.print("\n");
      expectedSizes.add(1 + encodedElem.length);
    }
    writer.close();

    TextReader<Integer> textReader = new TextReader<>(tmpFile.getPath(), true, null, null,
        TextualIntegerCoder.of(), TextIO.CompressionType.UNCOMPRESSED);
    ExecutorTestUtils.TestReaderObserver observer =
        new ExecutorTestUtils.TestReaderObserver(textReader);

    List<Integer> actual = new ArrayList<>();
    try (Reader.ReaderIterator<Integer> iterator = textReader.iterator()) {
      while (iterator.hasNext()) {
        actual.add(iterator.next());
      }
    }

    assertEquals(expected, actual);
    assertEquals(expectedSizes, observer.getActualSizes());
  }

  @Test
  public void testGetApproximatePosition() throws Exception {
    File tmpFile = initTestFile();
    TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), false, 0L, null,
        StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);

    try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
      ApproximateProgress progress = readerProgressToCloudProgress(iterator.getProgress());
      assertEquals(0L, progress.getPosition().getByteOffset().longValue());
      iterator.next();
      progress = readerProgressToCloudProgress(iterator.getProgress());
      assertEquals(11L, progress.getPosition().getByteOffset().longValue());
      iterator.next();
      progress = readerProgressToCloudProgress(iterator.getProgress());
      assertEquals(24L, progress.getPosition().getByteOffset().longValue());
      iterator.next();
      progress = readerProgressToCloudProgress(iterator.getProgress());
      assertEquals(34L, progress.getPosition().getByteOffset().longValue());
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testUpdateStopPosition() throws Exception {
    final long end = 10L; // in the first line
    final long stop = 14L; // in the middle of the second line
    File tmpFile = initTestFile();

    // Illegal proposed stop position, no update.
    {
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), false, null, null,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);

      try (TextReader<String>.TextFileIterator iterator =
          (TextReader<String>.TextFileIterator) textReader.iterator()) {
        assertNull(iterator.requestFork(forkRequestAtPosition(new Position())));
      }
    }

    // Successful update.
    {
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), false, null, null,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (TextReader<String>.TextFileIterator iterator =
          (TextReader<String>.TextFileIterator) textReader.iterator()) {
        assertNull(iterator.getEndOffset());
        assertEquals(
            Long.valueOf(stop),
            positionFromForkResult(iterator.requestFork(forkRequestAtByteOffset(stop)))
                .getByteOffset());
        assertEquals(stop, iterator.getEndOffset().longValue());
        assertEquals(fileContent[0], iterator.next());
        assertEquals(fileContent[1], iterator.next());
        assertFalse(iterator.hasNext());
        assertEquals(
            Arrays.asList(fileContent[0].length(), fileContent[1].length()),
            observer.getActualSizes());
      }
    }

    // Proposed stop position is before the current position, no update.
    {
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), false, null, null,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (TextReader<String>.TextFileIterator iterator =
          (TextReader<String>.TextFileIterator) textReader.iterator()) {
        assertEquals(fileContent[0], iterator.next());
        assertEquals(fileContent[1], iterator.next());
        assertThat(
            readerProgressToCloudProgress(iterator.getProgress()).getPosition().getByteOffset(),
            greaterThan(stop));
        assertNull(iterator.requestFork(forkRequestAtByteOffset(stop)));
        assertNull(iterator.getEndOffset());
        assertTrue(iterator.hasNext());
        assertEquals(fileContent[2], iterator.next());
        assertEquals(
            Arrays.asList(
                fileContent[0].length(), fileContent[1].length(), fileContent[2].length()),
            observer.getActualSizes());
      }
    }

    // Proposed stop position is after the current stop (end) position, no update.
    {
      TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), false, null, end,
          StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
      ExecutorTestUtils.TestReaderObserver observer =
          new ExecutorTestUtils.TestReaderObserver(textReader);

      try (TextReader<String>.TextFileIterator iterator =
          (TextReader<String>.TextFileIterator) textReader.iterator()) {
        assertEquals(fileContent[0], iterator.next());
        try {
          iterator.requestFork(forkRequestAtByteOffset(stop));
          fail("IllegalArgumentException expected");
        } catch (IllegalArgumentException e) {
          assertThat(e.getMessage(), Matchers.containsString(
              "Fork requested at an offset beyond the end of the current range"));
        }

        assertEquals(end, iterator.getEndOffset().longValue());
        assertFalse(iterator.hasNext());
        assertEquals(Arrays.asList(fileContent[0].length()), observer.getActualSizes());
      }
    }
  }

  @Test
  public void testUpdateStopPositionExhaustive() throws Exception {
    File tmpFile = initTestFile();

    // Checks for every possible position in the file, that either we fail to
    // "updateStop" at it, or we succeed and then reading both halves together
    // yields the original file with no missed records or duplicates.
    for (long start = 0; start < TOTAL_BYTES_COUNT - 1; start++) {
      for (long end = start + 1; end < TOTAL_BYTES_COUNT; end++) {
        for (long stop = start; stop <= end; stop++) {
          stopPositionTestInternal(start, end, stop, tmpFile);
        }
      }
    }

    // Test with null start/end positions.
    for (long stop = 0L; stop < TOTAL_BYTES_COUNT; stop++) {
      stopPositionTestInternal(null, null, stop, tmpFile);
    }
  }

  private void stopPositionTestInternal(
      Long startOffset, Long endOffset, Long stopOffset, File tmpFile) throws Exception {
    String readWithoutSplit;
    String readWithSplit1, readWithSplit2;
    StringBuilder accumulatedRead = new StringBuilder();

    // Read from source without split attempts.
    TextReader<String> textReader = new TextReader<>(tmpFile.getPath(), false, startOffset,
        endOffset, StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);

    try (TextReader<String>.TextFileIterator iterator =
        (TextReader<String>.TextFileIterator) textReader.iterator()) {
      while (iterator.hasNext()) {
        accumulatedRead.append(iterator.next());
      }
      readWithoutSplit = accumulatedRead.toString();
    }

    // Read the first half of the split.
    textReader = new TextReader<>(tmpFile.getPath(), false, startOffset, stopOffset,
        StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
    accumulatedRead = new StringBuilder();

    try (TextReader<String>.TextFileIterator iterator =
        (TextReader<String>.TextFileIterator) textReader.iterator()) {
      while (iterator.hasNext()) {
        accumulatedRead.append(iterator.next());
      }
      readWithSplit1 = accumulatedRead.toString();
    }

    // Read the second half of the split.
    textReader = new TextReader<>(tmpFile.getPath(), false, stopOffset, endOffset,
        StringUtf8Coder.of(), TextIO.CompressionType.UNCOMPRESSED);
    accumulatedRead = new StringBuilder();

    try (TextReader<String>.TextFileIterator iterator =
        (TextReader<String>.TextFileIterator) textReader.iterator()) {
      while (iterator.hasNext()) {
        accumulatedRead.append(iterator.next());
      }
      readWithSplit2 = accumulatedRead.toString();
    }

    assertEquals(readWithoutSplit, readWithSplit1 + readWithSplit2);
  }

  private OutputStream getOutputStreamForCompressionType(
      OutputStream stream, CompressionType compressionType) throws IOException {
    switch (compressionType) {
      case GZIP:
        return new GZIPOutputStream(stream);
      case BZIP2:
        return new BZip2CompressorOutputStream(stream);
      case UNCOMPRESSED:
      case AUTO:
        return stream;
      default:
        fail("Unrecognized stream type");
    }
    return stream;
  }

  private File createFileWithCompressionType(
      String[] lines, String filename, CompressionType compressionType) throws IOException {
    File tmpFile = tmpFolder.newFile(filename);
    PrintStream writer = new PrintStream(
        getOutputStreamForCompressionType(new FileOutputStream(tmpFile), compressionType));
    for (String line : lines) {
      writer.println(line);
    }
    writer.close();
    return tmpFile;
  }

  private void testCompressionTypeHelper(String[] lines, String filename,
      CompressionType outputCompressionType, CompressionType inputCompressionType)
      throws IOException {
    File tmpFile = createFileWithCompressionType(lines, filename, outputCompressionType);

    List<String> expected = new ArrayList<>();
    for (String line : lines) {
      expected.add(line);
    }

    TextReader<String> textReader = new TextReader<>(
        tmpFile.getPath(), true, null, null, StringUtf8Coder.of(), inputCompressionType);

    List<String> actual = new ArrayList<>();
    try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
      while (iterator.hasNext()) {
        actual.add(iterator.next());
      }
    }
    assertEquals(expected, actual);
    tmpFile.delete();
  }

  @Test
  public void testCompressionTypeOneFile() throws IOException {
    String[] contents = {"Miserable pigeon", "Vulnerable sparrow", "Brazen crow"};
    // test AUTO compression type with different extensions
    testCompressionTypeHelper(contents, "test.gz", CompressionType.GZIP, CompressionType.AUTO);
    testCompressionTypeHelper(contents, "test.bz2", CompressionType.BZIP2, CompressionType.AUTO);
    testCompressionTypeHelper(
        contents, "test.txt", CompressionType.UNCOMPRESSED, CompressionType.AUTO);
    testCompressionTypeHelper(contents, "test", CompressionType.UNCOMPRESSED, CompressionType.AUTO);
    // test GZIP, BZIP2, and UNCOMPRESSED
    testCompressionTypeHelper(contents, "test.txt", CompressionType.GZIP, CompressionType.GZIP);
    testCompressionTypeHelper(contents, "test.txt", CompressionType.BZIP2, CompressionType.BZIP2);
    testCompressionTypeHelper(
        contents, "test.gz", CompressionType.UNCOMPRESSED, CompressionType.UNCOMPRESSED);
  }

  @Test
  public void testCompressionTypeFileGlob() throws IOException {
    String[][] contents = {
        {"Miserable pigeon", "Vulnerable sparrow", "Brazen crow"}, {"Timid osprey", "Lazy vulture"},
        {"Erratic finch", "Impressible parakeet"},
    };
    File[] files = {
        createFileWithCompressionType(contents[0], "test.gz", CompressionType.GZIP),
        createFileWithCompressionType(contents[1], "test.bz2", CompressionType.BZIP2),
        createFileWithCompressionType(contents[2], "test.txt", CompressionType.UNCOMPRESSED),
    };

    List<String> expected = new ArrayList<>();
    for (String[] fileContents : contents) {
      for (String line : fileContents) {
        expected.add(line);
      }
    }

    String path = tmpFolder.getRoot().getPath() + System.getProperty("file.separator") + "*";

    TextReader<String> textReader =
        new TextReader<>(path, true, null, null, StringUtf8Coder.of(), CompressionType.AUTO);

    List<String> actual = new ArrayList<>();
    try (Reader.ReaderIterator<String> iterator = textReader.iterator()) {
      while (iterator.hasNext()) {
        actual.add(iterator.next());
      }
    }
    assertThat(actual, containsInAnyOrder(expected.toArray()));
    for (File file : files) {
      file.delete();
    }
  }

  // TODO: sharded filenames
  // TODO: reading from GCS
}
