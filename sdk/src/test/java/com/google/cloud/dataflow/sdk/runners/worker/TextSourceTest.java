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

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.cloudProgressToSourceProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourcePositionToCloudPosition;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.sourceProgressToCloudProgress;
import static org.hamcrest.Matchers.greaterThan;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.cloud.dataflow.sdk.TestUtils;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests for TextSource.
 */
@RunWith(JUnit4.class)
public class TextSourceTest {
  private static final String[] fileContent = {"First line\n",
                                               "Second line\r\n",
                                               "Third line"};
  private static final long TOTAL_BYTES_COUNT;

  static {
    long sumLen = 0L;
    for (String s : fileContent) {
      sumLen += s.length();
    }
    TOTAL_BYTES_COUNT = sumLen;
  }

  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

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
    TextSource<String> textSource = new TextSource<>(
        "/dev/null", true, null, null, StringUtf8Coder.of());
    try (Source.SourceIterator<String> iterator = textSource.iterator()) {
      Assert.assertFalse(iterator.hasNext());
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
    for (int i = 0; i < TextSource.BUF_SIZE - 2; ++i) {
      payload.append('a');
    }
    String[] lines = {payload.toString(), payload.toString()};
    testStringPayload(lines , "\r", stripNewLines);
    testStringPayload(lines , "\r\n", stripNewLines);
    testStringPayload(lines , "\n", stripNewLines);
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
    for (int i = 0; i < TextSource.BUF_SIZE - 2; ++i) {
      payload.append('a');
    }
    String[] lines = {payload.toString(), payload.toString()};
    testStringPayload(lines , "\r", stripNewLines);
    testStringPayload(lines , "\r\n", stripNewLines);
    testStringPayload(lines , "\n", stripNewLines);
  }

  @Test
  public void testStartPosition() throws Exception {
    File tmpFile = initTestFile();

    {
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), false, 11L, null, StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (Source.SourceIterator<String> iterator = textSource.iterator()) {
        Assert.assertEquals("Second line\r\n", iterator.next());
        Assert.assertEquals("Third line", iterator.next());
        Assert.assertFalse(iterator.hasNext());
        // The first '1' in the array represents the reading of '\n' between first and
        // second line, to confirm that we are reading from the beginning of a record.
        Assert.assertEquals(Arrays.asList(1, 13, 10), observer.getActualSizes());
      }
    }

    {
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), false, 20L, null, StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (Source.SourceIterator<String> iterator = textSource.iterator()) {
        Assert.assertEquals("Third line", iterator.next());
        Assert.assertFalse(iterator.hasNext());
        // The first '5' in the array represents the reading of a portion of the second
        // line, which had to be read to find the beginning of the third line.
        Assert.assertEquals(Arrays.asList(5, 10), observer.getActualSizes());
      }
    }

    {
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), true, 0L, 20L, StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (Source.SourceIterator<String> iterator = textSource.iterator()) {
        Assert.assertEquals("First line", iterator.next());
        Assert.assertEquals("Second line", iterator.next());
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(Arrays.asList(11, 13), observer.getActualSizes());
      }
    }

    {
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), true, 1L, 20L, StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (Source.SourceIterator<String> iterator = textSource.iterator()) {
        Assert.assertEquals("Second line", iterator.next());
        Assert.assertFalse(iterator.hasNext());
        // The first '11' in the array represents the reading of the entire first
        // line, which had to be read to find the beginning of the second line.
        Assert.assertEquals(Arrays.asList(11, 13), observer.getActualSizes());
      }
    }
  }

  @Test
  public void testUtf8Handling() throws Exception {
    File tmpFile = tmpFolder.newFile();
    FileOutputStream output = new FileOutputStream(tmpFile);
    // first line:  €\n
    // second line: ¢\n
    output.write(new byte[]{(byte) 0xE2, (byte) 0x82, (byte) 0xAC, '\n',
        (byte) 0xC2, (byte) 0xA2, '\n'});
    output.close();

    {
      // 3L is after the first line if counting codepoints, but within
      // the first line if counting chars.  So correct behavior is to return
      // just one line, since offsets are in chars, not codepoints.
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), true, 0L, 3L, StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (Source.SourceIterator<String> iterator = textSource.iterator()) {
        Assert.assertArrayEquals("€".getBytes("UTF-8"),
            iterator.next().getBytes("UTF-8"));
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(Arrays.asList(4), observer.getActualSizes());
      }
    }

    {
      // Starting location is mid-way into a codepoint.
      // Ensures we don't fail when skipping over an incomplete codepoint.
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), true, 2L, null, StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (Source.SourceIterator<String> iterator = textSource.iterator()) {
        Assert.assertArrayEquals("¢".getBytes("UTF-8"),
            iterator.next().getBytes("UTF-8"));
        Assert.assertFalse(iterator.hasNext());
        // The first '3' in the array represents the reading of a portion of the first
        // line, which had to be read to find the beginning of the second line.
        Assert.assertEquals(Arrays.asList(3, 3), observer.getActualSizes());
      }
    }
  }

  private void testNewlineHandling(String separator, boolean stripNewlines)
      throws Exception {
    File tmpFile = tmpFolder.newFile();
    PrintStream writer =
        new PrintStream(
            new FileOutputStream(tmpFile));
    List<String> expected = Arrays.asList(
        "",
        "  hi there  ",
        "bob",
        "",
        "  ",
        "--zowie!--",
        "");
    List<Integer> expectedSizes = new ArrayList<>();
    for (String line : expected) {
      writer.print(line);
      writer.print(separator);
      expectedSizes.add(line.length() + separator.length());
    }
    writer.close();

    TextSource<String> textSource = new TextSource<>(
        tmpFile.getPath(), stripNewlines, null, null, StringUtf8Coder.of());
    ExecutorTestUtils.TestSourceObserver observer =
        new ExecutorTestUtils.TestSourceObserver(textSource);

    List<String> actual = new ArrayList<>();
    try (Source.SourceIterator<String> iterator = textSource.iterator()) {
      while (iterator.hasNext()) {
        actual.add(iterator.next());
      }
    }

    if (stripNewlines) {
      Assert.assertEquals(expected, actual);
    } else {
      List<String> unstripped = new LinkedList<>();
      for (String s : expected) {
        unstripped.add(s + separator);
      }
      Assert.assertEquals(unstripped, actual);
    }

    Assert.assertEquals(expectedSizes, observer.getActualSizes());
  }

  private void testStringPayload(
      String[] lines, String separator, boolean stripNewlines)
      throws Exception {
    File tmpFile = tmpFolder.newFile();
    List<String> expected = new ArrayList<>();
    PrintStream writer =
        new PrintStream(
            new FileOutputStream(tmpFile));
    for (String line : lines) {
      writer.print(line);
      writer.print(separator);
      expected.add(stripNewlines ? line : line + separator);
    }
    writer.close();

    TextSource<String> textSource = new TextSource<>(
        tmpFile.getPath(), stripNewlines, null, null, StringUtf8Coder.of());
    ExecutorTestUtils.TestSourceObserver observer =
        new ExecutorTestUtils.TestSourceObserver(textSource);

    List<String> actual = new ArrayList<>();
    try (Source.SourceIterator<String> iterator = textSource.iterator()) {
        while (iterator.hasNext()) {
          actual.add(iterator.next());
        }
      }
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testCloneIteratorWithEndPositionAndFinalBytesInBuffer()
      throws Exception {
    String line = "a\n";
    boolean stripNewlines = false;
    File tmpFile = tmpFolder.newFile();
    List<String> expected = new ArrayList<>();
    PrintStream writer = new PrintStream(new FileOutputStream(tmpFile));
    // Write 5x the size of the buffer and 10 extra trailing bytes
    for (long bytesWritten = 0;
         bytesWritten < TextSource.BUF_SIZE * 3 + 10; ) {
      writer.print(line);
      expected.add(line);
      bytesWritten += line.length();
    }
    writer.close();
    Long fileSize = tmpFile.length();

    TextSource<String> textSource = new TextSource<>(
        tmpFile.getPath(), stripNewlines,
        null, fileSize, StringUtf8Coder.of());

    List<String> actual = new ArrayList<>();
    Source.SourceIterator<String> iterator = textSource.iterator();
    while (iterator.hasNext()) {
      actual.add(iterator.next());
      iterator = iterator.copy();
    }
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testNonStringCoders() throws Exception {
    File tmpFile = tmpFolder.newFile();
    PrintStream writer =
        new PrintStream(
            new FileOutputStream(tmpFile));
    List<Integer> expected = TestUtils.INTS;
    List<Integer> expectedSizes = new ArrayList<>();
    for (Integer elem : expected) {
      byte[] encodedElem =
          CoderUtils.encodeToByteArray(TextualIntegerCoder.of(), elem);
      writer.print(elem);
      writer.print("\n");
      expectedSizes.add(1 + encodedElem.length);
    }
    writer.close();

    TextSource<Integer> textSource = new TextSource<>(
        tmpFile.getPath(), true, null, null, TextualIntegerCoder.of());
    ExecutorTestUtils.TestSourceObserver observer =
        new ExecutorTestUtils.TestSourceObserver(textSource);

    List<Integer> actual = new ArrayList<>();
    try (Source.SourceIterator<Integer> iterator = textSource.iterator()) {
      while (iterator.hasNext()) {
        actual.add(iterator.next());
      }
    }

    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expectedSizes, observer.getActualSizes());
  }

  @Test
  public void testGetApproximatePosition() throws Exception {
    File tmpFile = initTestFile();
    TextSource<String> textSource = new TextSource<>(
        tmpFile.getPath(), false, 0L, null, StringUtf8Coder.of());

    try (Source.SourceIterator<String> iterator = textSource.iterator()) {
      ApproximateProgress progress =
          sourceProgressToCloudProgress(iterator.getProgress());
      Assert.assertEquals(0L,
          progress.getPosition().getByteOffset().longValue());
      iterator.next();
      progress = sourceProgressToCloudProgress(iterator.getProgress());
      Assert.assertEquals(11L,
          progress.getPosition().getByteOffset().longValue());
      iterator.next();
      progress = sourceProgressToCloudProgress(iterator.getProgress());
      Assert.assertEquals(24L,
          progress.getPosition().getByteOffset().longValue());
      iterator.next();
      progress = sourceProgressToCloudProgress(iterator.getProgress());
      Assert.assertEquals(34L,
          progress.getPosition().getByteOffset().longValue());
      Assert.assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void testUpdateStopPosition() throws Exception {
    final long end = 10L;  // in the first line
    final long stop = 14L;  // in the middle of the second line
    File tmpFile = initTestFile();

    com.google.api.services.dataflow.model.Position proposedStopPosition =
        new com.google.api.services.dataflow.model.Position();

    // Illegal proposed stop position, no update.
    {
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), false, null, null,
          StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (TextSource<String>.TextFileIterator iterator =
          (TextSource<String>.TextFileIterator) textSource.iterator()) {
          Assert.assertNull(iterator.updateStopPosition(
              cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));
        }
    }

    proposedStopPosition.setByteOffset(stop);

    // Successful update.
    {
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), false, null, null,
          StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (TextSource<String>.TextFileIterator iterator =
          (TextSource<String>.TextFileIterator) textSource.iterator()) {
        Assert.assertNull(iterator.getEndOffset());
        Assert.assertEquals(
            stop,
            sourcePositionToCloudPosition(
                iterator.updateStopPosition(
                    cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))))
            .getByteOffset().longValue());
        Assert.assertEquals(stop, iterator.getEndOffset().longValue());
        Assert.assertEquals(fileContent[0], iterator.next());
        Assert.assertEquals(fileContent[1], iterator.next());
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(Arrays.asList(fileContent[0].length(),
                                          fileContent[1].length()),
                            observer.getActualSizes());
      }
    }

    // Proposed stop position is before the current position, no update.
    {
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), false, null, null,
          StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (TextSource<String>.TextFileIterator iterator =
          (TextSource<String>.TextFileIterator) textSource.iterator()) {
        Assert.assertEquals(fileContent[0], iterator.next());
        Assert.assertEquals(fileContent[1], iterator.next());
        Assert.assertThat(sourceProgressToCloudProgress(iterator.getProgress())
                          .getPosition().getByteOffset(),
            greaterThan(stop));
        Assert.assertNull(iterator.updateStopPosition(
            cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));
        Assert.assertNull(iterator.getEndOffset());
        Assert.assertTrue(iterator.hasNext());
        Assert.assertEquals(fileContent[2], iterator.next());
        Assert.assertEquals(Arrays.asList(fileContent[0].length(),
                                          fileContent[1].length(),
                                          fileContent[2].length()),
                            observer.getActualSizes());
      }
    }

    // Proposed stop position is after the current stop (end) position, no update.
    {
      TextSource<String> textSource = new TextSource<>(
          tmpFile.getPath(), false, null, end, StringUtf8Coder.of());
      ExecutorTestUtils.TestSourceObserver observer =
          new ExecutorTestUtils.TestSourceObserver(textSource);

      try (TextSource<String>.TextFileIterator iterator =
          (TextSource<String>.TextFileIterator) textSource.iterator()) {
        Assert.assertEquals(fileContent[0], iterator.next());
        Assert.assertNull(iterator.updateStopPosition(
            cloudProgressToSourceProgress(createApproximateProgress(proposedStopPosition))));
        Assert.assertEquals(end, iterator.getEndOffset().longValue());
        Assert.assertFalse(iterator.hasNext());
        Assert.assertEquals(Arrays.asList(fileContent[0].length()),
                            observer.getActualSizes());
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
          stopPositionTestInternal(start, end,
              stop, tmpFile);
        }
      }
    }

    // Test with null start/end positions.
    for (long stop = 0L; stop < TOTAL_BYTES_COUNT; stop++) {
      stopPositionTestInternal(null, null, stop, tmpFile);
    }
  }

  private void stopPositionTestInternal(Long startOffset,
                                        Long endOffset,
                                        Long stopOffset,
                                        File tmpFile) throws Exception {
    String readWithoutSplit;
    String readWithSplit1, readWithSplit2;
    StringBuilder accumulatedRead = new StringBuilder();

    // Read from source without split attempts.
    TextSource<String> textSource = new TextSource<>(
        tmpFile.getPath(), false, startOffset, endOffset,
        StringUtf8Coder.of());

    try (TextSource<String>.TextFileIterator iterator =
        (TextSource<String>.TextFileIterator) textSource.iterator()) {
      while (iterator.hasNext()) {
        accumulatedRead.append((String) iterator.next());
      }
      readWithoutSplit = accumulatedRead.toString();
    }

    // Read the first half of the split.
    textSource = new TextSource<>(
        tmpFile.getPath(), false, startOffset, stopOffset,
        StringUtf8Coder.of());
    accumulatedRead = new StringBuilder();

    try (TextSource<String>.TextFileIterator iterator =
        (TextSource<String>.TextFileIterator) textSource.iterator()) {
      while (iterator.hasNext()) {
        accumulatedRead.append((String) iterator.next());
      }
      readWithSplit1 = accumulatedRead.toString();
    }

    // Read the second half of the split.
    textSource = new TextSource<>(
        tmpFile.getPath(), false, stopOffset, endOffset,
        StringUtf8Coder.of());
    accumulatedRead = new StringBuilder();

    try (TextSource<String>.TextFileIterator iterator =
        (TextSource<String>.TextFileIterator) textSource.iterator()) {
      while (iterator.hasNext()) {
        accumulatedRead.append((String) iterator.next());
      }
      readWithSplit2 = accumulatedRead.toString();
    }

    Assert.assertEquals(readWithoutSplit, readWithSplit1 + readWithSplit2);
  }

  private ApproximateProgress createApproximateProgress(
      com.google.api.services.dataflow.model.Position position) {
    return new ApproximateProgress().setPosition(position);
  }

  // TODO: sharded filenames
  // TODO: reading from GCS
}
