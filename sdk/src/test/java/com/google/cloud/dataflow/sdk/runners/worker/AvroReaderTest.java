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

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.isA;

import com.google.cloud.dataflow.sdk.TestUtils;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.MimeTypes;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader.DynamicSplitResult;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader.Progress;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import javax.annotation.Nullable;

/**
 * Tests for AvroReader.
 */
@RunWith(JUnit4.class)
public class AvroReaderTest {
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  /** Class representing information about an Avro file generated from a list of elements. */
  private static class AvroFileInfo<T> {
    String filename;
    List<Integer> elementSizes = new ArrayList<>();
    List<Long> syncPoints = new ArrayList<>();
    long totalElementEncodedSize = 0;
  }

  /** Write input elements to a file and return information about the Avro-encoded file. */
  private <T> AvroFileInfo<T> initInputFile(List<List<T>> elemsList, AvroCoder<T> coder)
      throws Exception {
    File tmpFile = tmpFolder.newFile("file.avro");
    AvroFileInfo<T> fileInfo = new AvroFileInfo<>();
    fileInfo.filename = tmpFile.getPath();

    // Write the data.
    OutputStream outStream =
        Channels.newOutputStream(IOChannelUtils.create(fileInfo.filename, MimeTypes.BINARY));
    DatumWriter<T> datumWriter = coder.createDatumWriter();
    try (DataFileWriter<T> fileWriter = new DataFileWriter<>(datumWriter)) {
      fileWriter.create(coder.getSchema(), outStream);
      boolean first = true;
      for (List<T> elems : elemsList) {
        if (first) {
          first = false;
        } else {
          // Ensure a block boundary here.
          long syncPoint = fileWriter.sync();
          fileInfo.syncPoints.add(syncPoint);
        }
        for (T elem : elems) {
          fileWriter.append(elem);
          fileInfo.elementSizes.add(CoderUtils.encodeToByteArray(coder, elem).length);
          fileInfo.totalElementEncodedSize += CoderUtils.encodeToByteArray(coder, elem).length;
        }
      }
    }

    return fileInfo;
  }

  /**
   * Reads from a file generated from a collection of elements and verifies that the elements read
   * are the same as the elements written.
   *
   * @param elemsList a list of blocks of elements, each of which is as a list of elements.
   * @param coder the coder used to encode the elements
   * @param requireExactMatch if true, each block must match exactly
   * @throws Exception
   */
  private <T> void runTestRead(
      List<List<T>> elemsList, AvroCoder<T> coder, boolean requireExactMatch) throws Exception {
    // Initialize the input file.
    AvroFileInfo<T> fileInfo = initInputFile(elemsList, coder);

    // Test reading the data back.
    List<List<T>> actualElemsList = new ArrayList<>();
    List<Integer> actualSizes = new ArrayList<>();
    Long startOffset = null;
    Long endOffset;
    long prevSyncPoint = 0;
    for (long syncPoint : fileInfo.syncPoints) {
      endOffset = (prevSyncPoint + syncPoint) / 2;
      actualElemsList.add(readElems(fileInfo.filename, startOffset, endOffset, coder, actualSizes));
      startOffset = endOffset;
      prevSyncPoint = syncPoint;
    }
    actualElemsList.add(readElems(fileInfo.filename, startOffset, null, coder, actualSizes));

    // Compare the expected and the actual elements.
    if (requireExactMatch) {
      // Require the blocks to match exactly.  (This works only for
      // small block sizes.  Large block sizes, bigger than Avro's
      // internal sizes, lead to different splits.)
      Assert.assertEquals(elemsList, actualElemsList);
    } else {
      // Just require the overall elements to be the same.  (This
      // works for any block size.)
      List<T> expected = new ArrayList<>();
      for (List<T> elems : elemsList) {
        expected.addAll(elems);
      }
      List<T> actual = new ArrayList<>();
      for (List<T> actualElems : actualElemsList) {
        actual.addAll(actualElems);
      }
      Assert.assertEquals(expected, actual);
    }

    long actualTotalSize = 0;
    for (int elemSize : actualSizes) {
      actualTotalSize += elemSize;
    }
    Assert.assertEquals(fileInfo.totalElementEncodedSize, actualTotalSize);
  }

  private <T> List<T> readElems(String filename, @Nullable Long startOffset,
      @Nullable Long endOffset, AvroCoder<T> coder, List<Integer> actualSizes) throws Exception {
    AvroReader<T> avroReader = new AvroReader<>(filename, startOffset, endOffset, coder, null);
    new ExecutorTestUtils.TestReaderObserver(avroReader, actualSizes);

    float progressReported = 0;
    List<T> actualElems = new ArrayList<>();
    try (Reader.ReaderIterator<WindowedValue<T>> iterator = avroReader.iterator()) {
      while (iterator.hasNext()) {
        actualElems.add(iterator.next().getValue());
        float progress = 0.0f;
        Progress readerProgress = iterator.getProgress();
        if (readerProgress != null) {
          progress = readerProgressToCloudProgress(iterator.getProgress()).getPercentComplete();
        }
        // Make sure that the reported progress is monotonous.
        Assert.assertThat(progress, greaterThanOrEqualTo(progressReported));
        progressReported = progress;
      }
    }
    return actualElems;
  }

  // Verification behavior for split requests. Used for testRequestDynamicSplitInternal.
  private static enum SplitVerificationBehavior {
    VERIFY_SUCCESS, // Split request must succeed.
    VERIFY_FAILURE, // Split request must fail.
    DO_NOT_VERIFY; // Perform no verification.
  }

  private <T> void testRequestDynamicSplitInternal(AvroReader<T> reader, float splitAtFraction,
      long readBeforeSplit, SplitVerificationBehavior splitVerificationBehavior) throws Exception {
    // Read all elements from the reader
    List<T> expectedElements = new ArrayList<>();
    Long endOffset = reader.endPosition;
    List<WindowedValue<T>> windowedValues = new ArrayList<>();
    ReaderTestUtils.readRemainingFromReader(reader, windowedValues);
    ReaderTestUtils.windowedValuesToValues(windowedValues, expectedElements);


    List<T> primaryElements = new ArrayList<>();
    List<T> residualElements = new ArrayList<>();
    try (Reader.ReaderIterator<WindowedValue<T>> iterator = reader.iterator()) {
      // Read n elements from the reader
      windowedValues.clear();
      ReaderTestUtils.readAtMostNElementsFromIterator(iterator, readBeforeSplit, windowedValues);
      ReaderTestUtils.windowedValuesToValues(windowedValues, primaryElements);

      // Request a split at the specified position
      DynamicSplitResult splitResult =
          iterator.requestDynamicSplit(ReaderTestUtils.splitRequestAtFraction(splitAtFraction));

      switch (splitVerificationBehavior) {
        case VERIFY_SUCCESS:
          Assert.assertNotNull(splitResult);
          break;
        case VERIFY_FAILURE:
          Assert.assertNull(splitResult);
          break;
        case DO_NOT_VERIFY:
      }

      // Finish reading from the original reader.
      windowedValues.clear();
      ReaderTestUtils.readRemainingFromIterator(iterator, windowedValues);
      ReaderTestUtils.windowedValuesToValues(windowedValues, primaryElements);

      if (splitResult != null) {
        Long splitPosition = ReaderTestUtils.positionFromSplitResult(splitResult).getByteOffset();
        AvroReader<T> residualReader = new AvroReader<T>(reader.avroSource.getFileOrPatternSpec(),
            splitPosition, endOffset, reader.avroCoder, reader.options);
        // Read from the residual until it is complete.
        windowedValues.clear();
        ReaderTestUtils.readRemainingFromReader(residualReader, windowedValues);
        ReaderTestUtils.windowedValuesToValues(windowedValues, residualElements);
      }
    }

    primaryElements.addAll(residualElements);
    Assert.assertEquals(expectedElements, primaryElements);
    if (splitVerificationBehavior == SplitVerificationBehavior.VERIFY_SUCCESS) {
      Assert.assertNotEquals(0, residualElements.size());
    }
  }

  @Test
  public void testRequestDynamicSplit() throws Exception {
    // Note that exhaustive tests for AvroSource's split behavior exist in {@link AvroSourceTest}.
    List<List<String>> elements = generateInputBlocks(10, 100 * 100, 100);
    AvroCoder<String> coder = AvroCoder.of(String.class);
    AvroFileInfo<String> fileInfo = initInputFile(elements, coder);
    AvroReader<String> reader = new AvroReader<String>(fileInfo.filename, null, null, coder, null);
    // Read most of the records before the proposed split point.
    testRequestDynamicSplitInternal(reader, 0.5F, 490L, SplitVerificationBehavior.VERIFY_SUCCESS);
    // Read a single record.
    testRequestDynamicSplitInternal(reader, 0.5F, 1L, SplitVerificationBehavior.VERIFY_SUCCESS);
    // Read zero records.
    testRequestDynamicSplitInternal(reader, 0.5F, 0L, SplitVerificationBehavior.VERIFY_FAILURE);
    // Read almost the entire input.
    testRequestDynamicSplitInternal(reader, 0.5F, 900L, SplitVerificationBehavior.VERIFY_FAILURE);
    // Read the entire input.
    testRequestDynamicSplitInternal(reader, 0.5F, 2000L, SplitVerificationBehavior.VERIFY_FAILURE);
  }

  @Test
  public void testRequestDynamicSplitExhaustive() throws Exception {
    List<List<String>> elements = generateInputBlocks(5, 10 * 10, 10);
    AvroCoder<String> coder = AvroCoder.of(String.class);
    AvroFileInfo<String> fileInfo = initInputFile(elements, coder);
    AvroReader<String> reader = new AvroReader<String>(fileInfo.filename, null, null, coder, null);
    for (float splitFraction = 0.0F; splitFraction < 1.0F; splitFraction += 0.02F) {
      for (long recordsToRead = 0L; recordsToRead <= 500; recordsToRead += 5) {
        testRequestDynamicSplitInternal(
            reader, splitFraction, recordsToRead, SplitVerificationBehavior.DO_NOT_VERIFY);
      }
    }
  }

  @Test
  public void testRead() throws Exception {
    runTestRead(Collections.singletonList(TestUtils.INTS), AvroCoder.of(Integer.class),
        true/* require exact match */);
  }

  @Test
  public void testReadEmpty() throws Exception {
    runTestRead(Collections.singletonList(TestUtils.NO_INTS), AvroCoder.of(Integer.class),
        true/* require exact match */);
  }

  private List<List<String>> generateInputBlocks(
      int numBlocks, int blockSizeBytes, int averageLineSizeBytes) {
    Random random = new Random(0);
    List<List<String>> blocks = new ArrayList<>(numBlocks);
    for (int blockNum = 0; blockNum < numBlocks; blockNum++) {
      int numLines = blockSizeBytes / averageLineSizeBytes;
      List<String> lines = new ArrayList<>(numLines);
      for (int lineNum = 0; lineNum < numLines; lineNum++) {
        int numChars = random.nextInt(averageLineSizeBytes * 2);
        StringBuilder sb = new StringBuilder();
        for (int charNum = 0; charNum < numChars; charNum++) {
          sb.appendCodePoint(random.nextInt('z' - 'a' + 1) + 'a');
        }
        lines.add(sb.toString());
      }
      blocks.add(lines);
    }
    return blocks;
  }

  @Test
  public void testReadSmallRanges() throws Exception {
    runTestRead(generateInputBlocks(3, 50, 5), AvroCoder.of(String.class),
        true/* require exact match */);
  }

  @Test
  public void testReadBigRanges() throws Exception {
    runTestRead(generateInputBlocks(10, 128 * 1024, 100), AvroCoder.of(String.class),
        false/* don't require exact match */);
  }

  @Test
  public void testErrorOnFileNotFound() throws Exception {
    expectedException.expectCause(isA(FileNotFoundException.class));
    readElems("file-not-found", 0L, 100L, AvroCoder.of(String.class), new ArrayList<Integer>());
  }

  @Test
  public void testErrorOnMultipleFiles() throws Exception {
    File file1 = tmpFolder.newFile("foo1.avro");
    File file2 = tmpFolder.newFile("foo2.avro");
    Channels.newOutputStream(IOChannelUtils.create(file1.getPath(), MimeTypes.BINARY)).close();
    Channels.newOutputStream(IOChannelUtils.create(file2.getPath(), MimeTypes.BINARY)).close();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("More than 1 file matched");
    readElems(new File(tmpFolder.getRoot(), "*").getPath(), 0L, 100L,
        AvroCoder.of(String.class), new ArrayList<Integer>());
  }

  // TODO: sharded filenames
  // TODO: reading from GCS
}
