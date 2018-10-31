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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.worker.ReaderUtils.readAllFromReader;
import static org.apache.beam.runners.dataflow.worker.ReaderUtils.readNItemsFromUnstartedIterator;
import static org.apache.beam.runners.dataflow.worker.ReaderUtils.readRemainingFromIterator;
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.readerProgressToCloudProgress;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import java.io.File;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.beam.runners.dataflow.worker.AvroByteReader.AvroByteFileIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutorTestUtils;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.DynamicSplitResult;
import org.apache.beam.sdk.TestUtils;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.MimeTypes;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for AvroByteReader. */
@RunWith(JUnit4.class)
public class AvroByteReaderTest {
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  /** Class representing information about an Avro file generated from a list of elements. */
  private static class AvroFileInfo<T> {
    String filename;
    List<Integer> elementSizes = new ArrayList<>();
    List<Long> syncPoints = new ArrayList<>();
    long totalElementEncodedSize = 0;
  }

  /** Write input elements to a file and return information about the Avro-encoded file. */
  private <T> AvroFileInfo<T> initInputFile(List<List<T>> elemsList, Coder<T> coder)
      throws Exception {
    File tmpFile = tmpFolder.newFile("file.avro");
    AvroFileInfo<T> fileInfo = new AvroFileInfo<>();
    fileInfo.filename = tmpFile.getPath();

    // Write the data.
    OutputStream outStream =
        Channels.newOutputStream(
            FileSystems.create(
                FileSystems.matchNewResource(fileInfo.filename, false), MimeTypes.BINARY));
    Schema schema = Schema.create(Schema.Type.BYTES);
    DatumWriter<ByteBuffer> datumWriter = new GenericDatumWriter<>(schema);
    try (DataFileWriter<ByteBuffer> fileWriter = new DataFileWriter<>(datumWriter)) {
      fileWriter.create(schema, outStream);
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
          byte[] encodedElement = CoderUtils.encodeToByteArray(coder, elem);
          fileWriter.append(ByteBuffer.wrap(encodedElement));
          fileInfo.elementSizes.add(encodedElement.length);
          fileInfo.totalElementEncodedSize += encodedElement.length;
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
  private <T> void runTestRead(List<List<T>> elemsList, Coder<T> coder, boolean requireExactMatch)
      throws Exception {
    AvroFileInfo<T> fileInfo = initInputFile(elemsList, coder);

    // Test reading the data back.
    List<List<T>> actualElemsList = new ArrayList<>();
    List<Integer> actualSizes = new ArrayList<>();
    long startOffset = 0;
    long endOffset;
    long prevSyncPoint = 0;
    for (long syncPoint : fileInfo.syncPoints) {
      endOffset = (prevSyncPoint + syncPoint) / 2;
      actualElemsList.add(readElems(fileInfo.filename, startOffset, endOffset, coder, actualSizes));
      startOffset = endOffset;
      prevSyncPoint = syncPoint;
    }
    actualElemsList.add(
        readElems(fileInfo.filename, startOffset, Long.MAX_VALUE, coder, actualSizes));

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

  private <T> List<T> readElems(
      String filename, long startOffset, long endOffset, Coder<T> coder, List<Integer> actualSizes)
      throws Exception {
    AvroByteReader<T> avroReader =
        new AvroByteReader<>(
            filename, startOffset, endOffset, coder, PipelineOptionsFactory.create());
    new ExecutorTestUtils.TestReaderObserver(avroReader, actualSizes);
    return readAllFromReader(avroReader);
  }

  @Test
  public void testRead() throws Exception {
    runTestRead(
        Collections.singletonList(TestUtils.INTS),
        BigEndianIntegerCoder.of(),
        true /* require exact match */);
  }

  @Test
  public void testReadEmpty() throws Exception {
    runTestRead(
        Collections.singletonList(TestUtils.NO_INTS),
        BigEndianIntegerCoder.of(),
        true /* require exact match */);
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
    runTestRead(
        generateInputBlocks(3, 50, 5), StringUtf8Coder.of(), true /* require exact match */);
  }

  @Test
  public void testReadBigRanges() throws Exception {
    runTestRead(
        generateInputBlocks(10, 128 * 1024, 100),
        StringUtf8Coder.of(),
        false /* don't require exact match */);
  }

  // Verification behavior for split requests. Used for testRequestDynamicSplitInternal.
  private static enum SplitVerificationBehavior {
    VERIFY_SUCCESS, // Split request must succeed.
    VERIFY_FAILURE, // Split request must fail.
    DO_NOT_VERIFY; // Perform no verification.
  }

  private <T> void testRequestDynamicSplitInternal(
      AvroByteReader<T> reader,
      float splitAtFraction,
      int readBeforeSplit,
      SplitVerificationBehavior splitVerificationBehavior)
      throws Exception {
    // Read all elements from the reader
    Long endOffset = reader.endPosition;
    List<T> expectedElements = readAllFromReader(reader);

    List<T> primaryElements;
    List<T> residualElements = new ArrayList<>();
    try (AvroByteReader<T>.AvroByteFileIterator iterator = reader.iterator()) {
      // Read n elements from the reader
      primaryElements = readNItemsFromUnstartedIterator(iterator, readBeforeSplit);

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
      primaryElements.addAll(readRemainingFromIterator(iterator, readBeforeSplit > 0));

      if (splitResult != null) {
        Long splitPosition = ReaderTestUtils.positionFromSplitResult(splitResult).getByteOffset();
        AvroByteReader<T> residualReader =
            new AvroByteReader<T>(
                reader.avroSource.getFileOrPatternSpec(),
                splitPosition,
                endOffset,
                reader.coder,
                reader.options);
        // Read from the residual until it is complete.
        residualElements = readAllFromReader(residualReader);
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
    Coder<String> coder = StringUtf8Coder.of();
    AvroFileInfo<String> fileInfo = initInputFile(elements, coder);
    AvroByteReader<String> reader =
        new AvroByteReader<String>(
            fileInfo.filename, 0L, Long.MAX_VALUE, coder, PipelineOptionsFactory.create());
    // Read most of the records before the proposed split point.
    testRequestDynamicSplitInternal(reader, 0.5F, 490, SplitVerificationBehavior.VERIFY_SUCCESS);
    // Read a single record.
    testRequestDynamicSplitInternal(reader, 0.5F, 1, SplitVerificationBehavior.VERIFY_SUCCESS);
    // Read zero records.
    testRequestDynamicSplitInternal(reader, 0.5F, 0, SplitVerificationBehavior.VERIFY_FAILURE);
    // Read almost the entire input.
    testRequestDynamicSplitInternal(reader, 0.5F, 900, SplitVerificationBehavior.VERIFY_FAILURE);
    // Read the entire input.
    testRequestDynamicSplitInternal(reader, 0.5F, 2000, SplitVerificationBehavior.VERIFY_FAILURE);
  }

  @Test
  public void testRequestDynamicSplitExhaustive() throws Exception {
    List<List<String>> elements = generateInputBlocks(5, 10 * 10, 10);
    Coder<String> coder = StringUtf8Coder.of();
    AvroFileInfo<String> fileInfo = initInputFile(elements, coder);
    AvroByteReader<String> reader =
        new AvroByteReader<String>(
            fileInfo.filename, 0L, Long.MAX_VALUE, coder, PipelineOptionsFactory.create());
    for (float splitFraction = 0.0F; splitFraction < 1.0F; splitFraction += 0.02F) {
      for (int recordsToRead = 0; recordsToRead <= 500; recordsToRead += 5) {
        testRequestDynamicSplitInternal(
            reader, splitFraction, recordsToRead, SplitVerificationBehavior.DO_NOT_VERIFY);
      }
    }
  }

  @Test
  public void testGetProgress() throws Exception {
    // Ensure that AvroByteReader reports progress from the underlying AvroSource.

    // 4 blocks with 4 split points.
    List<List<String>> elements = generateInputBlocks(4, 10, 10);
    Coder<String> coder = StringUtf8Coder.of();
    AvroFileInfo<String> fileInfo = initInputFile(elements, coder);
    AvroByteReader<String> reader =
        new AvroByteReader<String>(
            fileInfo.filename, 0L, Long.MAX_VALUE, coder, PipelineOptionsFactory.create());

    AvroByteFileIterator iterator = reader.iterator();
    Assert.assertTrue(iterator.start());
    ApproximateReportedProgress progress =
        readerProgressToCloudProgress(reader.iterator().getProgress());
    Assert.assertEquals(0.0, progress.getConsumedParallelism().getValue(), 1e-6);
    Assert.assertEquals(0.0, progress.getFractionConsumed(), 1e-6);
    Assert.assertNull(progress.getRemainingParallelism());

    Assert.assertTrue(iterator.advance());
    progress = readerProgressToCloudProgress(iterator.getProgress());
    Assert.assertEquals(1.0, progress.getConsumedParallelism().getValue(), 1e-6);
    Assert.assertNull(progress.getRemainingParallelism());

    // Advance to the end of last block and check consumed parallelism along the way.
    Assert.assertTrue(iterator.advance());
    progress = readerProgressToCloudProgress(iterator.getProgress());
    Assert.assertEquals(2.0, progress.getConsumedParallelism().getValue(), 1e-6);
    Assert.assertNull(progress.getRemainingParallelism());

    Assert.assertTrue(iterator.advance());
    progress = readerProgressToCloudProgress(iterator.getProgress());
    Assert.assertEquals(3.0, progress.getConsumedParallelism().getValue(), 1e-6);
    Assert.assertEquals(1.0, progress.getRemainingParallelism().getValue(), 1e-6);

    Assert.assertFalse(iterator.advance());
    progress = readerProgressToCloudProgress(iterator.getProgress());
    Assert.assertEquals(4.0, progress.getConsumedParallelism().getValue(), 1e-6);
    Assert.assertEquals(0.0, progress.getRemainingParallelism().getValue(), 1e-6);
    Assert.assertEquals(1.0, progress.getFractionConsumed(), 1e-6);
  }

  // TODO: sharded filenames
  // TODO: reading from GCS
}
