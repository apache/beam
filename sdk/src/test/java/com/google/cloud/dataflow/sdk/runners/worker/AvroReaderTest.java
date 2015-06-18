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

import com.google.cloud.dataflow.sdk.TestUtils;
import com.google.cloud.dataflow.sdk.coders.AvroCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.IOChannelUtils;
import com.google.cloud.dataflow.sdk.util.MimeTypes;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

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

  private <T> void runTestRead(
      List<List<T>> elemsList, AvroCoder<T> coder, boolean requireExactMatch) throws Exception {
    File tmpFile = tmpFolder.newFile("file.avro");
    String filename = tmpFile.getPath();

    // Write the data.
    OutputStream outStream =
        Channels.newOutputStream(IOChannelUtils.create(filename, MimeTypes.BINARY));
    DatumWriter<T> datumWriter = coder.createDatumWriter();
    List<Long> syncPoints = new ArrayList<>();
    List<Integer> expectedSizes = new ArrayList<>();
    try (DataFileWriter<T> fileWriter = new DataFileWriter<>(datumWriter)) {
      fileWriter.create(coder.getSchema(), outStream);
      boolean first = true;
      for (List<T> elems : elemsList) {
        if (first) {
          first = false;
        } else {
          // Ensure a block boundary here.
          long syncPoint = fileWriter.sync();
          syncPoints.add(syncPoint);
        }
        for (T elem : elems) {
          fileWriter.append(elem);
          expectedSizes.add(CoderUtils.encodeToByteArray(coder, elem).length);
        }
      }
    }

    // Test reading the data back.
    List<List<T>> actualElemsList = new ArrayList<>();
    List<Integer> actualSizes = new ArrayList<>();
    Long startOffset = null;
    Long endOffset;
    long prevSyncPoint = 0;
    for (long syncPoint : syncPoints) {
      endOffset = (prevSyncPoint + syncPoint) / 2;
      actualElemsList.add(readElems(filename, startOffset, endOffset, coder, actualSizes));
      startOffset = endOffset;
      prevSyncPoint = syncPoint;
    }
    actualElemsList.add(readElems(filename, startOffset, null, coder, actualSizes));

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

    Assert.assertEquals(expectedSizes, actualSizes);
  }

  private <T> List<T> readElems(String filename, @Nullable Long startOffset,
      @Nullable Long endOffset, Coder<T> coder, List<Integer> actualSizes) throws Exception {
    AvroReader<T> avroReader =
        new AvroReader<>(filename, startOffset, endOffset, WindowedValue.getValueOnlyCoder(coder));
    new ExecutorTestUtils.TestReaderObserver(avroReader, actualSizes);

    long offsetReported = 0;
    List<T> actualElems = new ArrayList<>();
    try (Reader.ReaderIterator<WindowedValue<T>> iterator = avroReader.iterator()) {
      while (iterator.hasNext()) {
        actualElems.add(iterator.next().getValue());
        long progress =
            readerProgressToCloudProgress(iterator.getProgress()).getPosition().getByteOffset();
        // Make sure that the reported progress is monotonous.
        Assert.assertThat(progress, greaterThanOrEqualTo(offsetReported));
        offsetReported = progress;
      }
    }
    return actualElems;
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
    runTestRead(
        generateInputBlocks(3, 50, 5), AvroCoder.of(String.class), true/* require exact match */);
  }

  @Test
  public void testReadBigRanges() throws Exception {
    runTestRead(generateInputBlocks(10, 128 * 1024, 100), AvroCoder.of(String.class),
        false/* don't require exact match */);
  }

  @Test
  public void testErrorOnFileNotFound() throws Exception {
    expectedException.expect(FileNotFoundException.class);
    readElems("file-not-found", 0L, 100L, AvroCoder.of(String.class), new ArrayList<Integer>());
  }

  @Test
  public void testErrorOnMultipleFiles() throws Exception {
    File file1 = tmpFolder.newFile("foo1.avro");
    File file2 = tmpFolder.newFile("foo2.avro");
    Channels.newOutputStream(IOChannelUtils.create(file1.getPath(), MimeTypes.BINARY)).close();
    Channels.newOutputStream(IOChannelUtils.create(file2.getPath(), MimeTypes.BINARY)).close();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("more than 1 file matched");
    readElems(new File(tmpFolder.getRoot(), "*").getPath(), 0L, 100L,
        AvroCoder.of(String.class), new ArrayList<Integer>());
  }

  // TODO: sharded filenames
  // TODO: reading from GCS
}
