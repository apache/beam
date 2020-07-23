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
import static org.apache.beam.runners.dataflow.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.api.services.dataflow.model.ApproximateReportedProgress;
import com.google.api.services.dataflow.model.Source;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.dataflow.util.CloudObject;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader;
import org.apache.beam.runners.dataflow.worker.util.common.worker.NativeReader.DynamicSplitResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@code ConcatReader}. */
@RunWith(JUnit4.class)
public class ConcatReaderTest {
  private static final String READER_OBJECT = "reader_object";

  @Rule public ExpectedException expectedException = ExpectedException.none();

  private List<TestReader<?>> recordedReaders = new ArrayList<>();

  private ReaderRegistry registry;

  @Before
  public void setUp() {
    recordedReaders.clear();
    registry =
        ReaderRegistry.defaultRegistry()
            .register(TestReader.class.getName(), new TestReaderFactory());
  }

  /**
   * A {@link NativeReader} used for testing purposes. Delegates functionality to an underlying
   * {@link InMemoryReader}.
   */
  public class TestReader<T> extends NativeReader<T> {
    private final long recordToFailAt;
    private final boolean failWhenClosing;
    private TestIterator lastIterator = null;
    private final NativeReader<T> readerDelegator;

    /**
     * Create a TestReader.
     *
     * @param encodedElements list of elements read by the {@code Reader}
     * @param coder {@code Coder} to by used by the underlying {@code Reader}
     * @param recordToFailAt if non-negative, a {@code TestIterator} will fail throwing an {@code
     *     IOException} when trying to read the element at this index
     * @param failWhenClosing if {@code true}, a {@code TestIterator} will fail throwing an {@code
     *     IOException} when {@link TestIterator#close()} is invoked
     */
    public TestReader(
        List<String> encodedElements,
        Coder<T> coder,
        long recordToFailAt,
        boolean failWhenClosing) {
      this.recordToFailAt = recordToFailAt;
      this.failWhenClosing = failWhenClosing;
      readerDelegator = new InMemoryReader<>(encodedElements, 0, encodedElements.size(), coder);
      recordedReaders.add(this);
    }

    public boolean isClosedOrUnopened() {
      if (lastIterator != null) {
        return lastIterator.isClosed;
      }

      // A reader was not created
      return true;
    }

    @Override
    public NativeReaderIterator<T> iterator() throws IOException {
      lastIterator = new TestIterator(readerDelegator.iterator());
      return lastIterator;
    }

    private class TestIterator extends NativeReaderIterator<T> {
      private final NativeReaderIterator<T> iteratorImpl;
      private long currentIndex = -1;
      private boolean isClosed = false;

      private TestIterator(NativeReaderIterator<T> iteratorImpl) {
        this.iteratorImpl = iteratorImpl;
      }

      @Override
      public boolean start() throws IOException {
        currentIndex++;
        if (currentIndex == recordToFailAt) {
          throw new IOException("Failing at record " + currentIndex);
        }
        return iteratorImpl.start();
      }

      @Override
      public boolean advance() throws IOException {
        currentIndex++;
        if (currentIndex == recordToFailAt) {
          throw new IOException("Failing at record " + currentIndex);
        }
        return iteratorImpl.advance();
      }

      @Override
      public T getCurrent() throws NoSuchElementException {
        return iteratorImpl.getCurrent();
      }

      @Override
      public void close() throws IOException {
        isClosed = true;
        if (failWhenClosing) {
          throw new IOException("Failing when closing");
        }
        iteratorImpl.close();
      }

      @Override
      public Progress getProgress() {
        return iteratorImpl.getProgress();
      }

      @Override
      public DynamicSplitResult requestDynamicSplit(DynamicSplitRequest request) {
        throw new UnsupportedOperationException();
      }

      @Override
      public double getRemainingParallelism() {
        return Double.NaN;
      }
    }
  }

  private static class TestReaderFactory implements ReaderFactory {
    @Override
    public NativeReader<?> create(
        CloudObject spec,
        @Nullable Coder<?> coder,
        @Nullable PipelineOptions options,
        @Nullable DataflowExecutionContext executionContext,
        DataflowOperationContext operationContext)
        throws Exception {
      return (NativeReader<?>) spec.get(READER_OBJECT);
    }
  }

  private TestReader<String> createTestReader(
      long recordsPerReader,
      long recordToFailAt,
      boolean failWhenClosing,
      List<String> expectedData)
      throws Exception {
    List<String> records = new ArrayList<>();
    for (int i = 0; i < recordsPerReader; i++) {
      String record = "Record" + i;
      records.add(record);
      if (recordToFailAt < 0 || i < recordToFailAt) {
        expectedData.add(record);
      }
    }

    return new TestReader<String>(records, StringUtf8Coder.of(), recordToFailAt, failWhenClosing);
  }

  private static void assertAllOpenReadersClosed(List<TestReader<?>> readers) {
    for (TestReader<?> reader : readers) {
      if (!reader.isClosedOrUnopened()) {
        throw new AssertionError("At least one reader was not closed");
      }
    }
  }

  private Source createSourceForTestReader(TestReader<String> testReader) {
    Source source = new Source();
    CloudObject specObj = CloudObject.forClass(TestReader.class);
    specObj.put(READER_OBJECT, testReader);
    source.setSpec(specObj);
    return source;
  }

  private ConcatReader<String> createConcatReadersOfSizes(
      List<String> expected, int... recordsPerReader) throws Exception {
    List<Source> sourceList = new ArrayList<>();

    for (int items : recordsPerReader) {
      sourceList.add(
          createSourceForTestReader(
              createTestReader(
                  items /* recordsPerReader */,
                  -1 /* recordToFailAt */,
                  false /* failWhenClosing */,
                  expected)));
    }
    return new ConcatReader<>(
        registry,
        null /* options */,
        null /* executionContext */,
        TestOperationContext.create(),
        sourceList);
  }

  private void testReadersOfSizes(int... recordsPerReader) throws Exception {
    List<String> expected = new ArrayList<>();
    ConcatReader<String> concatReader = createConcatReadersOfSizes(expected, recordsPerReader);
    assertThat(readAllFromReader(concatReader), containsInAnyOrder(expected.toArray()));
    assertEquals(recordedReaders.size(), recordsPerReader.length);
    assertAllOpenReadersClosed(recordedReaders);
  }

  @Test
  public void testCreateFromNull() throws Exception {
    expectedException.expect(NullPointerException.class);
    new ConcatReader<String>(
        registry,
        null /* options */,
        null /* executionContext */,
        TestOperationContext.create(),
        null /* sources */);
  }

  @Test
  public void testReadEmptyList() throws Exception {
    ConcatReader<String> concat =
        new ConcatReader<>(
            registry,
            null /* options */,
            null /* executionContext */,
            TestOperationContext.create(),
            new ArrayList<Source>());
    ConcatReader.ConcatIterator<String> iterator = concat.iterator();
    assertNotNull(iterator);
    assertFalse(concat.iterator().start());

    expectedException.expect(NoSuchElementException.class);
    iterator.getCurrent();
  }

  @Test
  public void testReadOne() throws Exception {
    testReadersOfSizes(100);
  }

  @Test
  public void testReadMulti() throws Exception {
    testReadersOfSizes(10, 5, 20, 40);
  }

  @Test
  public void testReadFirstReaderEmpty() throws Exception {
    testReadersOfSizes(0, 5, 20, 40);
  }

  @Test
  public void testReadLastReaderEmpty() throws Exception {
    testReadersOfSizes(10, 5, 20, 0);
  }

  @Test
  public void testEmptyReaderBeforeNonEmptyReader() throws Exception {
    testReadersOfSizes(10, 0, 20, 30);
  }

  @Test
  public void testMultipleReadersAreEmpty() throws Exception {
    testReadersOfSizes(10, 0, 20, 0, 30, 0, 40);
  }

  @Test
  public void testAReaderFailsAtClose() throws Exception {
    List<String> expected = new ArrayList<>();
    List<Source> sources =
        Arrays.asList(
            createSourceForTestReader(
                createTestReader(
                    10 /* recordsPerReader */,
                    -1 /* recordToFailAt */,
                    false /* failWhenClosing */,
                    expected)),
            createSourceForTestReader(
                createTestReader(
                    10 /* recordsPerReader */,
                    -1 /* recordToFailAt */,
                    true /* failWhenClosing */,
                    expected)),
            createSourceForTestReader(
                createTestReader(
                    10 /* recordsPerReader */,
                    -1 /* recordToFailAt */,
                    false /* failWhenClosing */,
                    new ArrayList<String>())));

    ConcatReader<String> concatReader =
        new ConcatReader<>(
            registry,
            null /* options */,
            null /* executionContext */,
            TestOperationContext.create(),
            sources);
    try {
      readAllFromReader(concatReader);
      fail();
    } catch (IOException e) {
      assertEquals(3, recordedReaders.size());
      assertAllOpenReadersClosed(recordedReaders);
    }
  }

  @Test
  public void testReaderFailsAtRead() throws Exception {
    List<String> expected = new ArrayList<>();
    List<Source> sources =
        Arrays.asList(
            createSourceForTestReader(
                createTestReader(
                    10 /* recordsPerReader */,
                    -1 /* recordToFailAt */,
                    false /* failWhenClosing */,
                    expected)),
            createSourceForTestReader(
                createTestReader(
                    10 /* recordsPerReader */,
                    6 /* recordToFailAt */,
                    false /* failWhenClosing */,
                    expected)),
            createSourceForTestReader(
                createTestReader(
                    10 /* recordsPerReader */,
                    -1 /* recordToFailAt */,
                    false /* failWhenClosing */,
                    expected)));
    expected = expected.subList(0, 16);
    assertEquals(16, expected.size());

    ConcatReader<String> concatReader =
        new ConcatReader<>(
            registry,
            null /* options */,
            null /* executionContext */,
            TestOperationContext.create(),
            sources);
    try {
      readAllFromReader(concatReader);
      fail();
    } catch (IOException e) {
      assertEquals(3, recordedReaders.size());
      assertAllOpenReadersClosed(recordedReaders);
    }
  }

  private void runProgressTest(int... sizes) throws Exception {
    ConcatReader<String> concatReader = createConcatReadersOfSizes(new ArrayList<String>(), sizes);
    try (ConcatReader.ConcatIterator<String> iterator = concatReader.iterator()) {
      for (int readerIndex = 0; readerIndex < sizes.length; readerIndex++) {
        for (int recordIndex = 0; recordIndex < sizes[readerIndex]; recordIndex++) {
          if (readerIndex == 0 && recordIndex == 0) {
            iterator.start();
          } else {
            iterator.advance();
          }
          ApproximateReportedProgress progress =
              readerProgressToCloudProgress(iterator.getProgress());
          assertEquals(
              readerIndex, progress.getPosition().getConcatPosition().getIndex().intValue());
        }
      }
    }
  }

  @Test
  public void testGetProgressSingle() throws Exception {
    runProgressTest(10);
  }

  @Test
  public void testGetProgressSameSize() throws Exception {
    runProgressTest(10, 10, 10);
  }

  @Test
  public void testGetProgressDifferentSizes() throws Exception {
    runProgressTest(10, 30, 20, 15, 7);
  }

  // This is an exhaustive test for method ConcatIterator#splitAtPosition.
  // Given an array of reader sizes of length 's' this method exhaustively create ConcatReaders that
  // have read up to every possible position. For each position 'p', this method creates a set of
  // ConcatReaders of size 's+1' that have read up to position 'p' and tests splitting those
  // ConcatReaders for index positions in the range [0, s].
  public void runUpdateStopPositionTest(int... readerSizes) throws Exception {
    ConcatReader<String> concatReader =
        createConcatReadersOfSizes(new ArrayList<String>(), readerSizes);

    // This includes indexToSplit == sizes.length case to test out of range split requests.
    for (int indexToSplit = 0; indexToSplit <= readerSizes.length; indexToSplit++) {
      int recordsToRead = -1; // Number of records to read from the ConcatReader before splitting.
      for (int readerIndex = 0; readerIndex < readerSizes.length; readerIndex++) {
        for (int recordIndex = 0; recordIndex <= readerSizes[readerIndex]; recordIndex++) {
          if (readerIndex > 0 && recordIndex == 0) {
            // This is an invalid state as far as ConcatReader is concerned.
            // When we have not read any records from the reader at 'readerIndex', current reader
            // should be the reader at 'readerIndex - 1'.
            continue;
          }

          recordsToRead++;

          NativeReader.NativeReaderIterator<String> iterator = concatReader.iterator();
          for (int i = 0; i < recordsToRead; i++) {
            if (i == 0) {
              iterator.start();
            } else {
              iterator.advance();
            }
          }

          DynamicSplitResult splitResult =
              iterator.requestDynamicSplit(
                  ReaderTestUtils.splitRequestAtConcatPosition(indexToSplit, null));

          // We will not be able to successfully perform the request to dynamically split (and hence
          // splitResult will be null) in following cases.
          // * recordsToRead == 0 - ConcatReader has not started reading
          // * readerIndex >= indexToSplit - ConcatReader has already read at least one record from
          //   reader proposed in the split request.
          // * indexToSplit < 0 || indexToSplit >= sizes.length - split position is out of range

          if ((recordsToRead == 0)
              || (readerIndex >= indexToSplit)
              || (indexToSplit < 0 || indexToSplit >= readerSizes.length)) {
            assertNull(splitResult);
          } else {
            Assert.assertEquals(
                indexToSplit,
                ReaderTestUtils.positionFromSplitResult(splitResult)
                    .getConcatPosition()
                    .getIndex()
                    .intValue());
          }
        }
      }
    }
  }

  @Test
  public void testUpdateStopPositionSingle() throws Exception {
    runUpdateStopPositionTest(10);
  }

  @Test
  public void testUpdateStopPositionSameSize() throws Exception {
    runUpdateStopPositionTest(10, 10, 10);
  }

  @Test
  public void testUpdateStopPositionDifferentSizes() throws Exception {
    runUpdateStopPositionTest(10, 30, 20, 15, 7);
  }
}
