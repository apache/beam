/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.runners.worker;

import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.readFully;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.api.services.dataflow.model.Source;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.ExecutionContext;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader.ReaderIterator;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests for {@code ConcatReader}.
 */
@RunWith(JUnit4.class)
public class ConcatReaderTest {
  private static final String READER_OBJECT = "reader_object";

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private List<TestReader<?>> recordedReaders = new ArrayList<>();

  @Before
  public void setUp() {
    recordedReaders.clear();
  }

  /**
   * A {@code Reader} used for testing purposes. Delegates functionality to an underlying {@link
   * InMemoryReader}.
   */
  public class TestReader<T> extends Reader<T> {
    private final long recordToFailAt;
    private final boolean failWhenClosing;
    private TestIterator<T> lastIterator = null;
    private final Reader<T> readerDelegator;

    /**
     * Create a TestReader.
     *
     * @param encodedElements list of elements read by the {@code Reader}
     * @param coder {@code Coder} to by used by the underlying {@code Reader}
     * @param recordToFailAt if non-negative, a {@code TestIterator} will fail throwing an {@code
     * IOException} when trying to read the element at this index
     * @param failWhenClosing if {@code true}, a {@code TestIterator} will fail throwing an {@code
     * IOException} when {@link TestIterator#close()} is invoked
     */
    public TestReader(List<String> encodedElements, Coder<T> coder, long recordToFailAt,
        boolean failWhenClosing) {
      this.recordToFailAt = recordToFailAt;
      this.failWhenClosing = failWhenClosing;
      readerDelegator =
          new InMemoryReader<>(encodedElements, 0L, (long) encodedElements.size(), coder);
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
    public ReaderIterator<T> iterator() throws IOException {
      lastIterator = new TestIterator<T>(readerDelegator.iterator());
      return lastIterator;
    }

    private class TestIterator<T> implements ReaderIterator<T> {
      private final ReaderIterator<T> iteratorImpl;
      private long currentRecord;
      private boolean isClosed = false;

      private TestIterator(ReaderIterator<T> iteratorImpl) {
        this.iteratorImpl = iteratorImpl;
      }

      @Override
      public boolean hasNext() throws IOException {
        return iteratorImpl.hasNext();
      }

      @Override
      public T next() throws IOException, NoSuchElementException {
        if (currentRecord == recordToFailAt) {
          throw new IOException("Failing at record " + currentRecord);
        }
        currentRecord++;
        return iteratorImpl.next();
      }

      @Override
      public ReaderIterator<T> copy() throws IOException {
        return new TestIterator<T>(iteratorImpl.copy());
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
    }
  }

  // This create method was defined outside "TestReader" since a static method has to be in a static
  // or a top level class. "TestReader" was not defined static since it needs to have access to the
  // "recordedReaders" instance variable.
  static <T> TestReader<T> create(@SuppressWarnings("unused") PipelineOptions options,
      CloudObject spec, @SuppressWarnings("unused") Coder<T> coder,
      @SuppressWarnings("unused") ExecutionContext executionContext) throws Exception {
    @SuppressWarnings("unchecked")
    TestReader<T> reader = (TestReader<T>) spec.get(READER_OBJECT);
    return reader;
  }

  private TestReader<String> createTestReader(long recordsPerReader, long recordToFailAt,
      boolean failWhenClosing, List<String> expectedData) throws Exception {
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
    CloudObject specObj = CloudObject.forClass(ConcatReaderTest.class);
    specObj.put(READER_OBJECT, testReader);
    source.setSpec(specObj);
    return source;
  }

  private void testReadersOfSizes(int... recordsPerReader) throws Exception {
    List<Source> sourceList = new ArrayList<>();
    List<String> expected = new ArrayList<>();
    for (int items : recordsPerReader) {
      sourceList.add(createSourceForTestReader(createTestReader(items/* recordsPerReader */,
          -1/* recordToFailAt */, false/* failWhenClosing */, expected)));
    }
    ConcatReader<String> concatReader = new ConcatReader<>(null, null, sourceList);

    List<String> actual = new ArrayList<>();
    readFully(concatReader, actual);
    assertThat(actual, containsInAnyOrder(expected.toArray()));
    assertEquals(recordedReaders.size(), recordsPerReader.length);
    assertAllOpenReadersClosed(recordedReaders);
  }

  @Test
  public void testCreateFromNull() throws Exception {
    expectedException.expect(NullPointerException.class);
    new ConcatReader<String>(null, null, null);
  }

  @Test
  public void testReadEmptyList() throws Exception {
    ConcatReader<String> concat = new ConcatReader<>(null, null, new ArrayList<Source>());
    ReaderIterator<String> iterator = concat.iterator();
    assertNotNull(iterator);
    assertFalse(concat.iterator().hasNext());

    expectedException.expect(NoSuchElementException.class);
    iterator.next();
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
    List<Source> sources = Arrays.asList(
        createSourceForTestReader(createTestReader(10 /* recordsPerReader */,
            -1 /* recordToFailAt */, false /* failWhenClosing */, expected)),
        createSourceForTestReader(createTestReader(10 /* recordsPerReader */,
            -1 /* recordToFailAt */, true /* failWhenClosing */, expected)),
        createSourceForTestReader(createTestReader(10/* recordsPerReader */, -1/* recordToFailAt */,
            false/* failWhenClosing */, new ArrayList<String>())));

    ConcatReader<String> concatReader = new ConcatReader<>(null, null, sources);
    List<String> actual = new ArrayList<>();
    try {
      readFully(concatReader, actual);
      fail();
    } catch (IOException e) {
      assertThat(actual, containsInAnyOrder(expected.toArray()));
      assertEquals(3, recordedReaders.size());
      assertAllOpenReadersClosed(recordedReaders);
    }
  }

  @Test
  public void testReaderFailsAtRead() throws Exception {
    List<String> expected = new ArrayList<>();
    List<Source> sources = Arrays.asList(
        createSourceForTestReader(createTestReader(10 /* recordsPerReader */,
            -1 /* recordToFailAt */, false /* failWhenClosing */, expected)),
        createSourceForTestReader(createTestReader(10 /* recordsPerReader */,
            6 /* recordToFailAt */, false /* failWhenClosing */, expected)),
        createSourceForTestReader(createTestReader(10 /* recordsPerReader */,
            -1 /* recordToFailAt */, false /* failWhenClosing */, expected)));
    expected = expected.subList(0, 16);
    assertEquals(16, expected.size());

    ConcatReader<String> concatReader = new ConcatReader<>(null, null, sources);
    List<String> actual = new ArrayList<>();
    try {
      readFully(concatReader, actual);
      fail();
    } catch (IOException e) {
      assertThat(actual, containsInAnyOrder(expected.toArray()));

      assertEquals(3, recordedReaders.size());
      assertAllOpenReadersClosed(recordedReaders);
    }
  }
}
