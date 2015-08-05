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

import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.approximateProgressAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.positionFromSplitResult;
import static com.google.cloud.dataflow.sdk.runners.worker.ReaderTestUtils.splitRequestAtIndex;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.readerProgressToCloudProgress;
import static com.google.cloud.dataflow.sdk.runners.worker.SourceTranslationUtils.toDynamicSplitRequest;
import static com.google.cloud.dataflow.sdk.util.CoderUtils.encodeToByteArray;
import static com.google.cloud.dataflow.sdk.util.StringUtils.byteArrayToJsonString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for InMemoryReader.
 */
@RunWith(JUnit4.class)
public class InMemoryReaderTest {
  static <T> List<String> encodedElements(List<T> elements, Coder<T> coder) throws Exception {
    List<String> encodedElements = new ArrayList<>();
    for (T element : elements) {
      byte[] encodedElement = encodeToByteArray(coder, element);
      String encodedElementString = byteArrayToJsonString(encodedElement);
      encodedElements.add(encodedElementString);
    }
    return encodedElements;
  }

  <T> void runTestReadInMemory(List<T> elements, Long startIndex, Long endIndex,
      List<T> expectedElements, List<Integer> expectedSizes, Coder<T> coder) throws Exception {
    InMemoryReader<T> inMemoryReader =
        new InMemoryReader<>(encodedElements(elements, coder), startIndex, endIndex, coder);
    ExecutorTestUtils.TestReaderObserver observer =
        new ExecutorTestUtils.TestReaderObserver(inMemoryReader);
    List<T> actualElements = new ArrayList<>();
    try (Reader.ReaderIterator<T> iterator = inMemoryReader.iterator()) {
      for (long i = inMemoryReader.startIndex; iterator.hasNext(); i++) {
        assertEquals(
            approximateProgressAtIndex(i), readerProgressToCloudProgress(iterator.getProgress()));
        actualElements.add(iterator.next());
      }
    }
    assertEquals(expectedElements, actualElements);
    assertEquals(expectedSizes, observer.getActualSizes());
  }

  @Test
  public void testReadAllElements() throws Exception {
    runTestReadInMemory(Arrays.asList(33, 44, 55, 66, 77, 88), null, null,
        Arrays.asList(33, 44, 55, 66, 77, 88), Arrays.asList(4, 4, 4, 4, 4, 4),
        BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsFromStart() throws Exception {
    runTestReadInMemory(Arrays.asList(33, 44, 55, 66, 77, 88), 2L, null,
        Arrays.asList(55, 66, 77, 88), Arrays.asList(4, 4, 4, 4), BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsToEnd() throws Exception {
    runTestReadInMemory(Arrays.asList(33, 44, 55, 66, 77, 88), null, 3L, Arrays.asList(33, 44, 55),
        Arrays.asList(4, 4, 4), BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsFromStartToEnd() throws Exception {
    runTestReadInMemory(Arrays.asList(33, 44, 55, 66, 77, 88), 2L, 5L, Arrays.asList(55, 66, 77),
        Arrays.asList(4, 4, 4), BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsOffEnd() throws Exception {
    runTestReadInMemory(Arrays.asList(33, 44, 55, 66, 77, 88), null, 30L,
        Arrays.asList(33, 44, 55, 66, 77, 88), Arrays.asList(4, 4, 4, 4, 4, 4),
        BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsFromStartPastEnd() throws Exception {
    runTestReadInMemory(Arrays.asList(33, 44, 55, 66, 77, 88), 20L, null, Arrays.<Integer>asList(),
        Arrays.<Integer>asList(), BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsFromStartToEndEmptyRange() throws Exception {
    runTestReadInMemory(Arrays.asList(33, 44, 55, 66, 77, 88), 2L, 2L, Arrays.<Integer>asList(),
        Arrays.<Integer>asList(), BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadNoElements() throws Exception {
    runTestReadInMemory(Arrays.<Integer>asList(), null, null, Arrays.<Integer>asList(),
        Arrays.<Integer>asList(), BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadNoElementsFromStartToEndEmptyRange() throws Exception {
    runTestReadInMemory(Arrays.<Integer>asList(), 0L, 0L, Arrays.<Integer>asList(),
        Arrays.<Integer>asList(), BigEndianIntegerCoder.of());
  }

  @Test
  public void testDynamicSplit() throws Exception {
    List<Integer> elements = Arrays.asList(33, 44, 55, 66, 77, 88);
    // Should initially read elements at indices: 44@1, 55@2, 66@3, 77@4

    Coder<Integer> coder = BigEndianIntegerCoder.of();
    InMemoryReader<Integer> inMemoryReader =
        new InMemoryReader<>(encodedElements(elements, coder), 1L, 4L, coder);

    // Illegal proposed split position.
    try (Reader.ReaderIterator<Integer> iterator = inMemoryReader.iterator()) {
      // Poke the iterator so that we can test dynamic splitting.
      assertTrue(iterator.hasNext());
      assertNull(iterator.requestDynamicSplit(toDynamicSplitRequest(new ApproximateProgress())));
      assertNull(iterator.requestDynamicSplit(splitRequestAtIndex(null)));
    }

    // Successful update.
    try (InMemoryReader<Integer>.InMemoryReaderIterator iterator =
        (InMemoryReader<Integer>.InMemoryReaderIterator) inMemoryReader.iterator()) {
      // Poke the iterator so that we can test dynamic splitting.
      assertTrue(iterator.hasNext());
      Reader.DynamicSplitResult dynamicSplitResult =
          iterator.requestDynamicSplit(splitRequestAtIndex(3L));
      assertEquals(positionAtIndex(3L), positionFromSplitResult(dynamicSplitResult));
      assertEquals(3, iterator.tracker.getStopPosition().longValue());
      assertEquals(44, iterator.next().intValue());
      assertEquals(55, iterator.next().intValue());
      assertFalse(iterator.hasNext());
    }

    // Proposed split position is before the current position, no update.
    try (InMemoryReader<Integer>.InMemoryReaderIterator iterator =
        (InMemoryReader<Integer>.InMemoryReaderIterator) inMemoryReader.iterator()) {
      // Poke the iterator so that we can test dynamic splitting.
      assertTrue(iterator.hasNext());
      assertEquals(44, iterator.next().intValue());
      assertEquals(55, iterator.next().intValue());
      assertTrue(iterator.hasNext()); // Returns true => we promised to return 66.
      // Now we have to refuse the split.
      assertNull(iterator.requestDynamicSplit(splitRequestAtIndex(3L)));
      assertEquals(4, iterator.tracker.getStopPosition().longValue());
      assertTrue(iterator.hasNext());
    }

    // Proposed split position is after the current stop (end) position, no update.
    try (InMemoryReader<Integer>.InMemoryReaderIterator iterator =
        (InMemoryReader<Integer>.InMemoryReaderIterator) inMemoryReader.iterator()) {
      // Poke the iterator so that we can test dynamic splitting.
      assertTrue(iterator.hasNext());
      assertNull(iterator.requestDynamicSplit(splitRequestAtIndex(5L)));
      assertEquals(4, iterator.tracker.getStopPosition().longValue());
    }
  }
}
