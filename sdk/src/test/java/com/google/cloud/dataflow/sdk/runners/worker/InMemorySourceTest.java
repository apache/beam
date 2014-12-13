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
import static com.google.cloud.dataflow.sdk.util.CoderUtils.encodeToByteArray;
import static com.google.cloud.dataflow.sdk.util.StringUtils.byteArrayToJsonString;

import com.google.api.services.dataflow.model.ApproximateProgress;
import com.google.api.services.dataflow.model.Position;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for InMemorySource.
 */
@RunWith(JUnit4.class)
public class InMemorySourceTest {
  static <T> List<String> encodedElements(List<T> elements, Coder<T> coder)
      throws Exception {
    List<String> encodedElements = new ArrayList<>();
    for (T element : elements) {
      byte[] encodedElement = encodeToByteArray(coder, element);
      String encodedElementString = byteArrayToJsonString(encodedElement);
      encodedElements.add(encodedElementString);
    }
    return encodedElements;
  }

  <T> void runTestReadInMemorySource(List<T> elements,
                                     Long startIndex,
                                     Long endIndex,
                                     List<T> expectedElements,
                                     List<Integer> expectedSizes,
                                     Coder<T> coder)
      throws Exception {
    InMemorySource<T> inMemorySource = new InMemorySource<>(
        encodedElements(elements, coder), startIndex, endIndex, coder);
    ExecutorTestUtils.TestSourceObserver observer =
        new ExecutorTestUtils.TestSourceObserver(inMemorySource);
    List<T> actualElements = new ArrayList<>();
    try (Source.SourceIterator<T> iterator = inMemorySource.iterator()) {
      for (long i = inMemorySource.startIndex; iterator.hasNext(); i++) {
        Assert.assertEquals(
            new ApproximateProgress().setPosition(makeIndexPosition(i)),
            sourceProgressToCloudProgress(iterator.getProgress()));
        actualElements.add(iterator.next());
      }
    }
    Assert.assertEquals(expectedElements, actualElements);
    Assert.assertEquals(expectedSizes, observer.getActualSizes());
  }

  @Test
  public void testReadAllElements() throws Exception {
    runTestReadInMemorySource(Arrays.asList(33, 44, 55, 66, 77, 88),
                              null,
                              null,
                              Arrays.asList(33, 44, 55, 66, 77, 88),
                              Arrays.asList(4, 4, 4, 4, 4, 4),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsFromStart() throws Exception {
    runTestReadInMemorySource(Arrays.asList(33, 44, 55, 66, 77, 88),
                              2L,
                              null,
                              Arrays.asList(55, 66, 77, 88),
                              Arrays.asList(4, 4, 4, 4),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsToEnd() throws Exception {
    runTestReadInMemorySource(Arrays.asList(33, 44, 55, 66, 77, 88),
                              null,
                              3L,
                              Arrays.asList(33, 44, 55),
                              Arrays.asList(4, 4, 4),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsFromStartToEnd() throws Exception {
    runTestReadInMemorySource(Arrays.asList(33, 44, 55, 66, 77, 88),
                              2L,
                              5L,
                              Arrays.asList(55, 66, 77),
                              Arrays.asList(4, 4, 4),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsOffEnd() throws Exception {
    runTestReadInMemorySource(Arrays.asList(33, 44, 55, 66, 77, 88),
                              null,
                              30L,
                              Arrays.asList(33, 44, 55, 66, 77, 88),
                              Arrays.asList(4, 4, 4, 4, 4, 4),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsFromStartPastEnd() throws Exception {
    runTestReadInMemorySource(Arrays.asList(33, 44, 55, 66, 77, 88),
                              20L,
                              null,
                              Arrays.<Integer>asList(),
                              Arrays.<Integer>asList(),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadElementsFromStartToEndEmptyRange() throws Exception {
    runTestReadInMemorySource(Arrays.asList(33, 44, 55, 66, 77, 88),
                              2L,
                              2L,
                              Arrays.<Integer>asList(),
                              Arrays.<Integer>asList(),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadNoElements() throws Exception {
    runTestReadInMemorySource(Arrays.<Integer>asList(),
                              null,
                              null,
                              Arrays.<Integer>asList(),
                              Arrays.<Integer>asList(),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testReadNoElementsFromStartToEndEmptyRange() throws Exception {
    runTestReadInMemorySource(Arrays.<Integer>asList(),
                              0L,
                              0L,
                              Arrays.<Integer>asList(),
                              Arrays.<Integer>asList(),
                              BigEndianIntegerCoder.of());
  }

  @Test
  public void testUpdatePosition() throws Exception {
    List<Integer> elements = Arrays.asList(33, 44, 55, 66, 77, 88);
    final long start = 1L;
    final long stop = 3L;
    final long end = 4L;

    Coder<Integer> coder = BigEndianIntegerCoder.of();
    InMemorySource<Integer> inMemorySource = new InMemorySource<>(
        encodedElements(elements, coder), start, end, coder);

    // Illegal proposed stop position.
    try (Source.SourceIterator<Integer> iterator = inMemorySource.iterator()) {
      Assert.assertNull(iterator.updateStopPosition(
          cloudProgressToSourceProgress(new ApproximateProgress())));
      Assert.assertNull(iterator.updateStopPosition(
          cloudProgressToSourceProgress(
              new ApproximateProgress().setPosition(makeIndexPosition(null)))));
    }

    // Successful update.
    try (InMemorySource<Integer>.InMemorySourceIterator iterator =
        (InMemorySource<Integer>.InMemorySourceIterator) inMemorySource.iterator()) {
      Assert.assertEquals(
          makeIndexPosition(stop),
          sourcePositionToCloudPosition(
              iterator.updateStopPosition(
                  cloudProgressToSourceProgress(
                      new ApproximateProgress().setPosition(makeIndexPosition(stop))))));
      Assert.assertEquals(stop, iterator.endPosition);
      Assert.assertEquals(44, iterator.next().intValue());
      Assert.assertEquals(55, iterator.next().intValue());
      Assert.assertFalse(iterator.hasNext());
    }

    // Proposed stop position is before the current position, no update.
    try (InMemorySource<Integer>.InMemorySourceIterator iterator =
        (InMemorySource<Integer>.InMemorySourceIterator) inMemorySource.iterator()) {
      Assert.assertEquals(44, iterator.next().intValue());
      Assert.assertEquals(55, iterator.next().intValue());
      Assert.assertNull(iterator.updateStopPosition(
          cloudProgressToSourceProgress(
              new ApproximateProgress().setPosition(makeIndexPosition(stop)))));
      Assert.assertEquals((int) end, iterator.endPosition);
      Assert.assertTrue(iterator.hasNext());
    }

    // Proposed stop position is after the current stop (end) position, no update.
    try (InMemorySource<Integer>.InMemorySourceIterator iterator =
        (InMemorySource<Integer>.InMemorySourceIterator) inMemorySource.iterator()) {
      Assert.assertNull(
          iterator.updateStopPosition(
              cloudProgressToSourceProgress(
                  new ApproximateProgress().setPosition(makeIndexPosition(end + 1)))));
      Assert.assertEquals((int) end, iterator.endPosition);
    }
  }

  private Position makeIndexPosition(Long index) {
    Position position = new Position();
    if (index != null) {
      position.setRecordIndex(index);
    }
    return position;
  }
}
