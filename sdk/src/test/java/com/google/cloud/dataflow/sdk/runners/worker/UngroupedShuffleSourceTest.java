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

import com.google.cloud.dataflow.sdk.TestUtils;
import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.BigEndianLongCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.CoderUtils;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;
import com.google.common.collect.Lists;

import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests for UngroupedShuffleSource.
 */
@RunWith(JUnit4.class)
public class UngroupedShuffleSourceTest {
  static final Instant timestamp = new Instant(123000);
  static final IntervalWindow window = new IntervalWindow(timestamp, timestamp.plus(1000));

  byte[] asShuffleKey(long seqNum) throws Exception {
    return CoderUtils.encodeToByteArray(BigEndianLongCoder.of(), seqNum);
  }

  byte[] asShuffleValue(Integer value) throws Exception {
    return CoderUtils.encodeToByteArray(
        WindowedValue.getFullCoder(BigEndianIntegerCoder.of(), IntervalWindow.getCoder()),
        WindowedValue.of(value, timestamp, Lists.newArrayList(window)));
  }

  void runTestReadShuffleSource(List<Integer> expected) throws Exception {
    UngroupedShuffleSource<WindowedValue<Integer>> shuffleSource =
        new UngroupedShuffleSource<>(
            PipelineOptionsFactory.create(),
            null, null, null,
            WindowedValue.getFullCoder(BigEndianIntegerCoder.of(), IntervalWindow.getCoder()));
    ExecutorTestUtils.TestSourceObserver observer =
        new ExecutorTestUtils.TestSourceObserver(shuffleSource);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Integer> expectedSizes = new ArrayList<>();
    long seqNum = 0;
    for (Integer value : expected) {
      byte[] shuffleKey = asShuffleKey(seqNum++);
      byte[] shuffleValue = asShuffleValue(value);
      shuffleReader.addEntry(shuffleKey, shuffleValue);

      ShuffleEntry record = new ShuffleEntry(shuffleKey, null, shuffleValue);
      expectedSizes.add(record.length());
    }

    List<Integer> actual = new ArrayList<>();
    try (Source.SourceIterator<WindowedValue<Integer>> iter =
             shuffleSource.iterator(shuffleReader)) {
      while (iter.hasNext()) {
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());
        WindowedValue<Integer> elem = iter.next();
        actual.add(elem.getValue());
      }
      Assert.assertFalse(iter.hasNext());
      Assert.assertFalse(iter.hasNext());
      try {
        iter.next();
        Assert.fail("should have failed");
      } catch (NoSuchElementException exn) {
        // As expected.
      }
    }

    Assert.assertEquals(expected, actual);
    Assert.assertEquals(expectedSizes, observer.getActualSizes());
  }

  @Test
  public void testReadEmptyShuffleSource() throws Exception {
    runTestReadShuffleSource(TestUtils.NO_INTS);
  }

  @Test
  public void testReadNonEmptyShuffleSource() throws Exception {
    runTestReadShuffleSource(TestUtils.INTS);
  }
}
