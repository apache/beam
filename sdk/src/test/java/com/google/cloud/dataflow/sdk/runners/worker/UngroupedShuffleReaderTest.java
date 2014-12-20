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
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.Reader;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
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
 * Tests for UngroupedShuffleReader.
 */
@RunWith(JUnit4.class)
public class UngroupedShuffleReaderTest {
  private static final Instant timestamp = new Instant(123000);
  private static final IntervalWindow window = new IntervalWindow(timestamp, timestamp.plus(1000));

  void runTestReadFromShuffle(List<Integer> expected) throws Exception {
    Coder<WindowedValue<Integer>> elemCoder =
        WindowedValue.getFullCoder(BigEndianIntegerCoder.of(), IntervalWindow.getCoder());

    // Write to shuffle with UNGROUPED ShuffleSink.
    ShuffleSink<Integer> shuffleSink = new ShuffleSink<>(
        PipelineOptionsFactory.create(),
        null, ShuffleSink.ShuffleKind.UNGROUPED,
        elemCoder);

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();

    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<Integer>> shuffleSinkWriter =
             shuffleSink.writer(shuffleWriter)) {
      for (Integer value : expected) {
        actualSizes.add(shuffleSinkWriter.add(
            WindowedValue.of(value, timestamp, Lists.newArrayList(window))));
      }
    }
    List<ShuffleEntry> records = shuffleWriter.getRecords();
    Assert.assertEquals(expected.size(), records.size());
    Assert.assertEquals(shuffleWriter.getSizes(), actualSizes);

    // Read from shuffle with UngroupedShuffleReader.
    UngroupedShuffleReader<WindowedValue<Integer>> ungroupedShuffleReader =
        new UngroupedShuffleReader<>(
            PipelineOptionsFactory.create(),
            null, null, null,
            elemCoder);
    ExecutorTestUtils.TestReaderObserver observer =
        new ExecutorTestUtils.TestReaderObserver(ungroupedShuffleReader);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Integer> expectedSizes = new ArrayList<>();
    for (ShuffleEntry record : records) {
      expectedSizes.add(record.length());
      shuffleReader.addEntry(record);
    }

    List<Integer> actual = new ArrayList<>();
    try (Reader.ReaderIterator<WindowedValue<Integer>> iter =
        ungroupedShuffleReader.iterator(shuffleReader)) {
      while (iter.hasNext()) {
        Assert.assertTrue(iter.hasNext());
        Assert.assertTrue(iter.hasNext());
        WindowedValue<Integer> elem = iter.next();
        Assert.assertEquals(timestamp, elem.getTimestamp());
        Assert.assertEquals(Lists.newArrayList(window), elem.getWindows());
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
  public void testReadEmptyShuffleData() throws Exception {
    runTestReadFromShuffle(TestUtils.NO_INTS);
  }

  @Test
  public void testReadNonEmptyShuffleData() throws Exception {
    runTestReadFromShuffle(TestUtils.INTS);
  }
}
