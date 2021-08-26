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

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.dataflow.worker.UngroupedShuffleReader.UngroupedShuffleReaderIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutorTestUtils;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.TestUtils;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for UngroupedShuffleReader. */
@RunWith(JUnit4.class)
public class UngroupedShuffleReaderTest {
  private static final Instant timestamp = new Instant(123000);
  private static final IntervalWindow window = new IntervalWindow(timestamp, timestamp.plus(1000));

  void runTestReadFromShuffle(List<Integer> expected) throws Exception {
    Coder<WindowedValue<Integer>> elemCoder =
        WindowedValue.getFullCoder(BigEndianIntegerCoder.of(), IntervalWindow.getCoder());

    BatchModeExecutionContext executionContext =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "STAGE");
    // Write to shuffle with UNGROUPED ShuffleSink.
    ShuffleSink<Integer> shuffleSink =
        new ShuffleSink<>(
            PipelineOptionsFactory.create(),
            null,
            ShuffleSink.ShuffleKind.UNGROUPED,
            elemCoder,
            executionContext,
            TestOperationContext.create());

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();

    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<Integer>> shuffleSinkWriter =
        shuffleSink.writer(shuffleWriter, "dataset")) {
      for (Integer value : expected) {
        actualSizes.add(
            shuffleSinkWriter.add(
                WindowedValue.of(
                    value, timestamp, Lists.newArrayList(window), PaneInfo.NO_FIRING)));
      }
    }
    List<ShuffleEntry> records = shuffleWriter.getRecords();
    Assert.assertEquals(expected.size(), records.size());
    Assert.assertEquals(shuffleWriter.getSizes(), actualSizes);

    // Read from shuffle with UngroupedShuffleReader.
    UngroupedShuffleReader<WindowedValue<Integer>> ungroupedShuffleReader =
        new UngroupedShuffleReader<>(
            PipelineOptionsFactory.create(),
            null,
            null,
            null,
            elemCoder,
            executionContext,
            TestOperationContext.create());
    ExecutorTestUtils.TestReaderObserver observer =
        new ExecutorTestUtils.TestReaderObserver(ungroupedShuffleReader);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Integer> expectedSizes = new ArrayList<>();
    for (ShuffleEntry record : records) {
      expectedSizes.add(record.length());
      shuffleReader.addEntry(record);
    }

    List<Integer> actual = new ArrayList<>();
    Assert.assertFalse(shuffleReader.isClosed());
    try (UngroupedShuffleReaderIterator<WindowedValue<Integer>> iter =
        ungroupedShuffleReader.iterator(shuffleReader)) {
      for (boolean more = iter.start(); more; more = iter.advance()) {
        WindowedValue<Integer> elem = iter.getCurrent();
        Assert.assertEquals(timestamp, elem.getTimestamp());
        Assert.assertEquals(Lists.newArrayList(window), elem.getWindows());
        actual.add(elem.getValue());
      }
      Assert.assertFalse(iter.advance());
      try {
        iter.getCurrent();
        Assert.fail("should have failed");
      } catch (NoSuchElementException exn) {
        // As expected.
      }
    }
    Assert.assertTrue(shuffleReader.isClosed());

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
