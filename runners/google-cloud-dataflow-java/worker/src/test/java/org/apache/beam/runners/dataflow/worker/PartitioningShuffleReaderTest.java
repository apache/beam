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

import static org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.beam.runners.dataflow.worker.PartitioningShuffleReader.PartitioningShuffleReaderIterator;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ExecutorTestUtils;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for PartitioningShuffleReader. */
@RunWith(JUnit4.class)
public class PartitioningShuffleReaderTest {
  private static final List<WindowedValue<KV<Integer, String>>> NO_KVS = Collections.emptyList();

  private static final Instant timestamp = new Instant(123000);
  private static final IntervalWindow window = new IntervalWindow(timestamp, timestamp.plus(1000));

  private static final List<WindowedValue<KV<Integer, String>>> KVS =
      Arrays.asList(
          WindowedValue.of(KV.of(1, "in 1a"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(1, "in 1b"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(2, "in 2a"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(2, "in 2b"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(3, "in 3"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(4, "in 4a"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(4, "in 4b"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(4, "in 4c"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(4, "in 4d"), timestamp, Lists.newArrayList(window), NO_FIRING),
          WindowedValue.of(KV.of(5, "in 5"), timestamp, Lists.newArrayList(window), NO_FIRING));

  private void runTestReadFromShuffle(List<WindowedValue<KV<Integer, String>>> expected)
      throws Exception {
    Coder<WindowedValue<KV<Integer, String>>> elemCoder =
        WindowedValue.getFullCoder(
            KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()),
            IntervalWindow.getCoder());

    BatchModeExecutionContext executionContext =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "STAGE");
    // Write to shuffle with PARTITION_KEYS ShuffleSink.
    ShuffleSink<KV<Integer, String>> shuffleSink =
        new ShuffleSink<>(
            PipelineOptionsFactory.create(),
            null,
            ShuffleSink.ShuffleKind.PARTITION_KEYS,
            elemCoder,
            executionContext,
            TestOperationContext.create());

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();

    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<KV<Integer, String>>> shuffleSinkWriter =
        shuffleSink.writer(shuffleWriter, "dataset")) {
      for (WindowedValue<KV<Integer, String>> value : expected) {
        actualSizes.add(shuffleSinkWriter.add(value));
      }
    }
    List<ShuffleEntry> records = shuffleWriter.getRecords();
    Assert.assertEquals(expected.size(), records.size());
    Assert.assertEquals(shuffleWriter.getSizes(), actualSizes);

    // Read from shuffle with PartitioningShuffleReader.
    PartitioningShuffleReader<Integer, String> partitioningShuffleReader =
        new PartitioningShuffleReader<>(
            PipelineOptionsFactory.create(),
            null,
            null,
            null,
            elemCoder,
            executionContext,
            TestOperationContext.create());
    ExecutorTestUtils.TestReaderObserver observer =
        new ExecutorTestUtils.TestReaderObserver(partitioningShuffleReader);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Integer> expectedSizes = new ArrayList<>();
    for (ShuffleEntry record : records) {
      expectedSizes.add(record.length());
      shuffleReader.addEntry(record);
    }

    List<WindowedValue<KV<Integer, String>>> actual = new ArrayList<>();
    Assert.assertFalse(shuffleReader.isClosed());
    try (PartitioningShuffleReaderIterator<Integer, String> iter =
        partitioningShuffleReader.iterator(shuffleReader)) {
      for (boolean more = iter.start(); more; more = iter.advance()) {
        actual.add(iter.getCurrent());
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
    runTestReadFromShuffle(NO_KVS);
  }

  @Test
  public void testReadNonEmptyShuffleData() throws Exception {
    runTestReadFromShuffle(KVS);
  }
}
