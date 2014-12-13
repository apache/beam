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

import com.google.cloud.dataflow.sdk.coders.BigEndianIntegerCoder;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.KvCoder;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.ShuffleEntry;
import com.google.cloud.dataflow.sdk.util.common.worker.Sink;
import com.google.cloud.dataflow.sdk.util.common.worker.Source;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.collect.Lists;

import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Tests for PartitioningShuffleSource.
 */
@RunWith(JUnit4.class)
public class PartitioningShuffleSourceTest {
  static final List<WindowedValue<KV<Integer, String>>> NO_KVS = Collections.emptyList();

  static final Instant timestamp = new Instant(123000);
  static final IntervalWindow window = new IntervalWindow(timestamp, timestamp.plus(1000));

  static final List<WindowedValue<KV<Integer, String>>> KVS = Arrays.asList(
      WindowedValue.of(KV.of(1, "in 1a"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(1, "in 1b"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(2, "in 2a"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(2, "in 2b"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(3, "in 3"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(4, "in 4a"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(4, "in 4b"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(4, "in 4c"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(4, "in 4d"), timestamp, Lists.newArrayList(window)),
      WindowedValue.of(KV.of(5, "in 5"), timestamp, Lists.newArrayList(window)));

  void runTestReadShuffleSource(List<WindowedValue<KV<Integer, String>>> expected)
      throws Exception {
    Coder<WindowedValue<KV<Integer, String>>> elemCoder = WindowedValue.getFullCoder(
            KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()),
            IntervalWindow.getCoder());

    // Write to shuffle with PARTITION_KEYS ShuffleSink.
    ShuffleSink<KV<Integer, String>> shuffleSink = new ShuffleSink<>(
        PipelineOptionsFactory.create(),
        null, ShuffleSink.ShuffleKind.PARTITION_KEYS,
        elemCoder);

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();

    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<KV<Integer, String>>> shuffleSinkWriter =
             shuffleSink.writer(shuffleWriter)) {
      for (WindowedValue<KV<Integer, String>> value : expected) {
        actualSizes.add(shuffleSinkWriter.add(value));
      }
    }
    List<ShuffleEntry> records = shuffleWriter.getRecords();
    Assert.assertEquals(expected.size(), records.size());
    Assert.assertEquals(shuffleWriter.getSizes(), actualSizes);

    // Read from shuffle with PartitioningShuffleSource.
    PartitioningShuffleSource<Integer, String> shuffleSource =
        new PartitioningShuffleSource<>(
            PipelineOptionsFactory.create(),
            null, null, null,
            elemCoder);
    ExecutorTestUtils.TestSourceObserver observer =
        new ExecutorTestUtils.TestSourceObserver(shuffleSource);

    TestShuffleReader shuffleReader = new TestShuffleReader();
    List<Integer> expectedSizes = new ArrayList<>();
    for (ShuffleEntry record : records) {
      expectedSizes.add(record.length());
      shuffleReader.addEntry(record);
    }

    List<WindowedValue<KV<Integer, String>>> actual = new ArrayList<>();
    try (Source.SourceIterator<WindowedValue<KV<Integer, String>>> iter =
             shuffleSource.iterator(shuffleReader)) {
      while (iter.hasNext()) {
        Assert.assertTrue(iter.hasNext());
        actual.add(iter.next());
      }
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
    runTestReadShuffleSource(NO_KVS);
  }

  @Test
  public void testReadNonEmptyShuffleSource() throws Exception {
    runTestReadShuffleSource(KVS);
  }
}
