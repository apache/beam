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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.dataflow.worker.util.common.worker.ShuffleEntry;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Sink.SinkWriter;
import org.apache.beam.sdk.TestUtils;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ShuffleSink}. */
@RunWith(JUnit4.class)
public class ShuffleSinkTest {
  private static final List<KV<Integer, String>> NO_KVS = Collections.emptyList();

  private static final List<KV<Integer, String>> KVS =
      Arrays.asList(
          KV.of(1, "in 1a"),
          KV.of(1, "in 1b"),
          KV.of(2, "in 2a"),
          KV.of(2, "in 2b"),
          KV.of(3, "in 3"),
          KV.of(4, "in 4a"),
          KV.of(4, "in 4b"),
          KV.of(4, "in 4c"),
          KV.of(4, "in 4d"),
          KV.of(5, "in 5"));

  private static final List<KV<Integer, KV<String, Integer>>> NO_SORTING_KVS =
      Collections.emptyList();

  private static final List<KV<Integer, KV<String, Integer>>> SORTING_KVS =
      Arrays.asList(
          KV.of(1, KV.of("in 1a", 3)),
          KV.of(1, KV.of("in 1b", 9)),
          KV.of(2, KV.of("in 2a", 2)),
          KV.of(2, KV.of("in 2b", 77)),
          KV.of(3, KV.of("in 3", 33)),
          KV.of(4, KV.of("in 4a", -123)),
          KV.of(4, KV.of("in 4b", 0)),
          KV.of(4, KV.of("in 4c", -1)),
          KV.of(4, KV.of("in 4d", 1)),
          KV.of(5, KV.of("in 5", 666)));

  private static final Instant timestamp = new Instant(123000);
  private static final IntervalWindow window =
      new IntervalWindow(timestamp, timestamp.plus(Duration.millis(1000)));

  private void runTestWriteUngroupingShuffleSink(List<Integer> expected) throws Exception {
    Coder<WindowedValue<Integer>> windowedValueCoder =
        WindowedValue.getFullCoder(BigEndianIntegerCoder.of(), new GlobalWindows().windowCoder());
    BatchModeExecutionContext executionContext =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "STAGE");
    ShuffleSink<Integer> shuffleSink =
        new ShuffleSink<>(
            PipelineOptionsFactory.create(),
            null,
            ShuffleSink.ShuffleKind.UNGROUPED,
            windowedValueCoder,
            executionContext,
            TestOperationContext.create());

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();
    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<Integer>> shuffleSinkWriter =
        shuffleSink.writer(shuffleWriter, "dataset")) {
      for (Integer value : expected) {
        actualSizes.add(shuffleSinkWriter.add(WindowedValue.valueInGlobalWindow(value)));
      }
    }

    List<ShuffleEntry> records = shuffleWriter.getRecords();

    List<Integer> actual = new ArrayList<>();
    for (ShuffleEntry record : records) {
      // Ignore the key.
      ByteString valueBytes = record.getValue();
      WindowedValue<Integer> value =
          CoderUtils.decodeFromByteString(windowedValueCoder, valueBytes);
      Assert.assertEquals(Lists.newArrayList(GlobalWindow.INSTANCE), value.getWindows());
      actual.add(value.getValue());
    }

    Assert.assertEquals(expected, actual);
    Assert.assertEquals(shuffleWriter.getSizes(), actualSizes);
  }

  void runTestWriteGroupingShuffleSink(List<KV<Integer, String>> expected) throws Exception {
    BatchModeExecutionContext executionContext =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "STAGE");
    ShuffleSink<KV<Integer, String>> shuffleSink =
        new ShuffleSink<>(
            PipelineOptionsFactory.create(),
            null,
            ShuffleSink.ShuffleKind.GROUP_KEYS,
            WindowedValue.getFullCoder(
                KvCoder.of(BigEndianIntegerCoder.of(), StringUtf8Coder.of()),
                IntervalWindow.getCoder()),
            executionContext,
            TestOperationContext.create());

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();
    List<Long> actualSizes = new ArrayList<>();
    try (SinkWriter<WindowedValue<KV<Integer, String>>> shuffleSinkWriter =
        shuffleSink.writer(shuffleWriter, "dataset")) {
      for (KV<Integer, String> kv : expected) {
        actualSizes.add(
            shuffleSinkWriter.add(
                WindowedValue.of(
                    KV.of(kv.getKey(), kv.getValue()),
                    timestamp,
                    Lists.newArrayList(window),
                    PaneInfo.NO_FIRING)));
      }
    }

    List<ShuffleEntry> records = shuffleWriter.getRecords();

    List<KV<Integer, String>> actual = new ArrayList<>();
    for (ShuffleEntry record : records) {
      ByteString keyBytes = record.getKey();
      ByteString valueBytes = record.getValue();
      Assert.assertEquals(
          timestamp, CoderUtils.decodeFromByteString(InstantCoder.of(), record.getSecondaryKey()));

      Integer key = CoderUtils.decodeFromByteString(BigEndianIntegerCoder.of(), keyBytes);
      String valueElem = CoderUtils.decodeFromByteString(StringUtf8Coder.of(), valueBytes);

      actual.add(KV.of(key, valueElem));
    }

    Assert.assertEquals(expected, actual);
    Assert.assertEquals(shuffleWriter.getSizes(), actualSizes);
  }

  void runTestWriteGroupingSortingShuffleSink(List<KV<Integer, KV<String, Integer>>> expected)
      throws Exception {
    BatchModeExecutionContext executionContext =
        BatchModeExecutionContext.forTesting(PipelineOptionsFactory.create(), "STAGE");
    ShuffleSink<KV<Integer, KV<String, Integer>>> shuffleSink =
        new ShuffleSink<>(
            PipelineOptionsFactory.create(),
            null,
            ShuffleSink.ShuffleKind.GROUP_KEYS_AND_SORT_VALUES,
            WindowedValue.getFullCoder(
                KvCoder.of(
                    BigEndianIntegerCoder.of(),
                    KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of())),
                new GlobalWindows().windowCoder()),
            executionContext,
            TestOperationContext.create());

    TestShuffleWriter shuffleWriter = new TestShuffleWriter();
    List<Long> actualSizes = new ArrayList<>();
    try (Sink.SinkWriter<WindowedValue<KV<Integer, KV<String, Integer>>>> shuffleSinkWriter =
        shuffleSink.writer(shuffleWriter, "dataset")) {
      for (KV<Integer, KV<String, Integer>> kv : expected) {
        actualSizes.add(shuffleSinkWriter.add(WindowedValue.valueInGlobalWindow(kv)));
      }
    }

    List<ShuffleEntry> records = shuffleWriter.getRecords();

    List<KV<Integer, KV<String, Integer>>> actual = new ArrayList<>();
    for (ShuffleEntry record : records) {
      ByteString keyBytes = record.getKey();
      ByteString valueBytes = record.getValue();
      ByteString sortKeyBytes = record.getSecondaryKey();

      Integer key = CoderUtils.decodeFromByteString(BigEndianIntegerCoder.of(), keyBytes);
      String sortKey =
          CoderUtils.decodeFromByteString(StringUtf8Coder.of(), sortKeyBytes, Coder.Context.NESTED);
      Integer sortValue = CoderUtils.decodeFromByteString(BigEndianIntegerCoder.of(), valueBytes);

      actual.add(KV.of(key, KV.of(sortKey, sortValue)));
    }

    Assert.assertEquals(expected, actual);
    Assert.assertEquals(shuffleWriter.getSizes(), actualSizes);
  }

  @Test
  public void testWriteEmptyUngroupingShuffleSink() throws Exception {
    runTestWriteUngroupingShuffleSink(TestUtils.NO_INTS);
  }

  @Test
  public void testWriteNonEmptyUngroupingShuffleSink() throws Exception {
    runTestWriteUngroupingShuffleSink(TestUtils.INTS);
  }

  @Test
  public void testWriteEmptyGroupingShuffleSink() throws Exception {
    runTestWriteGroupingShuffleSink(NO_KVS);
  }

  @Test
  public void testWriteNonEmptyGroupingShuffleSink() throws Exception {
    runTestWriteGroupingShuffleSink(KVS);
  }

  @Test
  public void testWriteEmptyGroupingSortingShuffleSink() throws Exception {
    runTestWriteGroupingSortingShuffleSink(NO_SORTING_KVS);
  }

  @Test
  public void testWriteNonEmptyGroupingSortingShuffleSink() throws Exception {
    runTestWriteGroupingSortingShuffleSink(SORTING_KVS);
  }
}
