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
package org.apache.beam.runners.flink.translation.wrappers.streaming;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link PartialReduceBundleOperator}. */
@RunWith(JUnit4.class)
public class PartialReduceBundleOperatorTest {

  /** Sums longs; the accumulator is the running sum so partial results are easy to compare. */
  private static class SumLongs extends Combine.CombineFn<Long, Long, Long> {
    @Override
    public Long createAccumulator() {
      return 0L;
    }

    @Override
    public Long addInput(Long accumulator, Long input) {
      return accumulator + input;
    }

    @Override
    public Long mergeAccumulators(Iterable<Long> accumulators) {
      long sum = 0;
      for (Long accumulator : accumulators) {
        sum += accumulator;
      }
      return sum;
    }

    @Override
    public Long extractOutput(Long accumulator) {
      return accumulator;
    }

    @Override
    public Coder<Long> getAccumulatorCoder(CoderRegistry registry, Coder<Long> inputCoder) {
      return VarLongCoder.of();
    }
  }

  private static <K, W extends BoundedWindow>
      PartialReduceBundleOperator<K, Long, Long, Long> createOperator(
          Coder<K> keyCoder, WindowingStrategy<?, W> windowingStrategy, long maxBundleSize) {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setStreaming(false);
    options.setMaxBundleSize(maxBundleSize);

    Coder<W> windowCoder = windowingStrategy.getWindowFn().windowCoder();
    Coder<WindowedValue<KV<K, Long>>> windowedCoder =
        WindowedValues.getFullCoder(KvCoder.of(keyCoder, VarLongCoder.of()), windowCoder);
    TupleTag<KV<K, Long>> mainTag = new TupleTag<>("main-output");

    return new PartialReduceBundleOperator<>(
        new SumLongs(),
        "stepName",
        windowedCoder,
        mainTag,
        emptyList(),
        new DoFnOperator.MultiOutputOutputManagerFactory<>(
            mainTag, windowedCoder, new SerializablePipelineOptions(options)),
        windowingStrategy,
        emptyMap(),
        emptyList(),
        options);
  }

  /** Runs one bundle containing {@code input} through the operator and returns its output. */
  private static <K> List<KV<K, Long>> preCombine(Coder<K> keyCoder, List<KV<K, Long>> input)
      throws Exception {
    // Finish the bundle, and so emit the partial results, right after the last input.
    PartialReduceBundleOperator<K, Long, Long, Long> operator =
        createOperator(keyCoder, WindowingStrategy.globalDefault(), input.size());

    List<KV<K, Long>> output = new ArrayList<>();
    try (OneInputStreamOperatorTestHarness<WindowedValue<KV<K, Long>>, WindowedValue<KV<K, Long>>>
        testHarness = new OneInputStreamOperatorTestHarness<>(operator)) {
      testHarness.open();
      for (KV<K, Long> element : input) {
        testHarness.processElement(new StreamRecord<>(WindowedValues.valueInGlobalWindow(element)));
      }
      for (WindowedValue<KV<K, Long>> value :
          StreamRecordStripper.<KV<K, Long>>stripStreamRecordFromWindowedValue(
              testHarness.getOutput())) {
        output.add(value.getValue());
      }
    }
    return output;
  }

  private static <K, W extends BoundedWindow> WindowedValue<KV<K, Long>> windowedElement(
      WindowFn<Object, W> windowFn, K key, long value, long timestampMs) throws Exception {
    Instant timestamp = new Instant(timestampMs);
    W window =
        Iterables.getOnlyElement(
            windowFn.assignWindows(
                windowFn.new AssignContext() {
                  @Override
                  public Object element() {
                    return key;
                  }

                  @Override
                  public Instant timestamp() {
                    return timestamp;
                  }

                  @Override
                  public BoundedWindow window() {
                    return GlobalWindow.INSTANCE;
                  }
                }));
    return WindowedValues.of(KV.of(key, value), timestamp, window, PaneInfo.NO_FIRING);
  }

  @Test
  public void combinesKeysThatAreEqualOnlyByEncoding() throws Exception {
    // Distinct byte[] instances with equal contents: byte[] uses identity equals/hashCode.
    List<KV<byte[], Long>> input =
        Arrays.asList(
            KV.of(new byte[] {1, 2}, 1L),
            KV.of(new byte[] {3}, 10L),
            KV.of(new byte[] {1, 2}, 2L),
            KV.of(new byte[] {1, 2}, 3L),
            KV.of(new byte[] {3}, 20L));

    List<KV<String, Long>> output = new ArrayList<>();
    for (KV<byte[], Long> kv : preCombine(ByteArrayCoder.of(), input)) {
      output.add(KV.of(Arrays.toString(kv.getKey()), kv.getValue()));
    }

    assertThat(output, containsInAnyOrder(KV.of("[1, 2]", 6L), KV.of("[3]", 30L)));
  }

  @Test
  public void combinesKeysWithConsistentEquals() throws Exception {
    List<KV<Long, Long>> input =
        Arrays.asList(KV.of(1L, 1L), KV.of(2L, 10L), KV.of(1L, 2L), KV.of(2L, 20L), KV.of(1L, 3L));

    assertThat(
        preCombine(VarLongCoder.of(), input), containsInAnyOrder(KV.of(1L, 6L), KV.of(2L, 30L)));
  }

  @Test
  public void flushesIncompleteBundleOnClose() throws Exception {
    PartialReduceBundleOperator<byte[], Long, Long, Long> operator =
        createOperator(ByteArrayCoder.of(), WindowingStrategy.globalDefault(), 100L);
    OneInputStreamOperatorTestHarness<
            WindowedValue<KV<byte[], Long>>, WindowedValue<KV<byte[], Long>>>
        testHarness = new OneInputStreamOperatorTestHarness<>(operator);
    testHarness.open();
    testHarness.processElement(
        new StreamRecord<>(WindowedValues.valueInGlobalWindow(KV.of(new byte[] {1, 2}, 1L))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValues.valueInGlobalWindow(KV.of(new byte[] {3}, 10L))));
    testHarness.processElement(
        new StreamRecord<>(WindowedValues.valueInGlobalWindow(KV.of(new byte[] {1, 2}, 5L))));

    assertThat(
        StreamRecordStripper.stripStreamRecordFromWindowedValue(testHarness.getOutput()),
        emptyIterable());

    testHarness.close();

    List<KV<String, Long>> output = new ArrayList<>();
    for (WindowedValue<KV<byte[], Long>> value :
        StreamRecordStripper.<KV<byte[], Long>>stripStreamRecordFromWindowedValue(
            testHarness.getOutput())) {
      output.add(KV.of(Arrays.toString(value.getValue().getKey()), value.getValue().getValue()));
    }
    assertThat(output, containsInAnyOrder(KV.of("[1, 2]", 6L), KV.of("[3]", 10L)));
  }

  @Test
  public void combinesStructuralKeysPerFixedWindow() throws Exception {
    FixedWindows windowFn = FixedWindows.of(Duration.millis(10));
    PartialReduceBundleOperator<byte[], Long, Long, Long> operator =
        createOperator(ByteArrayCoder.of(), WindowingStrategy.of(windowFn), 4L);

    List<KV<String, Long>> output = new ArrayList<>();
    try (OneInputStreamOperatorTestHarness<
            WindowedValue<KV<byte[], Long>>, WindowedValue<KV<byte[], Long>>>
        testHarness = new OneInputStreamOperatorTestHarness<>(operator)) {
      testHarness.open();
      testHarness.processElement(
          new StreamRecord<>(windowedElement(windowFn, new byte[] {1, 2}, 1L, 1L)));
      testHarness.processElement(
          new StreamRecord<>(windowedElement(windowFn, new byte[] {1, 2}, 2L, 5L)));
      testHarness.processElement(
          new StreamRecord<>(windowedElement(windowFn, new byte[] {1, 2}, 10L, 12L)));
      testHarness.processElement(
          new StreamRecord<>(windowedElement(windowFn, new byte[] {3}, 20L, 3L)));

      for (WindowedValue<KV<byte[], Long>> value :
          StreamRecordStripper.<KV<byte[], Long>>stripStreamRecordFromWindowedValue(
              testHarness.getOutput())) {
        IntervalWindow w = (IntervalWindow) Iterables.getOnlyElement(value.getWindows());
        output.add(
            KV.of(
                Arrays.toString(value.getValue().getKey()) + "@" + w.start().getMillis(),
                value.getValue().getValue()));
      }
    }

    assertThat(
        output,
        containsInAnyOrder(KV.of("[1, 2]@0", 3L), KV.of("[1, 2]@10", 10L), KV.of("[3]@0", 20L)));
  }

  @Test
  public void combinesStructuralKeysAcrossMergedSessionWindows() throws Exception {
    Sessions windowFn = Sessions.withGapDuration(Duration.millis(10));
    PartialReduceBundleOperator<byte[], Long, Long, Long> operator =
        createOperator(ByteArrayCoder.of(), WindowingStrategy.of(windowFn), 4L);

    List<KV<String, Long>> output = new ArrayList<>();
    try (OneInputStreamOperatorTestHarness<
            WindowedValue<KV<byte[], Long>>, WindowedValue<KV<byte[], Long>>>
        testHarness = new OneInputStreamOperatorTestHarness<>(operator)) {
      testHarness.open();
      // Merge into [0, 15) for key [1, 2]; exercises SortingFlinkCombineRunner.
      testHarness.processElement(
          new StreamRecord<>(windowedElement(windowFn, new byte[] {1, 2}, 1L, 0L)));
      testHarness.processElement(
          new StreamRecord<>(windowedElement(windowFn, new byte[] {1, 2}, 2L, 5L)));
      // Separate session [20, 30) for the same key [1, 2].
      testHarness.processElement(
          new StreamRecord<>(windowedElement(windowFn, new byte[] {1, 2}, 10L, 20L)));
      testHarness.processElement(
          new StreamRecord<>(windowedElement(windowFn, new byte[] {3}, 20L, 3L)));

      for (WindowedValue<KV<byte[], Long>> value :
          StreamRecordStripper.<KV<byte[], Long>>stripStreamRecordFromWindowedValue(
              testHarness.getOutput())) {
        IntervalWindow w = (IntervalWindow) Iterables.getOnlyElement(value.getWindows());
        output.add(
            KV.of(
                Arrays.toString(value.getValue().getKey())
                    + "@["
                    + w.start().getMillis()
                    + ","
                    + w.end().getMillis()
                    + ")",
                value.getValue().getValue()));
      }
    }

    assertThat(
        output,
        containsInAnyOrder(
            KV.of("[1, 2]@[0,15)", 3L), KV.of("[1, 2]@[20,30)", 10L), KV.of("[3]@[3,13)", 20L)));
  }

  @Test
  public void combinesNullableStructuralKeys() throws Exception {
    List<KV<byte @Nullable [], Long>> input =
        Arrays.asList(
            KV.of(null, 1L),
            KV.of(new byte[] {1, 2}, 10L),
            KV.of(null, 2L),
            KV.of(new byte[] {1, 2}, 20L));

    List<KV<String, Long>> output = new ArrayList<>();
    for (KV<byte @Nullable [], Long> kv :
        preCombine(NullableCoder.of(ByteArrayCoder.of()), input)) {
      output.add(KV.of(kv.getKey() == null ? "null" : Arrays.toString(kv.getKey()), kv.getValue()));
    }

    assertThat(output, containsInAnyOrder(KV.of("null", 3L), KV.of("[1, 2]", 30L)));
  }
}
