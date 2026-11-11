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
import static org.apache.beam.runners.flink.translation.wrappers.streaming.StreamRecordStripper.stripStreamRecordFromWindowedValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.core.Is.is;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.SystemReduceFn;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.core.metrics.MetricsContainerImpl;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.runners.flink.adapter.FlinkKey;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator.MultiOutputOutputManagerFactory;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.metrics.MetricName;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.CombineWithContext;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.util.AppliedCombineFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowedValues.FullWindowedValueCoder;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link SortedBatchCombineOperator}. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
  "unchecked"
})
public class SortedBatchCombineOperatorTest {

  private static final String STEP = "stepName";

  /** Input grouped by key, as Flink provides it in batch: (key, value, timestamp). */
  private static List<long[]> sortedInput() {
    List<long[]> input = new ArrayList<>();
    input.add(new long[] {1, 10, 5});
    // More inputs than MergingCombiner.MERGE_BATCH_SIZE.
    for (int i = 0; i < 40; i++) {
      input.add(new long[] {2, i, 100 - i});
    }
    input.add(new long[] {3, 1, 7});
    input.add(new long[] {3, 2, 3});
    input.add(new long[] {3, 3, 9});
    return input;
  }

  private static FlinkPipelineOptions options() {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setStreaming(false);
    return options;
  }

  private static <InputT, OutputT>
      KeyedOneInputStreamOperatorTestHarness<
              FlinkKey, WindowedValue<KV<Long, InputT>>, WindowedValue<KV<Long, OutputT>>>
          harness(
              OneInputStreamOperator<
                      WindowedValue<KV<Long, InputT>>, WindowedValue<KV<Long, OutputT>>>
                  operator)
              throws Exception {
    return new KeyedOneInputStreamOperatorTestHarness<>(
        operator,
        new KvToFlinkKeyKeySelector<>(VarLongCoder.of()),
        ValueTypeInfo.of(FlinkKey.class));
  }

  private static <T> WindowedValue<KV<Long, T>> element(long key, T value, long timestamp) {
    return WindowedValues.of(
        KV.of(key, value), new Instant(timestamp), GlobalWindow.INSTANCE, PaneInfo.NO_FIRING);
  }

  /** The operator used today for the final step: WindowDoFnOperator with a combining ReduceFn. */
  private static WindowDoFnOperator<Long, Long, Long> windowDoFnOperator(
      TimestampCombiner timestampCombiner) {
    WindowingStrategy<Object, GlobalWindow> windowingStrategy =
        (WindowingStrategy)
            WindowingStrategy.globalDefault().withTimestampCombiner(timestampCombiner);
    TupleTag<KV<Long, Long>> outputTag = new TupleTag<>("main-output");
    SystemReduceFn<Long, Long, long[], Long, BoundedWindow> reduceFn =
        SystemReduceFn.combining(
            VarLongCoder.of(),
            AppliedCombineFn.withInputCoder(
                Sum.ofLongs(),
                CoderRegistry.createDefault(),
                KvCoder.of(VarLongCoder.of(), VarLongCoder.of())));
    Coder<GlobalWindow> windowCoder = GlobalWindow.Coder.INSTANCE;
    FullWindowedValueCoder<KeyedWorkItem<Long, Long>> inputCoder =
        WindowedValues.getFullCoder(
            SingletonKeyedWorkItemCoder.of(VarLongCoder.of(), VarLongCoder.of(), windowCoder),
            windowCoder);
    FullWindowedValueCoder<KV<Long, Long>> outputCoder =
        WindowedValues.getFullCoder(KvCoder.of(VarLongCoder.of(), VarLongCoder.of()), windowCoder);
    FlinkPipelineOptions options = options();
    return new WindowDoFnOperator<Long, Long, Long>(
        reduceFn,
        STEP,
        (Coder) inputCoder,
        outputTag,
        emptyList(),
        new MultiOutputOutputManagerFactory<>(
            outputTag, outputCoder, new SerializablePipelineOptions(options)),
        windowingStrategy,
        emptyMap(),
        emptyList(),
        options,
        VarLongCoder.of(),
        new WorkItemKeySelector(VarLongCoder.of()));
  }

  private static <AccumT, OutputT> SortedBatchCombineOperator<Long, AccumT, OutputT> sortedOperator(
      SortedBatchCombineOperator.KeyCombiner<AccumT, OutputT> combiner,
      TimestampCombiner timestampCombiner,
      FlinkPipelineOptions options) {
    return new SortedBatchCombineOperator<>(
        STEP, combiner, timestampCombiner, new SerializablePipelineOptions(options));
  }

  @Test
  public void producesTheSameOutputAsWindowDoFnOperator() throws Exception {
    for (TimestampCombiner timestampCombiner : TimestampCombiner.values()) {
      KeyedOneInputStreamOperatorTestHarness<
              FlinkKey, WindowedValue<KV<Long, Long>>, WindowedValue<KV<Long, Long>>>
          expectedHarness = harness(windowDoFnOperator(timestampCombiner));
      expectedHarness.open();
      for (long[] e : sortedInput()) {
        expectedHarness.processElement(element(e[0], e[1], e[2]), e[2]);
      }
      expectedHarness.processWatermark(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
      List<WindowedValue<KV<Long, Long>>> expected = new ArrayList<>();
      stripStreamRecordFromWindowedValue(expectedHarness.getOutput())
          .forEach(v -> expected.add((WindowedValue<KV<Long, Long>>) (WindowedValue) v));
      expectedHarness.close();

      // The final step receives the accumulators of the partial combine.
      KeyedOneInputStreamOperatorTestHarness<
              FlinkKey, WindowedValue<KV<Long, long[]>>, WindowedValue<KV<Long, Long>>>
          harness =
              harness(
                  sortedOperator(
                      new SortedBatchCombineOperator.MergingCombiner<>(Sum.ofLongs()),
                      timestampCombiner,
                      options()));
      harness.open();
      for (long[] e : sortedInput()) {
        harness.processElement(element(e[0], new long[] {e[1]}, e[2]), e[2]);
      }
      harness.endInput();
      List<WindowedValue<KV<Long, Long>>> actual = new ArrayList<>();
      stripStreamRecordFromWindowedValue(harness.getOutput())
          .forEach(v -> actual.add((WindowedValue<KV<Long, Long>>) (WindowedValue) v));
      harness.close();

      assertThat(timestampCombiner.toString(), expected.size(), is(3));
      assertThat(timestampCombiner.toString(), actual, containsInAnyOrder(expected.toArray()));
    }
  }

  @Test
  public void bufferingCombinerKeepsAllAccumulatorsInOrder() throws Exception {
    KeyedOneInputStreamOperatorTestHarness<
            FlinkKey,
            WindowedValue<KV<Long, Iterable<Long>>>,
            WindowedValue<KV<Long, Iterable<Iterable<Long>>>>>
        harness =
            harness(
                sortedOperator(
                    new SortedBatchCombineOperator.BufferingCombiner<Iterable<Long>>(),
                    TimestampCombiner.END_OF_WINDOW,
                    options()));
    harness.open();
    harness.processElement(element(1L, Arrays.asList(1L, 2L), 0), 0);
    harness.processElement(element(1L, Arrays.asList(3L), 0), 0);
    harness.processElement(element(2L, Arrays.asList(4L), 0), 0);
    harness.endInput();

    List<KV<Long, List<List<Long>>>> actual = new ArrayList<>();
    for (Object o : stripStreamRecordFromWindowedValue(harness.getOutput())) {
      KV<Long, Iterable<Iterable<Long>>> kv =
          ((WindowedValue<KV<Long, Iterable<Iterable<Long>>>>) o).getValue();
      List<List<Long>> lists = new ArrayList<>();
      kv.getValue().forEach(l -> lists.add(new ArrayList<>((List<Long>) l)));
      actual.add(KV.of(kv.getKey(), lists));
    }
    harness.close();

    assertThat(
        actual,
        is(
            Arrays.asList(
                KV.of(1L, Arrays.asList(Arrays.asList(1L, 2L), Arrays.asList(3L))),
                KV.of(2L, Arrays.asList(Arrays.asList(4L))))));
  }

  /** Sums accumulators and appends the job name, read from the combine context. */
  private static class JobNameSum
      extends CombineWithContext.CombineFnWithContext<Long, List<Long>, String> {
    @Override
    public List<Long> createAccumulator(CombineWithContext.Context c) {
      return new ArrayList<>();
    }

    @Override
    public List<Long> addInput(List<Long> accumulator, Long input, CombineWithContext.Context c) {
      accumulator.add(input);
      return accumulator;
    }

    @Override
    public List<Long> mergeAccumulators(
        Iterable<List<Long>> accumulators, CombineWithContext.Context c) {
      List<Long> merged = new ArrayList<>();
      accumulators.forEach(merged::addAll);
      return merged;
    }

    @Override
    public String extractOutput(List<Long> accumulator, CombineWithContext.Context c) {
      long sum = accumulator.stream().mapToLong(Long::longValue).sum();
      return c.getPipelineOptions().getJobName() + ":" + sum;
    }
  }

  @Test
  public void providesPipelineOptionsToCombineFnsWithContext() throws Exception {
    FlinkPipelineOptions options = options();
    options.setJobName("job");
    KeyedOneInputStreamOperatorTestHarness<
            FlinkKey, WindowedValue<KV<Long, List<Long>>>, WindowedValue<KV<Long, String>>>
        harness =
            harness(
                sortedOperator(
                    new SortedBatchCombineOperator.MergingCombiner<>(new JobNameSum()),
                    TimestampCombiner.END_OF_WINDOW,
                    options));
    harness.open();
    harness.processElement(element(1L, new ArrayList<>(Arrays.asList(1L, 2L)), 0), 0);
    harness.processElement(element(1L, new ArrayList<>(Arrays.asList(4L)), 0), 0);
    harness.endInput();

    List<WindowedValue<KV<Long, String>>> actual = new ArrayList<>();
    stripStreamRecordFromWindowedValue(harness.getOutput())
        .forEach(v -> actual.add((WindowedValue<KV<Long, String>>) (WindowedValue) v));
    harness.close();

    assertThat(
        actual,
        is(
            Arrays.asList(
                WindowedValues.of(
                    KV.of(1L, "job:7"),
                    GlobalWindow.INSTANCE.maxTimestamp(),
                    GlobalWindow.INSTANCE,
                    PaneInfo.ON_TIME_AND_ONLY_FIRING))));
  }

  /** A sum that counts how often accumulators are merged. */
  private static class CountingSum extends Combine.BinaryCombineLongFn {
    @Override
    public long apply(long left, long right) {
      Metrics.counter("test", "merges").inc();
      return left + right;
    }

    @Override
    public long identity() {
      return 0;
    }
  }

  @Test
  public void reportsMetricsOfTheCombineFn() throws Exception {
    SortedBatchCombineOperator<Long, long[], Long> operator =
        sortedOperator(
            new SortedBatchCombineOperator.MergingCombiner<>(new CountingSum()),
            TimestampCombiner.END_OF_WINDOW,
            options());
    KeyedOneInputStreamOperatorTestHarness<
            FlinkKey, WindowedValue<KV<Long, long[]>>, WindowedValue<KV<Long, Long>>>
        harness = harness(operator);
    harness.open();
    harness.processElement(element(1L, new long[] {1}, 0), 0);
    harness.processElement(element(1L, new long[] {2}, 0), 0);
    harness.endInput();

    MetricsContainerImpl container = operator.metricContainer.getMetricsContainer(STEP);
    assertThat(
        container.getCounter(MetricName.named("test", "merges")).getCumulative() > 0, is(true));
    harness.close();
  }

  @Test
  public void emitsTheLastKeyBeforeTheFinalWatermark() throws Exception {
    KeyedOneInputStreamOperatorTestHarness<
            FlinkKey, WindowedValue<KV<Long, long[]>>, WindowedValue<KV<Long, Long>>>
        harness =
            harness(
                sortedOperator(
                    new SortedBatchCombineOperator.MergingCombiner<>(Sum.ofLongs()),
                    TimestampCombiner.END_OF_WINDOW,
                    options()));
    harness.open();
    harness.processElement(element(1L, new long[] {1}, 0), 0);
    // Intermediate watermarks are held back, as the pending output may be behind them.
    harness.processWatermark(10L);
    harness.processElement(element(1L, new long[] {2}, 0), 0);
    harness.processWatermark(Watermark.MAX_WATERMARK);
    harness.endInput();

    List<Object> output = new ArrayList<>(harness.getOutput());
    harness.close();

    assertThat(output.size(), is(2));
    assertThat(
        ((StreamRecord<WindowedValue<KV<Long, Long>>>) output.get(0)).getValue().getValue(),
        is(KV.of(1L, 3L)));
    assertThat(output.get(1), is(Watermark.MAX_WATERMARK));
  }

  @Test
  public void emitsNothingForEmptyInput() throws Exception {
    KeyedOneInputStreamOperatorTestHarness<
            FlinkKey, WindowedValue<KV<Long, long[]>>, WindowedValue<KV<Long, Long>>>
        harness =
            harness(
                sortedOperator(
                    new SortedBatchCombineOperator.MergingCombiner<>(Sum.ofLongs()),
                    TimestampCombiner.END_OF_WINDOW,
                    options()));
    harness.open();
    harness.endInput();
    assertThat(stripStreamRecordFromWindowedValue(harness.getOutput()), emptyIterable());
    harness.close();
  }
}
