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
package org.apache.beam.runners.flink;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.core.Is.is;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.wrappers.streaming.DoFnOperator;
import org.apache.beam.runners.flink.translation.wrappers.streaming.PartialReduceBundleOperator;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterPane;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests {@link FlinkStreamingAggregationsTranslators}. */
@RunWith(JUnit4.class)
public class FlinkStreamingAggregationsTranslatorsTest {

  private static FlinkPipelineOptions options(boolean streaming, boolean sortedBatchCombine) {
    FlinkPipelineOptions options = PipelineOptionsFactory.as(FlinkPipelineOptions.class);
    options.setStreaming(streaming);
    options.setSortedBatchCombine(sortedBatchCombine);
    return options;
  }

  private static boolean eligible(
      FlinkPipelineOptions options,
      WindowingStrategy<?, ?> windowingStrategy,
      List<PCollectionView<?>> sideInputs) {
    return FlinkStreamingAggregationsTranslators.useSortedBatchCombine(
        options, windowingStrategy, sideInputs);
  }

  @Test
  public void usesSortedBatchCombineOnlyWhenEligible() {
    WindowingStrategy<?, ?> global = WindowingStrategy.globalDefault();
    List<PCollectionView<?>> none = Collections.emptyList();

    assertThat(eligible(options(false, true), global, none), is(true));

    assertThat("option off", eligible(options(false, false), global, none), is(false));
    assertThat("streaming", eligible(options(true, true), global, none), is(false));
    assertThat(
        "fixed windows",
        eligible(
            options(false, true),
            WindowingStrategy.of(FixedWindows.of(Duration.standardMinutes(1))),
            none),
        is(false));
    assertThat(
        "custom trigger",
        eligible(
            options(false, true),
            global.withTrigger(Repeatedly.forever(AfterPane.elementCountAtLeast(1))),
            none),
        is(false));

    TestPipeline pipeline = TestPipeline.create();
    PCollectionView<?> view = pipeline.apply(Create.of(1)).apply(View.asSingleton());
    assertThat(
        "side inputs",
        eligible(options(false, true), global, Collections.singletonList(view)),
        is(false));
  }

  @Test
  public void concatenateAsIterableAppendsInPlaceToArrayListAccumulator() {
    FlinkStreamingAggregationsTranslators.ConcatenateAsIterable<Integer> combineFn =
        new FlinkStreamingAggregationsTranslators.ConcatenateAsIterable<>();

    Iterable<Integer> acc = combineFn.createAccumulator();
    Iterable<Integer> afterFirst = combineFn.addInput(acc, 1);
    Iterable<Integer> afterSecond = combineFn.addInput(afterFirst, 2);
    Iterable<Integer> afterThird = combineFn.addInput(afterSecond, 3);

    assertThat(afterFirst, sameInstance(acc));
    assertThat(afterThird, sameInstance(acc));
    assertThat(combineFn.extractOutput(afterThird), contains(1, 2, 3));
  }

  @Test
  public void concatenateAsIterableCopiesNonArrayListAccumulator() {
    FlinkStreamingAggregationsTranslators.ConcatenateAsIterable<Integer> combineFn =
        new FlinkStreamingAggregationsTranslators.ConcatenateAsIterable<>();

    Iterable<Integer> merged =
        combineFn.mergeAccumulators(Arrays.asList(ImmutableList.of(1, 2), ImmutableList.of(3)));
    Iterable<Integer> updated = combineFn.addInput(merged, 4);

    assertThat(combineFn.extractOutput(updated), contains(1, 2, 3, 4));
  }

  @Test
  public void concatenateAsIterablePreGroupsStructuralKeysInPartialReduceOperator()
      throws Exception {
    FlinkPipelineOptions opts = options(false, true);
    opts.setMaxBundleSize(4L);

    Coder<WindowedValue<KV<byte[], Long>>> inputCoder =
        WindowedValues.getFullCoder(
            KvCoder.of(ByteArrayCoder.of(), VarLongCoder.of()), GlobalWindow.Coder.INSTANCE);
    Coder<WindowedValue<KV<byte[], Iterable<Long>>>> accumCoder =
        WindowedValues.getFullCoder(
            KvCoder.of(ByteArrayCoder.of(), IterableCoder.of(VarLongCoder.of())),
            GlobalWindow.Coder.INSTANCE);
    TupleTag<KV<byte[], Iterable<Long>>> mainTag = new TupleTag<>("main output");

    PartialReduceBundleOperator<byte[], Long, Iterable<Long>, Iterable<Long>> operator =
        new PartialReduceBundleOperator<>(
            new FlinkStreamingAggregationsTranslators.ConcatenateAsIterable<>(),
            "GroupByKey",
            inputCoder,
            mainTag,
            emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(
                mainTag, accumCoder, new SerializablePipelineOptions(opts)),
            WindowingStrategy.globalDefault(),
            emptyMap(),
            emptyList(),
            opts);

    List<KV<String, List<Long>>> output = new ArrayList<>();
    try (OneInputStreamOperatorTestHarness<
            WindowedValue<KV<byte[], Long>>, WindowedValue<KV<byte[], Iterable<Long>>>>
        harness = new OneInputStreamOperatorTestHarness<>(operator)) {
      harness.open();
      harness.processElement(
          new StreamRecord<>(WindowedValues.valueInGlobalWindow(KV.of(new byte[] {1, 2}, 10L))));
      harness.processElement(
          new StreamRecord<>(WindowedValues.valueInGlobalWindow(KV.of(new byte[] {3}, 99L))));
      harness.processElement(
          new StreamRecord<>(WindowedValues.valueInGlobalWindow(KV.of(new byte[] {1, 2}, 20L))));
      harness.processElement(
          new StreamRecord<>(WindowedValues.valueInGlobalWindow(KV.of(new byte[] {1, 2}, 30L))));

      for (WindowedValue<KV<byte[], Iterable<Long>>> record : harness.extractOutputValues()) {
        output.add(
            KV.of(
                Arrays.toString(record.getValue().getKey()),
                Lists.newArrayList(record.getValue().getValue())));
      }
    }

    assertThat(
        output,
        containsInAnyOrder(
            KV.of("[1, 2]", Arrays.asList(10L, 20L, 30L)), KV.of("[3]", Arrays.asList(99L))));
  }
}
