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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
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

  /** Runs one bundle containing {@code input} through the operator and returns its output. */
  private static <K> List<KV<K, Long>> preCombine(Coder<K> keyCoder, List<KV<K, Long>> input)
      throws Exception {
    FlinkPipelineOptions options = FlinkPipelineOptions.defaults();
    options.setStreaming(false);
    // Finish the bundle, and so emit the partial results, right after the last input.
    options.setMaxBundleSize((long) input.size());

    Coder<GlobalWindow> windowCoder = GlobalWindow.Coder.INSTANCE;
    Coder<WindowedValue<KV<K, Long>>> windowedCoder =
        WindowedValues.getFullCoder(KvCoder.of(keyCoder, VarLongCoder.of()), windowCoder);
    TupleTag<KV<K, Long>> mainTag = new TupleTag<>("main-output");

    PartialReduceBundleOperator<K, Long, Long, Long> operator =
        new PartialReduceBundleOperator<>(
            new SumLongs(),
            "stepName",
            windowedCoder,
            mainTag,
            emptyList(),
            new DoFnOperator.MultiOutputOutputManagerFactory<>(
                mainTag, windowedCoder, new SerializablePipelineOptions(options)),
            WindowingStrategy.globalDefault(),
            emptyMap(),
            emptyList(),
            options);

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
}
