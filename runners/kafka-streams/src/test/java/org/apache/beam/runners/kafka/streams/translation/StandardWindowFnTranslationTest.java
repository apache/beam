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
package org.apache.beam.runners.kafka.streams.translation;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.kafka.streams.KafkaStreamsTestRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.construction.PipelineTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.util.construction.WindowingStrategyTranslation;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.joda.time.Duration;
import org.junit.Test;

/**
 * Checks that the runner reconstructs the standard WindowFns from the language-neutral windowing
 * strategy in the pipeline proto.
 *
 * <p>The runner executes GroupAlsoByWindow itself (see {@link WindowedGroupByKeyProcessor}), so it
 * has to rebuild the WindowFn from the proto rather than call into the SDK. Beam gives the standard
 * WindowFns a URN and a parameter payload — {@code beam:window_fn:fixed_windows:v1} and friends —
 * and those are what {@link org.apache.beam.runners.core.ReduceFnRunner} interprets directly. Every
 * SDK emits the same URNs for them, so a pipeline built in another language that uses fixed,
 * sliding, session or global windows produces a strategy this runner can rebuild; these tests pin
 * that down.
 *
 * <p>What this does <em>not</em> cover is a WindowFn the user wrote themselves. That cannot be
 * interpreted runner-side at all: it is opaque to the runner and would have to be executed through
 * the SDK harness that owns it. The runner does not support that today — {@code
 * WindowingStrategyTranslation.windowFnFromProto} rejects an unrecognised URN — and it is the case
 * that would really exercise cross-language windowing.
 */
public class StandardWindowFnTranslationTest {

  private static final Duration WINDOW_SIZE = Duration.millis(10);

  private static class EmitKvFn extends DoFn<byte[], KV<String, Integer>> {
    @ProcessElement
    public void processElement(OutputReceiver<KV<String, Integer>> out) {
      out.output(KV.of("a", 1));
    }
  }

  /** A windowed GroupByKey pipeline, as a proto — the form the runner is handed. */
  private static RunnerApi.Pipeline windowedPipelineProto(WindowFn<Object, ?> windowFn) {
    Pipeline pipeline = Pipeline.create(KafkaStreamsTestRunner.testOptions());
    pipeline
        .apply(Impulse.create())
        .apply(ParDo.of(new EmitKvFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), VarIntCoder.of()))
        .apply(Window.into(windowFn))
        .apply(GroupByKey.create());
    return PipelineTranslation.toProto(pipeline);
  }

  /** The windowing strategy of the GroupByKey's input, which is what the translator reads. */
  private static RunnerApi.WindowingStrategy nonGlobalStrategy(RunnerApi.Pipeline proto) {
    for (RunnerApi.WindowingStrategy strategy :
        proto.getComponents().getWindowingStrategiesMap().values()) {
      if (!strategy
          .getWindowFn()
          .getUrn()
          .equals(WindowingStrategyTranslation.GLOBAL_WINDOWS_URN)) {
        return strategy;
      }
    }
    throw new AssertionError("pipeline has no non-global windowing strategy");
  }

  @Test
  public void fixedWindowsTravelAsTheStandardUrn() {
    RunnerApi.WindowingStrategy strategy =
        nonGlobalStrategy(windowedPipelineProto(FixedWindows.of(WINDOW_SIZE)));

    // The same URN and payload any SDK emits for fixed windows.
    assertThat(strategy.getWindowFn().getUrn(), is(WindowingStrategyTranslation.FIXED_WINDOWS_URN));
  }

  @Test
  public void slidingWindowsTravelAsTheStandardUrn() {
    RunnerApi.WindowingStrategy strategy =
        nonGlobalStrategy(
            windowedPipelineProto(SlidingWindows.of(WINDOW_SIZE).every(Duration.millis(5))));

    assertThat(
        strategy.getWindowFn().getUrn(), is(WindowingStrategyTranslation.SLIDING_WINDOWS_URN));
  }

  @Test
  public void aStandardWindowFnNeedsNoJavaSerialization() {
    RunnerApi.Pipeline proto = windowedPipelineProto(FixedWindows.of(WINDOW_SIZE));

    // Java serialization is the fallback for a WindowFn with no standard URN. A strategy that fell
    // back to it here would only be reconstructable by a Java runner reading a Java pipeline.
    for (RunnerApi.WindowingStrategy strategy :
        proto.getComponents().getWindowingStrategiesMap().values()) {
      assertThat(
          strategy.getWindowFn().getUrn(),
          is(not(WindowingStrategyTranslation.SERIALIZED_JAVA_WINDOWFN_URN)));
    }
  }

  @Test
  public void hydratingTheStandardUrnRebuildsTheWindowFnTheRunnerRunsWith() throws Exception {
    RunnerApi.Pipeline proto = windowedPipelineProto(FixedWindows.of(WINDOW_SIZE));
    RunnerApi.WindowingStrategy strategy = nonGlobalStrategy(proto);

    // The translator's own path: rebuild the strategy from the proto alone.
    WindowingStrategy<?, ?> hydrated =
        WindowingStrategyTranslation.fromProto(
            strategy, RehydratedComponents.forComponents(proto.getComponents()));

    assertThat(hydrated.getWindowFn(), instanceOf(FixedWindows.class));
    assertThat(((FixedWindows) hydrated.getWindowFn()).getSize(), is(WINDOW_SIZE));
    // The window coder the runner encodes state and timers with comes from this WindowFn, so it is
    // the standard interval-window coder rather than anything SDK-specific.
    assertThat(hydrated.getWindowFn().windowCoder(), is(IntervalWindow.getCoder()));
  }
}
