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
 * Checks that the runner's windowing is driven by the language-neutral windowing strategy in the
 * pipeline proto, not by anything specific to the Java SDK.
 *
 * <p>This matters because the runner executes GroupAlsoByWindow itself (see {@link
 * WindowedGroupByKeyProcessor}), so it has to reconstruct the WindowFn from the proto. Beam encodes
 * the standard WindowFns as a URN plus a parameter payload — {@code
 * beam:window_fn:fixed_windows:v1} and friends — and only falls back to a serialized Java object
 * for a custom WindowFn. An SDK in another language emits exactly those same standard URNs, so if
 * the runner works from the URN form it works for any SDK, and if it had come to depend on the
 * Java-serialized form it would only ever have worked for Java.
 *
 * <p>These tests assert the proto a windowed pipeline produces carries the standard URN, and that
 * hydrating it back — which is what the translator does — yields the right WindowFn. They do not
 * replace running a pipeline from another SDK end to end, which additionally exercises the coders
 * and the SDK harness; that needs the job server against a real broker.
 */
public class PortableWindowingStrategyTest {

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
  public void fixedWindowsTravelAsTheStandardUrnNotSerializedJava() {
    RunnerApi.WindowingStrategy strategy =
        nonGlobalStrategy(windowedPipelineProto(FixedWindows.of(WINDOW_SIZE)));

    // The same bytes any SDK would send for FixedWindows.
    assertThat(strategy.getWindowFn().getUrn(), is(WindowingStrategyTranslation.FIXED_WINDOWS_URN));
    assertThat(
        strategy.getWindowFn().getUrn(),
        is(not(WindowingStrategyTranslation.SERIALIZED_JAVA_WINDOWFN_URN)));
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
  public void noWindowingStrategyInAWindowedPipelineNeedsJavaSerialization() {
    RunnerApi.Pipeline proto = windowedPipelineProto(FixedWindows.of(WINDOW_SIZE));

    // If any strategy needed the Java-serialized form, the runner could not reconstruct it for a
    // pipeline submitted from another SDK.
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
    assertThat(
        hydrated.getWindowFn().windowCoder(),
        is(org.apache.beam.sdk.transforms.windowing.IntervalWindow.getCoder()));
  }
}
