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
package org.apache.beam.sdk.util.construction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.transforms.windowing.WindowMappingFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Unit tests for {@link WindowingStrategy}. */
@RunWith(Enclosed.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class WindowingStrategyTranslationTest {
  // Each spec activates tests of all subsets of its fields
  @AutoValue
  @AutoValue.CopyAnnotations
  @SuppressWarnings({"rawtypes"})
  abstract static class ToProtoAndBackSpec {
    abstract WindowingStrategy getWindowingStrategy();
  }

  @RunWith(Parameterized.class)
  public static class ToProtoAndBackTests {

    private static ToProtoAndBackSpec toProtoAndBackSpec(WindowingStrategy windowingStrategy) {
      return new AutoValue_WindowingStrategyTranslationTest_ToProtoAndBackSpec(windowingStrategy);
    }

    private static final WindowFn<?, ?> REPRESENTATIVE_WINDOW_FN =
        FixedWindows.of(Duration.millis(12));

    private static final Trigger REPRESENTATIVE_TRIGGER = AfterWatermark.pastEndOfWindow();

    @Parameters(name = "{index}: {0}")
    public static Iterable<ToProtoAndBackSpec> data() {
      return ImmutableList.of(
          toProtoAndBackSpec(WindowingStrategy.globalDefault()),
          toProtoAndBackSpec(
              WindowingStrategy.of(
                  FixedWindows.of(Duration.millis(11)).withOffset(Duration.millis(3)))),
          toProtoAndBackSpec(
              WindowingStrategy.of(
                  SlidingWindows.of(Duration.millis(37))
                      .every(Duration.millis(3))
                      .withOffset(Duration.millis(2)))),
          toProtoAndBackSpec(WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(389)))),
          toProtoAndBackSpec(
              WindowingStrategy.of(REPRESENTATIVE_WINDOW_FN)
                  .withClosingBehavior(ClosingBehavior.FIRE_ALWAYS)
                  .withMode(AccumulationMode.ACCUMULATING_FIRED_PANES)
                  .withTrigger(REPRESENTATIVE_TRIGGER)
                  .withAllowedLateness(Duration.millis(71))
                  .withTimestampCombiner(TimestampCombiner.EARLIEST)),
          toProtoAndBackSpec(
              WindowingStrategy.of(REPRESENTATIVE_WINDOW_FN)
                  .withClosingBehavior(ClosingBehavior.FIRE_IF_NON_EMPTY)
                  .withMode(AccumulationMode.DISCARDING_FIRED_PANES)
                  .withTrigger(REPRESENTATIVE_TRIGGER)
                  .withAllowedLateness(Duration.millis(93))
                  .withTimestampCombiner(TimestampCombiner.LATEST)),
          toProtoAndBackSpec(
              WindowingStrategy.of(REPRESENTATIVE_WINDOW_FN)
                  .withClosingBehavior(ClosingBehavior.FIRE_IF_NON_EMPTY)
                  .withMode(AccumulationMode.RETRACTING_FIRED_PANES)
                  .withTrigger(REPRESENTATIVE_TRIGGER)
                  .withAllowedLateness(Duration.millis(100))
                  .withTimestampCombiner(TimestampCombiner.LATEST)),
          toProtoAndBackSpec(WindowingStrategy.of(new CustomWindowFn())));
    }

    @Parameter(0)
    public ToProtoAndBackSpec toProtoAndBackSpec;

    @Test
    public void testToProtoAndBack() throws Exception {
      WindowingStrategy<?, ?> windowingStrategy = toProtoAndBackSpec.getWindowingStrategy();
      SdkComponents components = SdkComponents.create();
      components.registerEnvironment(Environments.createDockerEnvironment("java"));
      WindowingStrategy<?, ?> toProtoAndBackWindowingStrategy =
          WindowingStrategyTranslation.fromProto(
              WindowingStrategyTranslation.toMessageProto(windowingStrategy, components));

      assertThat(
          toProtoAndBackWindowingStrategy,
          equalTo(
              (WindowingStrategy)
                  windowingStrategy
                      .withEnvironmentId(components.getOnlyEnvironmentId())
                      .fixDefaults()));
    }

    @Test
    public void testToProtoAndBackWithComponents() throws Exception {
      WindowingStrategy<?, ?> windowingStrategy = toProtoAndBackSpec.getWindowingStrategy();
      SdkComponents components = SdkComponents.create();
      components.registerEnvironment(Environments.createDockerEnvironment("java"));
      RunnerApi.WindowingStrategy proto =
          WindowingStrategyTranslation.toProto(windowingStrategy, components);
      RehydratedComponents protoComponents =
          RehydratedComponents.forComponents(components.toComponents());

      assertThat(
          WindowingStrategyTranslation.fromProto(proto, protoComponents).fixDefaults(),
          equalTo(
              windowingStrategy
                  .withEnvironmentId(components.getOnlyEnvironmentId())
                  .fixDefaults()));

      protoComponents.getCoder(
          components.registerCoder(windowingStrategy.getWindowFn().windowCoder()));
      assertThat(
          proto.getAssignsToOneWindow(),
          equalTo(windowingStrategy.getWindowFn().assignsToOneWindow()));
    }
  }

  @RunWith(JUnit4.class)
  public static class ExpectedProtoTests {

    @Test
    public void testSessionsMergeStatus() throws Exception {
      WindowingStrategy<?, ?> windowingStrategy =
          WindowingStrategy.of(Sessions.withGapDuration(Duration.millis(456)));
      SdkComponents components = SdkComponents.create();
      components.registerEnvironment(Environments.createDockerEnvironment("java"));
      RunnerApi.WindowingStrategy proto =
          WindowingStrategyTranslation.toProto(windowingStrategy, components);

      assertThat(proto.getMergeStatus(), equalTo(RunnerApi.MergeStatus.Enum.NEEDS_MERGE));
    }

    @Test
    public void testFixedMergeStatus() throws Exception {
      WindowingStrategy<?, ?> windowingStrategy =
          WindowingStrategy.of(FixedWindows.of(Duration.millis(2)));
      SdkComponents components = SdkComponents.create();
      components.registerEnvironment(Environments.createDockerEnvironment("java"));
      RunnerApi.WindowingStrategy proto =
          WindowingStrategyTranslation.toProto(windowingStrategy, components);

      assertThat(proto.getMergeStatus(), equalTo(RunnerApi.MergeStatus.Enum.NON_MERGING));
    }
  }

  private static class CustomWindow extends IntervalWindow {
    private boolean isBig;

    CustomWindow(Instant start, Instant end, boolean isBig) {
      super(start, end);
      this.isBig = isBig;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CustomWindow that = (CustomWindow) o;
      return super.equals(o) && this.isBig == that.isBig;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), isBig);
    }
  }

  private static class CustomWindowCoder extends CustomCoder<CustomWindow> {

    private static final CustomWindowCoder INSTANCE = new CustomWindowCoder();
    private static final Coder<IntervalWindow> INTERVAL_WINDOW_CODER = IntervalWindow.getCoder();
    private static final VarIntCoder VAR_INT_CODER = VarIntCoder.of();

    public static CustomWindowCoder of() {
      return INSTANCE;
    }

    @Override
    public void encode(CustomWindow window, OutputStream outStream) throws IOException {
      INTERVAL_WINDOW_CODER.encode(window, outStream);
      VAR_INT_CODER.encode(window.isBig ? 1 : 0, outStream);
    }

    @Override
    public CustomWindow decode(InputStream inStream) throws IOException {
      IntervalWindow superWindow = INTERVAL_WINDOW_CODER.decode(inStream);
      boolean isBig = VAR_INT_CODER.decode(inStream) != 0;
      return new CustomWindow(superWindow.start(), superWindow.end(), isBig);
    }

    @Override
    public void verifyDeterministic() throws NonDeterministicException {
      INTERVAL_WINDOW_CODER.verifyDeterministic();
      VAR_INT_CODER.verifyDeterministic();
    }
  }

  private static class CustomWindowFn<T> extends WindowFn<T, CustomWindow> {
    @Override
    public Collection<CustomWindow> assignWindows(AssignContext c) throws Exception {
      String element;
      // It loses genericity of type T but this is not a big deal for a test.
      // And it allows to avoid duplicating CustomWindowFn to support PCollection<KV>
      if (c.element() instanceof KV) {
        element = ((KV<Integer, String>) c.element()).getValue();
      } else {
        element = (String) c.element();
      }
      // put big elements in windows of 30s and small ones in windows of 5s
      if ("big".equals(element)) {
        return Collections.singletonList(
            new CustomWindow(
                c.timestamp(), c.timestamp().plus(Duration.standardSeconds(30)), true));
      } else {
        return Collections.singletonList(
            new CustomWindow(
                c.timestamp(), c.timestamp().plus(Duration.standardSeconds(5)), false));
      }
    }

    @Override
    public void mergeWindows(MergeContext c) throws Exception {
      Map<CustomWindow, Set<CustomWindow>> windowsToMerge = new HashMap<>();
      for (CustomWindow window : c.windows()) {
        if (window.isBig) {
          HashSet<CustomWindow> windows = new HashSet<>();
          windows.add(window);
          windowsToMerge.put(window, windows);
        }
      }
      for (CustomWindow window : c.windows()) {
        for (Map.Entry<CustomWindow, Set<CustomWindow>> bigWindow : windowsToMerge.entrySet()) {
          if (bigWindow.getKey().contains(window)) {
            bigWindow.getValue().add(window);
          }
        }
      }
      for (Map.Entry<CustomWindow, Set<CustomWindow>> mergeEntry : windowsToMerge.entrySet()) {
        c.merge(mergeEntry.getValue(), mergeEntry.getKey());
      }
    }

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return other instanceof CustomWindowFn;
    }

    @Override
    public Coder<CustomWindow> windowCoder() {
      return CustomWindowCoder.of();
    }

    @Override
    public WindowMappingFn<CustomWindow> getDefaultWindowMappingFn() {
      throw new UnsupportedOperationException("side inputs not supported");
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CustomWindowFn windowFn = (CustomWindowFn) o;
      if (this.isCompatible(windowFn)) {
        return true;
      }

      return false;
    }

    @Override
    public int hashCode() {
      // overriding hashCode() is required but it is not useful in terms of
      // writting test cases.
      return Objects.hash("test");
    }
  }
}
