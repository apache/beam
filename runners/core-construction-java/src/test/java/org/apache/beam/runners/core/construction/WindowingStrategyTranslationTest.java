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
package org.apache.beam.runners.core.construction;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.auto.value.AutoValue;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Sessions;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Unit tests for {@link WindowingStrategy}. */
@RunWith(Parameterized.class)
public class WindowingStrategyTranslationTest {

  // Each spec activates tests of all subsets of its fields
  @AutoValue
  abstract static class ToProtoAndBackSpec {
    abstract WindowingStrategy getWindowingStrategy();
  }

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
                .withTimestampCombiner(TimestampCombiner.LATEST)));
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
            windowingStrategy.withEnvironmentId(components.getOnlyEnvironmentId()).fixDefaults()));

    protoComponents.getCoder(
        components.registerCoder(windowingStrategy.getWindowFn().windowCoder()));
    assertThat(
        proto.getAssignsToOneWindow(),
        equalTo(windowingStrategy.getWindowFn().assignsToOneWindow()));
  }
}
