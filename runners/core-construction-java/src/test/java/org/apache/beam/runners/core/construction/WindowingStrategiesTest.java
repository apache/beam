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
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.common.runner.v1.RunnerApi;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

/** Unit tests for {@link WindowingStrategy}. */
@RunWith(Parameterized.class)
public class WindowingStrategiesTest {

  // Each spec activates tests of all subsets of its fields
  @AutoValue
  abstract static class ToProtoAndBackSpec {
    abstract WindowingStrategy getWindowingStrategy();
  }

  private static ToProtoAndBackSpec toProtoAndBackSpec(WindowingStrategy windowingStrategy) {
    return new AutoValue_WindowingStrategiesTest_ToProtoAndBackSpec(windowingStrategy);
  }

  private static final WindowFn<?, ?> REPRESENTATIVE_WINDOW_FN =
      FixedWindows.of(Duration.millis(12));

  private static final Trigger REPRESENTATIVE_TRIGGER = AfterWatermark.pastEndOfWindow();

  @Parameters(name = "{index}: {0}")
  public static Iterable<ToProtoAndBackSpec> data() {
    return ImmutableList.of(
        toProtoAndBackSpec(WindowingStrategy.globalDefault()),
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
                .withTimestampCombiner(TimestampCombiner.LATEST)));
  }

  @Parameter(0)
  public ToProtoAndBackSpec toProtoAndBackSpec;

  @Test
  public void testToProtoAndBack() throws Exception {
    WindowingStrategy<?, ?> windowingStrategy = toProtoAndBackSpec.getWindowingStrategy();
    WindowingStrategy<?, ?> toProtoAndBackWindowingStrategy =
        WindowingStrategies.fromProto(WindowingStrategies.toProto(windowingStrategy));

    assertThat(
        toProtoAndBackWindowingStrategy,
        equalTo((WindowingStrategy) windowingStrategy.fixDefaults()));
  }

  @Test
  public void testToProtoAndBackWithComponents() throws Exception {
    WindowingStrategy<?, ?> windowingStrategy = toProtoAndBackSpec.getWindowingStrategy();
    SdkComponents components = SdkComponents.create();
    RunnerApi.WindowingStrategy proto =
        WindowingStrategies.toProto(windowingStrategy, components);
    RunnerApi.Components protoComponents = components.toComponents();

    assertThat(
        WindowingStrategies.fromProto(proto, protoComponents).fixDefaults(),
        Matchers.<WindowingStrategy<?, ?>>equalTo(windowingStrategy.fixDefaults()));

    protoComponents.getCodersOrThrow(
        components.registerCoder(windowingStrategy.getWindowFn().windowCoder()));
  }
}
