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

import static org.apache.beam.sdk.testing.SerializableMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.state.MapState;
import org.apache.beam.sdk.state.SetState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.state.WatermarkHoldState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;
import org.junit.Test;

/** Test suite for {@link DoFnFeatures}. */
public class DoFnFeaturesTest {

  private interface FeatureTest {
    void test();
  }

  private static class StatelessDoFn extends DoFn<String, String> implements FeatureTest {
    @ProcessElement
    public void process(@Element String input) {}

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(false));
      assertThat(DoFnFeatures.isStateful(this), equalTo(false));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(false));
      assertThat(DoFnFeatures.usesState(this), equalTo(false));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(false));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(false));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(false));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(false));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(false));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(false));
    }
  }

  private static class StatefulWithValueState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @StateId("state")
    private final StateSpec<ValueState<String>> state = StateSpecs.value();

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(false));
      assertThat(DoFnFeatures.isStateful(this), equalTo(true));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(false));
      assertThat(DoFnFeatures.usesState(this), equalTo(true));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(false));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(false));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(false));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(true));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(false));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(false));
    }
  }

  private static class StatefulWithTimers extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @TimerId("timer")
    private final TimerSpec spec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(false));
      assertThat(DoFnFeatures.isStateful(this), equalTo(true));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(true));
      assertThat(DoFnFeatures.usesState(this), equalTo(false));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(false));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(false));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(false));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(false));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(false));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(false));
    }

    @OnTimer("timer")
    public void onTimer() {}
  }

  private static class StatefulWithTimersAndValueState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @TimerId("timer")
    private final TimerSpec timer = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @StateId("state")
    private final StateSpec<SetState<String>> state = StateSpecs.set();

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(false));
      assertThat(DoFnFeatures.isStateful(this), equalTo(true));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(true));
      assertThat(DoFnFeatures.usesState(this), equalTo(true));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(false));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(false));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(true));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(false));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(false));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(false));
    }

    @OnTimer("timer")
    public void onTimer() {}
  }

  private static class StatefulWithSetState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @StateId("state")
    private final StateSpec<SetState<String>> spec = StateSpecs.set();

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(false));
      assertThat(DoFnFeatures.isStateful(this), equalTo(true));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(false));
      assertThat(DoFnFeatures.usesState(this), equalTo(true));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(false));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(false));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(true));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(false));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(false));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(false));
    }
  }

  private static class StatefulWithMapState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @StateId("state")
    private final StateSpec<MapState<String, String>> spec = StateSpecs.map();

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(false));
      assertThat(DoFnFeatures.isStateful(this), equalTo(true));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(false));
      assertThat(DoFnFeatures.usesState(this), equalTo(true));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(false));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(true));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(false));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(false));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(false));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(false));
    }
  }

  private static class StatefulWithWatermarkHoldState extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @StateId("state")
    private final StateSpec<WatermarkHoldState> spec =
        StateSpecs.watermarkStateInternal(TimestampCombiner.LATEST);

    @ProcessElement
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(false));
      assertThat(DoFnFeatures.isStateful(this), equalTo(true));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(false));
      assertThat(DoFnFeatures.usesState(this), equalTo(true));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(false));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(false));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(false));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(false));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(true));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(false));
    }
  }

  private static class RequiresTimeSortedInput extends DoFn<KV<String, String>, String>
      implements FeatureTest {
    @ProcessElement
    @RequiresTimeSortedInput
    public void process(@Element KV<String, String> input) {}

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(false));
      assertThat(DoFnFeatures.isStateful(this), equalTo(true));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(true));
      assertThat(DoFnFeatures.usesState(this), equalTo(true));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(true));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(false));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(false));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(true));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(true));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(true));
    }
  }

  private static class Splittable extends DoFn<KV<String, Long>, String> implements FeatureTest {
    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<OffsetRange, ?> tracker) {}

    @GetInitialRestriction
    public OffsetRange getInitialRange(@Element KV<String, Long> element) {
      return new OffsetRange(0L, element.getValue());
    }

    @Override
    public void test() {
      assertThat(DoFnFeatures.isSplittable(this), equalTo(true));
      assertThat(DoFnFeatures.isStateful(this), equalTo(false));
      assertThat(DoFnFeatures.usesTimers(this), equalTo(false));
      assertThat(DoFnFeatures.usesState(this), equalTo(false));
      assertThat(DoFnFeatures.usesBagState(this), equalTo(false));
      assertThat(DoFnFeatures.usesMapState(this), equalTo(false));
      assertThat(DoFnFeatures.usesSetState(this), equalTo(false));
      assertThat(DoFnFeatures.usesValueState(this), equalTo(false));
      assertThat(DoFnFeatures.usesWatermarkHold(this), equalTo(false));
      assertThat(DoFnFeatures.requiresTimeSortedInput(this), equalTo(false));
    }
  }

  private final List<FeatureTest> tests =
      Lists.newArrayList(
          new StatelessDoFn(),
          new StatefulWithValueState(),
          new StatefulWithTimers(),
          new StatefulWithTimersAndValueState(),
          new StatefulWithSetState(),
          new StatefulWithMapState(),
          new StatefulWithWatermarkHoldState(),
          new RequiresTimeSortedInput(),
          new Splittable());

  @Test
  public void testAllDoFnFeatures() {
    tests.forEach(FeatureTest::test);
  }
}
