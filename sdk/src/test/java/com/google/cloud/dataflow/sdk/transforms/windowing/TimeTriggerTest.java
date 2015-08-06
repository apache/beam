/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.transforms.windowing;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;

import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Collections;
import java.util.List;

/**
 * Tests for the various time operations in {@link TimeTrigger}.
 */
@RunWith(JUnit4.class)
public class TimeTriggerTest {
  @Test
  public void testAlignTo() {
    TimeTrigger<?> size10 = new TestTimeTrigger().alignedTo(new Duration(10));
    TimeTrigger<?> size10offset5 =
        new TestTimeTrigger().alignedTo(new Duration(10), new Instant(5));

    assertEquals(new Instant(100), size10.computeTargetTimestamp(new Instant(100)));
    assertEquals(new Instant(110), size10.computeTargetTimestamp(new Instant(105)));
    assertEquals(new Instant(105), size10offset5.computeTargetTimestamp(new Instant(105)));
    assertEquals(new Instant(115), size10offset5.computeTargetTimestamp(new Instant(110)));
  }

  private static class TestTimeTrigger extends TimeTrigger<IntervalWindow> {

    private static final long serialVersionUID = 0L;

    private TestTimeTrigger() {
      this(Collections.<SerializableFunction<Instant, Instant>>emptyList());
    }

    private TestTimeTrigger(List<SerializableFunction<Instant, Instant>> timestampMappers) {
      super(timestampMappers);
    }

    @Override
    public Instant computeTargetTimestamp(Instant time) {
      return super.computeTargetTimestamp(time);
    }

    @Override
    protected TestTimeTrigger newWith(List<SerializableFunction<Instant, Instant>> transform) {
      return new TestTimeTrigger(transform);
    }

    @Override
    public TriggerResult onElement(OnElementContext c) throws Exception {
      return null;
    }

    @Override
    public MergeResult onMerge(OnMergeContext c) throws Exception {
      return null;
    }

    @Override
    public TriggerResult onTimer(OnTimerContext c) throws Exception {
      return null;
    }

    @Override
    protected Trigger<IntervalWindow> getContinuationTrigger(
        List<Trigger<IntervalWindow>> continuationTriggers) {
      return null;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(IntervalWindow window) {
      return null;
    }

  }
}
