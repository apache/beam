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

package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link ExecutableTrigger}.
 */
@RunWith(JUnit4.class)
public class ExecutableTriggerTest {

  @Test
  public void testIndexAssignmentLeaf() throws Exception {
    StubTrigger t1 = new StubTrigger();
    ExecutableTrigger<?> executable = ExecutableTrigger.create(t1);
    assertEquals(0, executable.getTriggerIndex());
  }

  @Test
  public void testIndexAssignmentOneLevel() throws Exception {
    StubTrigger t1 = new StubTrigger();
    StubTrigger t2 = new StubTrigger();
    StubTrigger t = new StubTrigger(t1, t2);

    ExecutableTrigger<?> executable = ExecutableTrigger.create(t);

    assertEquals(0, executable.getTriggerIndex());
    assertEquals(1, executable.subTriggers().get(0).getTriggerIndex());
    assertSame(t1, executable.subTriggers().get(0).getSpec());
    assertEquals(2, executable.subTriggers().get(1).getTriggerIndex());
    assertSame(t2, executable.subTriggers().get(1).getSpec());
  }

  @Test
  public void testIndexAssignmentTwoLevel() throws Exception {
    StubTrigger t11 = new StubTrigger();
    StubTrigger t12 = new StubTrigger();
    StubTrigger t13 = new StubTrigger();
    StubTrigger t14 = new StubTrigger();
    StubTrigger t21 = new StubTrigger();
    StubTrigger t22 = new StubTrigger();
    StubTrigger t1 = new StubTrigger(t11, t12, t13, t14);
    StubTrigger t2 = new StubTrigger(t21, t22);
    StubTrigger t = new StubTrigger(t1, t2);

    ExecutableTrigger<?> executable = ExecutableTrigger.create(t);

    assertEquals(0, executable.getTriggerIndex());
    assertEquals(1, executable.subTriggers().get(0).getTriggerIndex());
    assertEquals(6, executable.subTriggers().get(0).getFirstIndexAfterSubtree());
    assertEquals(6, executable.subTriggers().get(1).getTriggerIndex());

    assertSame(t1, executable.getSubTriggerContaining(1).getSpec());
    assertSame(t2, executable.getSubTriggerContaining(6).getSpec());
    assertSame(t1, executable.getSubTriggerContaining(2).getSpec());
    assertSame(t1, executable.getSubTriggerContaining(3).getSpec());
    assertSame(t1, executable.getSubTriggerContaining(5).getSpec());
    assertSame(t2, executable.getSubTriggerContaining(7).getSpec());
  }

  private static class StubTrigger extends Trigger<IntervalWindow> {

    @SafeVarargs
    protected StubTrigger(Trigger<IntervalWindow>... subTriggers) {
      super(Arrays.asList(subTriggers));
    }

    @Override
    public void onElement(OnElementContext c) throws Exception { }

    @Override
    public void onMerge(OnMergeContext c) throws Exception { }

    @Override
    public void clear(TriggerContext c) throws Exception {
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(IntervalWindow window) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }

    @Override
    public boolean isCompatible(Trigger<?> other) {
      return false;
    }

    @Override
    public Trigger<IntervalWindow> getContinuationTrigger(
        List<Trigger<IntervalWindow>> continuationTriggers) {
      return this;
    }

    @Override
    public boolean shouldFire(TriggerContext c) {
      return false;
    }

    @Override
    public void onFire(TriggerContext c) { }
  }
}
