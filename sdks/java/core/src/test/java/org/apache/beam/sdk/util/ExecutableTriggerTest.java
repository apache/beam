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
package org.apache.beam.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Trigger;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ExecutableTrigger}.
 */
@RunWith(JUnit4.class)
public class ExecutableTriggerTest {

  @Test
  public void testIndexAssignmentLeaf() throws Exception {
    StubTrigger t1 = new StubTrigger();
    ExecutableTrigger executable = ExecutableTrigger.create(t1);
    assertEquals(0, executable.getTriggerIndex());
  }

  @Test
  public void testIndexAssignmentOneLevel() throws Exception {
    StubTrigger t1 = new StubTrigger();
    StubTrigger t2 = new StubTrigger();
    StubTrigger t = new StubTrigger(t1, t2);

    ExecutableTrigger executable = ExecutableTrigger.create(t);

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

    ExecutableTrigger executable = ExecutableTrigger.create(t);

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

  private static class StubTrigger extends Trigger {

    @SafeVarargs
    protected StubTrigger(Trigger... subTriggers) {
      super(Arrays.asList(subTriggers));
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(BoundedWindow window) {
      return BoundedWindow.TIMESTAMP_MAX_VALUE;
    }

    @Override
    public boolean isCompatible(Trigger other) {
      return false;
    }

    @Override
    public Trigger getContinuationTrigger(List<Trigger> continuationTriggers) {
      return this;
    }
  }
}
