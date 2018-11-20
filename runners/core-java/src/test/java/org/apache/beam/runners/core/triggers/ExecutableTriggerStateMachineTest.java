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
package org.apache.beam.runners.core.triggers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ExecutableTriggerStateMachine}. */
@RunWith(JUnit4.class)
public class ExecutableTriggerStateMachineTest {

  @Test
  public void testIndexAssignmentLeaf() throws Exception {
    StubStateMachine t1 = new StubStateMachine();
    ExecutableTriggerStateMachine executable = ExecutableTriggerStateMachine.create(t1);
    assertEquals(0, executable.getTriggerIndex());
  }

  @Test
  public void testIndexAssignmentOneLevel() throws Exception {
    StubStateMachine t1 = new StubStateMachine();
    StubStateMachine t2 = new StubStateMachine();
    StubStateMachine t = new StubStateMachine(t1, t2);

    ExecutableTriggerStateMachine executable = ExecutableTriggerStateMachine.create(t);

    assertEquals(0, executable.getTriggerIndex());
    assertEquals(1, executable.subTriggers().get(0).getTriggerIndex());
    assertSame(t1, executable.subTriggers().get(0).getSpec());
    assertEquals(2, executable.subTriggers().get(1).getTriggerIndex());
    assertSame(t2, executable.subTriggers().get(1).getSpec());
  }

  @Test
  public void testIndexAssignmentTwoLevel() throws Exception {
    StubStateMachine t11 = new StubStateMachine();
    StubStateMachine t12 = new StubStateMachine();
    StubStateMachine t13 = new StubStateMachine();
    StubStateMachine t14 = new StubStateMachine();
    StubStateMachine t21 = new StubStateMachine();
    StubStateMachine t22 = new StubStateMachine();
    StubStateMachine t1 = new StubStateMachine(t11, t12, t13, t14);
    StubStateMachine t2 = new StubStateMachine(t21, t22);
    StubStateMachine t = new StubStateMachine(t1, t2);

    ExecutableTriggerStateMachine executable = ExecutableTriggerStateMachine.create(t);

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

  private static class StubStateMachine extends TriggerStateMachine {

    @SafeVarargs
    protected StubStateMachine(TriggerStateMachine... subTriggers) {
      super(Arrays.asList(subTriggers));
    }

    @Override
    public void onElement(OnElementContext c) throws Exception {}

    @Override
    public void onMerge(OnMergeContext c) throws Exception {}

    @Override
    public void clear(TriggerContext c) throws Exception {}

    @Override
    public boolean shouldFire(TriggerContext c) {
      return false;
    }

    @Override
    public void onFire(TriggerContext c) {}
  }
}
