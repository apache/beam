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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link TriggerStateMachine}. */
@RunWith(JUnit4.class)
public class TriggerStateMachineTest {

  @Test
  public void testTriggerToString() throws Exception {
    assertEquals(
        "AfterWatermark.pastEndOfWindow()",
        AfterWatermarkStateMachine.pastEndOfWindow().toString());
    assertEquals(
        "Repeatedly.forever(AfterWatermark.pastEndOfWindow())",
        RepeatedlyStateMachine.forever(AfterWatermarkStateMachine.pastEndOfWindow()).toString());
  }

  @Test
  public void testIsCompatible() throws Exception {
    assertTrue(new Trigger1(null).isCompatible(new Trigger1(null)));
    assertTrue(
        new Trigger1(Arrays.asList(new Trigger2(null)))
            .isCompatible(new Trigger1(Arrays.asList(new Trigger2(null)))));

    assertFalse(new Trigger1(null).isCompatible(new Trigger2(null)));
    assertFalse(
        new Trigger1(Arrays.asList(new Trigger1(null)))
            .isCompatible(new Trigger1(Arrays.asList(new Trigger2(null)))));
  }

  private static class Trigger1 extends TriggerStateMachine {

    private Trigger1(List<TriggerStateMachine> subTriggers) {
      super(subTriggers);
    }

    @Override
    public void prefetchOnElement(PrefetchContext c) {}

    @Override
    public void onElement(TriggerStateMachine.OnElementContext c) {}

    @Override
    public void prefetchOnMerge(MergingPrefetchContext c) {}

    @Override
    public void onMerge(TriggerStateMachine.OnMergeContext c) {}

    @Override
    public void prefetchShouldFire(PrefetchContext c) {}

    @Override
    public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
      return false;
    }

    @Override
    public void onFire(TriggerStateMachine.TriggerContext context) throws Exception {}
  }

  private static class Trigger2 extends TriggerStateMachine {

    private Trigger2(List<TriggerStateMachine> subTriggers) {
      super(subTriggers);
    }

    @Override
    public void prefetchOnElement(PrefetchContext c) {}

    @Override
    public void onElement(TriggerStateMachine.OnElementContext c) {}

    @Override
    public void prefetchOnMerge(MergingPrefetchContext c) {}

    @Override
    public void onMerge(TriggerStateMachine.OnMergeContext c) {}

    @Override
    public void prefetchShouldFire(PrefetchContext c) {}

    @Override
    public boolean shouldFire(TriggerStateMachine.TriggerContext context) throws Exception {
      return false;
    }

    @Override
    public void onFire(TriggerStateMachine.TriggerContext context) throws Exception {}
  }
}
