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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link Trigger}.
 */
@RunWith(JUnit4.class)
public class TriggerTest {

  @Test
  public void testTriggerToString() throws Exception {
    assertEquals("AfterWatermark.pastEndOfWindow()", AfterWatermark.pastEndOfWindow().toString());
    assertEquals("Repeatedly(AfterWatermark.pastEndOfWindow())",
        Repeatedly.forever(AfterWatermark.pastEndOfWindow()).toString());
  }

  @Test
  public void testIsCompatible() throws Exception {
    assertTrue(new Trigger1(null).isCompatible(new Trigger1(null)));
    assertTrue(new Trigger1(Arrays.<Trigger<IntervalWindow>>asList(new Trigger2(null)))
        .isCompatible(new Trigger1(Arrays.<Trigger<IntervalWindow>>asList(new Trigger2(null)))));

    assertFalse(new Trigger1(null).isCompatible(new Trigger2(null)));
    assertFalse(new Trigger1(Arrays.<Trigger<IntervalWindow>>asList(new Trigger1(null)))
        .isCompatible(new Trigger1(Arrays.<Trigger<IntervalWindow>>asList(new Trigger2(null)))));
  }

  private static class Trigger1 extends Trigger<IntervalWindow> {

    private Trigger1(List<Trigger<IntervalWindow>> subTriggers) {
      super(subTriggers);
    }

    @Override
    public void onElement(Trigger<IntervalWindow>.OnElementContext c) { }

    @Override
    public void onMerge(Trigger<IntervalWindow>.OnMergeContext c) { }

    @Override
    protected Trigger<IntervalWindow> getContinuationTrigger(
        List<Trigger<IntervalWindow>> continuationTriggers) {
      return null;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(IntervalWindow window) {
      return null;
    }

    @Override
    public boolean shouldFire(Trigger<IntervalWindow>.TriggerContext context) throws Exception {
      return false;
    }

    @Override
    public void onFire(Trigger<IntervalWindow>.TriggerContext context) throws Exception { }
  }

  private static class Trigger2 extends Trigger<IntervalWindow> {

    private Trigger2(List<Trigger<IntervalWindow>> subTriggers) {
      super(subTriggers);
    }

    @Override
    public void onElement(Trigger<IntervalWindow>.OnElementContext c) { }

    @Override
    public void onMerge(Trigger<IntervalWindow>.OnMergeContext c) { }

    @Override
    protected Trigger<IntervalWindow> getContinuationTrigger(
        List<Trigger<IntervalWindow>> continuationTriggers) {
      return null;
    }

    @Override
    public Instant getWatermarkThatGuaranteesFiring(IntervalWindow window) {
      return null;
    }

    @Override
    public boolean shouldFire(Trigger<IntervalWindow>.TriggerContext context) throws Exception {
      return false;
    }

    @Override
    public void onFire(Trigger<IntervalWindow>.TriggerContext context) throws Exception { }
  }
}
