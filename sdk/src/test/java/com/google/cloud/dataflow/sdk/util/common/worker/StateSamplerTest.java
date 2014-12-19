/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

/**
 * Unit tests for the {@link Counter} API.
 */
@RunWith(JUnit4.class)
public class StateSamplerTest {

  @Test
  public void basicTest() throws InterruptedException {
    CounterSet counters = new CounterSet();
    long periodMs = 50;
    StateSampler stateSampler = new StateSampler("test-",
        counters.getAddCounterMutator(), periodMs);

    int state1 = stateSampler.stateForName("1");
    int state2 = stateSampler.stateForName("2");

    assertEquals(new SimpleEntry<>("", 0L),
        stateSampler.getCurrentStateAndDuration());

    try (StateSampler.ScopedState s1 =
      stateSampler.scopedState(state1)) {
      assert s1 != null;
      Thread.sleep(2 * periodMs);
    }

    try (StateSampler.ScopedState s2 =
      stateSampler.scopedState(state2)) {
      assert s2 != null;
      Thread.sleep(3 * periodMs);
    }

    long s1 = stateSampler.getStateDuration(state1);
    long s2 = stateSampler.getStateDuration(state2);

    System.out.println("basic s1: " + s1);
    System.out.println("basic s2: " + s2);

    long toleranceMs = periodMs;
    assertTrue(s1 + s2 >= 4 * periodMs - toleranceMs);
    assertTrue(s1 + s2 <= 10 * periodMs + toleranceMs);
  }

  @Test
  public void nestingTest() throws InterruptedException {
    CounterSet counters = new CounterSet();
    long periodMs = 50;
    StateSampler stateSampler = new StateSampler("test-",
        counters.getAddCounterMutator(), periodMs);

    int state1 = stateSampler.stateForName("1");
    int state2 = stateSampler.stateForName("2");
    int state3 = stateSampler.stateForName("3");

    assertEquals(new SimpleEntry<>("", 0L),
        stateSampler.getCurrentStateAndDuration());

    try (StateSampler.ScopedState s1 =
        stateSampler.scopedState(state1)) {
      assert s1 != null;
      Thread.sleep(2 * periodMs);

      try (StateSampler.ScopedState s2 =
          stateSampler.scopedState(state2)) {
        assert s2 != null;
        Thread.sleep(2 * periodMs);

        try (StateSampler.ScopedState s3 =
            stateSampler.scopedState(state3)) {
          assert s3 != null;
          Thread.sleep(2 * periodMs);
        }
        Thread.sleep(periodMs);
      }
      Thread.sleep(periodMs);
    }

    long s1 = stateSampler.getStateDuration(state1);
    long s2 = stateSampler.getStateDuration(state2);
    long s3 = stateSampler.getStateDuration(state3);

    System.out.println("s1: " + s1);
    System.out.println("s2: " + s2);
    System.out.println("s3: " + s3);

    long toleranceMs = periodMs;
    assertTrue(s1 + s2 + s3 >= 4 * periodMs - toleranceMs);
    assertTrue(s1 + s2 + s3 <= 16 * periodMs + toleranceMs);
  }

  @Test
  public void nonScopedTest() throws InterruptedException {
    CounterSet counters = new CounterSet();
    long periodMs = 50;
    StateSampler stateSampler = new StateSampler("test-",
        counters.getAddCounterMutator(), periodMs);

    int state1 = stateSampler.stateForName("1");
    int previousState = stateSampler.setState(state1);
    Thread.sleep(2 * periodMs);
    Map.Entry<String, Long> currentStateAndDuration =
        stateSampler.getCurrentStateAndDuration();
    stateSampler.setState(previousState);
    assertEquals("test-1-msecs", currentStateAndDuration.getKey());
    long tolerance = periodMs;
    long s = currentStateAndDuration.getValue();
    System.out.println("s: " + s);
    assertTrue(s >= periodMs - tolerance);
    assertTrue(s <= 4 * periodMs + tolerance);

    assertTrue(stateSampler.getCurrentStateAndDuration()
        .getKey().isEmpty());
  }
}
