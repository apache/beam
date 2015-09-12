/*******************************************************************************
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
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for the {@link Counter} API.
 */
@RunWith(JUnit4.class)
public class StateSamplerTest {
  public static long getCounterLongValue(CounterSet counters, String name) {
    @SuppressWarnings("unchecked")
    Counter<Long> counter = (Counter<Long>) counters.getExistingCounter(name);
    return counter.getAggregate();
  }

  @Test
  public void basicTest() throws InterruptedException {
    CounterSet counters = new CounterSet();
    long periodMs = 50;
    try (StateSampler stateSampler =
        new StateSampler("test-", counters.getAddCounterMutator(), periodMs)) {

      int state1 = stateSampler.stateForName("1");
      int state2 = stateSampler.stateForName("2");

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

      long s1 = getCounterLongValue(counters, "test-1-msecs");
      long s2 = getCounterLongValue(counters, "test-2-msecs");

      long toleranceMs = periodMs;
      assertTrue(s1 + s2 >= 4 * periodMs - toleranceMs);
      assertTrue(s1 + s2 <= 10 * periodMs + toleranceMs);
    }
  }

  @Test
  public void nestingTest() throws InterruptedException {
    CounterSet counters = new CounterSet();
    long periodMs = 50;
    try (StateSampler stateSampler =
        new StateSampler("test-", counters.getAddCounterMutator(), periodMs)) {

      int state1 = stateSampler.stateForName("1");
      int state2 = stateSampler.stateForName("2");
      int state3 = stateSampler.stateForName("3");

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

      long s1 = getCounterLongValue(counters, "test-1-msecs");
      long s2 = getCounterLongValue(counters, "test-2-msecs");
      long s3 = getCounterLongValue(counters, "test-3-msecs");

      long toleranceMs = periodMs;
      assertTrue(s1 + s2 + s3 >= 4 * periodMs - toleranceMs);
      assertTrue(s1 + s2 + s3 <= 16 * periodMs + toleranceMs);
    }
  }

  @Test
  public void nonScopedTest() throws InterruptedException {
    CounterSet counters = new CounterSet();
    long periodMs = 50;
    try (StateSampler stateSampler = new StateSampler("test-",
        counters.getAddCounterMutator(), periodMs)) {

      int state1 = stateSampler.stateForName("1");
      int previousState = stateSampler.setState(state1);
      Thread.sleep(2 * periodMs);
      stateSampler.setState(previousState);
      long tolerance = periodMs;
      long s = getCounterLongValue(counters, "test-1-msecs");

      assertTrue(s >= periodMs - tolerance);
      assertTrue(s <= 4 * periodMs + tolerance);
    }
  }
}
