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

import static com.google.cloud.dataflow.sdk.testing.SystemNanoTimeSleeper.sleepMillis;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler.SamplingCallback;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler.ScopedState;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler.StateKind;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

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

      int state1 = stateSampler.stateForName("1", StateSampler.StateKind.USER);
      int state2 = stateSampler.stateForName("2", StateSampler.StateKind.USER);

      try (StateSampler.ScopedState s1 =
          stateSampler.scopedState(state1)) {
        assert s1 != null;
        sleepMillis(2 * periodMs);
      }

      try (StateSampler.ScopedState s2 =
          stateSampler.scopedState(state2)) {
        assert s2 != null;
        sleepMillis(3 * periodMs);
      }

      long s1 = getCounterLongValue(counters, "test-1-msecs");
      long s2 = getCounterLongValue(counters, "test-2-msecs");

      long toleranceMs = periodMs;
      assertTrue(s1 + s2 >= 1 * periodMs - toleranceMs);
      assertTrue(s1 + s2 <= 10 * periodMs + toleranceMs);
    }
  }

  @Test
  public void nestingTest() throws InterruptedException {
    CounterSet counters = new CounterSet();
    long periodMs = 50;
    try (StateSampler stateSampler =
        new StateSampler("test-", counters.getAddCounterMutator(), periodMs)) {

      int state1 = stateSampler.stateForName("1", StateSampler.StateKind.USER);
      int state2 = stateSampler.stateForName("2", StateSampler.StateKind.USER);
      int state3 = stateSampler.stateForName("3", StateSampler.StateKind.USER);

      try (StateSampler.ScopedState s1 =
          stateSampler.scopedState(state1)) {
        assert s1 != null;
        sleepMillis(2 * periodMs);

        try (StateSampler.ScopedState s2 =
            stateSampler.scopedState(state2)) {
          assert s2 != null;
          sleepMillis(2 * periodMs);

          try (StateSampler.ScopedState s3 =
              stateSampler.scopedState(state3)) {
            assert s3 != null;
            sleepMillis(2 * periodMs);
          }
          sleepMillis(periodMs);
        }
        sleepMillis(periodMs);
      }

      long s1 = getCounterLongValue(counters, "test-1-msecs");
      long s2 = getCounterLongValue(counters, "test-2-msecs");
      long s3 = getCounterLongValue(counters, "test-3-msecs");

      long toleranceMs = periodMs;
      assertTrue(s1 + s2 + s3 >= 4 * periodMs - toleranceMs);
      assertTrue(s1 + s2 + s3 <= 18 * periodMs + toleranceMs);
    }
  }

  @Test
  public void nonScopedTest() throws InterruptedException {
    CounterSet counters = new CounterSet();
    long periodMs = 50;
    try (StateSampler stateSampler = new StateSampler("test-",
        counters.getAddCounterMutator(), periodMs)) {

      int state1 = stateSampler.stateForName("1", StateSampler.StateKind.USER);
      int previousState = stateSampler.setState(state1);
      sleepMillis(2 * periodMs);
      stateSampler.setState(previousState);
      long tolerance = periodMs;
      long s = getCounterLongValue(counters, "test-1-msecs");

      assertTrue(s >= periodMs - tolerance);
      assertTrue(s <= 5 * periodMs + tolerance);
    }
  }

  private void noSamplingAfterCloseTestOnce() throws Exception {
    CounterSet counters = new CounterSet();
    long periodMs = 200;

    final AtomicLong lastSampledTimeStamp = new AtomicLong();
    final Semaphore sampleHappened = new Semaphore(0);
    try (StateSampler stateSampler = new StateSampler("test-",
        counters.getAddCounterMutator(), periodMs)) {
      stateSampler.setState("test", StateKind.USER);
      stateSampler.addSamplingCallback(new SamplingCallback(){
        @Override
        public void run(int state, StateKind kind, long elapsedMs) {
          lastSampledTimeStamp.set(System.nanoTime());
          sampleHappened.release();
        }
      });
      sampleHappened.acquire();
    }
    long samplerStoppedTimeStamp = System.nanoTime();
    assertThat(lastSampledTimeStamp.get(), Matchers.lessThanOrEqualTo(samplerStoppedTimeStamp));
  }

  @Test
  public void noSamplingAfterCloseTest() throws Exception {
    // Run it multiple times to detect flakyness.
    for (int i = 0; i < 10; ++i) {
      noSamplingAfterCloseTestOnce();
    }
  }

  @Test
  public void reuseStateByNameTest() throws Exception {
    StateSampler stateSampler = new StateSampler("test-",
        new CounterSet().getAddCounterMutator(), 200);
    int state1 = stateSampler.stateForName("test_state", StateKind.USER);
    int state2 = stateSampler.stateForName("test_state", StateKind.USER);
    assertEquals(state1, state2);
    stateSampler.close();
  }

  @Test
  public void reuseManyStatesByName() throws Exception {
    // Issue big number of stateForName() and setState calls to StateSampler.
    CounterSet counters = new CounterSet();
    StateSampler stateSampler = new StateSampler("test-",
        counters.getAddCounterMutator(), 200);
    for (int i = 0; i < 10000; i++) {
      for (int j = 0; j < 10000; j++) {
        int state = stateSampler.stateForName("state" + j, StateKind.USER);
        try (ScopedState scope = stateSampler.scopedState(state)) {}
      }
    }
    // All counters got reused.
    assertEquals(10000, counters.size());
    stateSampler.close();
  }
}
