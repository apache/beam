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
package com.google.cloud.dataflow.sdk.runners.worker;

import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler.StateKind;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for {@link UserCodeTimeTracker}.
 */
@RunWith(JUnit4.class)
public class UserCodeTimeTrackerTest {
  UserCodeTimeTracker getTracker(final int numProcessors) {
    return new UserCodeTimeTracker() {
      @Override
      protected int getNumProcessors() {
        return numProcessors;
      }
    };
  }
  @Test
  public void testUserFrameworkTimeDiscrimination() {
    CounterSet counters = new CounterSet();
    UserCodeTimeTracker tracker = getTracker(1);
    CounterSet.AddCounterMutator mutator = counters.getAddCounterMutator();
    tracker.workStarted("stage1-", 1, mutator);
    tracker.workObservedInState(1, StateKind.USER, 100);
    tracker.workObservedInState(1, StateKind.FRAMEWORK, 50);
    tracker.workFinished(1);

    assertEquals(100L, counters.getExistingCounter("stage1-user-code-msecs").getAggregate());
  }

  @Test
  public void testSaturated() {
    CounterSet counters = new CounterSet();
    UserCodeTimeTracker tracker = getTracker(2);
    CounterSet.AddCounterMutator mutator = counters.getAddCounterMutator();
    tracker.workStarted("stage1-", 1, mutator);
    tracker.workStarted("stage2-", 2, mutator);
    tracker.workObservedInState(1, StateKind.USER, 100);
    tracker.workObservedInState(2, StateKind.USER, 50);
    tracker.workFinished(1);
    tracker.workFinished(2);

    assertEquals(100L, counters.getExistingCounter("stage1-user-code-msecs").getAggregate());
    assertEquals(50L, counters.getExistingCounter("stage2-user-code-msecs").getAggregate());
  }

  @Test
  public void testOversubscribed() {
    CounterSet counters = new CounterSet();
    UserCodeTimeTracker tracker = getTracker(1);
    CounterSet.AddCounterMutator mutator = counters.getAddCounterMutator();
    tracker.workStarted("stage1-", 1, mutator);
    tracker.workStarted("stage2-", 2, mutator);
    // 1 user state work item.
    tracker.workObservedInState(1, StateKind.USER, 10);
    // 2 user state work items.
    tracker.workObservedInState(2, StateKind.USER, 20);
    // 2 user state work items.
    tracker.workObservedInState(1, StateKind.USER, 30);
    // 2 user state work items.
    tracker.workObservedInState(2, StateKind.USER, 40);
    // 1 user state work item.
    tracker.workObservedInState(1, StateKind.FRAMEWORK, 99);
    // 1 user state work items.
    tracker.workObservedInState(2, StateKind.USER, 50);
    // 2 user state work items.
    tracker.workFinished(1);
    // 1 user state work item.
    tracker.workObservedInState(2, StateKind.USER, 60);
    tracker.workFinished(2);
    // 0 user state work item.

    // 10 + 30 / 2 = 25
    assertEquals(25L, counters.getExistingCounter("stage1-user-code-msecs").getAggregate());
    // 20 / 2 + 40 / 2 + 50 + 60 = 140
    assertEquals(140L, counters.getExistingCounter("stage2-user-code-msecs").getAggregate());
  }

  @Test
  public void testScopedWork() throws Exception {
    CounterSet counters = new CounterSet();
    UserCodeTimeTracker tracker = getTracker(1);
    try (AutoCloseable scope1 = tracker.scopedWork("stage1-", 1, counters.getAddCounterMutator())) {
      tracker.workObservedInState(1, StateKind.USER, 10);
      try (AutoCloseable scope2 = tracker.scopedWork(
          "stage2-", 2, counters.getAddCounterMutator())) {
        tracker.workObservedInState(2, StateKind.USER, 30);
        tracker.workObservedInState(1, StateKind.USER, 100);
      }
    }

    // 10 + 100 / 2 = 60
    assertEquals(60L, counters.getExistingCounter("stage1-user-code-msecs").getAggregate());
    // 30 / 2 = 15
    assertEquals(15L, counters.getExistingCounter("stage2-user-code-msecs").getAggregate());
  }
}

