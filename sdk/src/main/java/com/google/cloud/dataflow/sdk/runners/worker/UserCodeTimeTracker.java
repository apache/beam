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

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.CounterSet.AddCounterMutator;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler;
import com.google.cloud.dataflow.sdk.util.common.worker.StateSampler.StateKind;

import org.eclipse.jetty.util.ConcurrentHashSet;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class tracks the user code time spent on every work item executed by a Dataflow worker.
 * User code time is the time spent on doing Dataflow SDK operations other than shuffling (like
 * GroupByKey or CoGroupByKey) and reading / writing states from / to windmill.
 *
 * <p>We assume that user code in work items is CPU-bound. To account for proportional slowdown in
 * case there are more work items concurrently executing user code than there are CPUs, we introduce
 * "effective user code time" that is invariant to the level of multithreading. More precisely,
 * a unit of time spent in user code counts as min(1, numCPUs / numActiveItemsInUserCode) units of
 * "effective time in user code".
 */
public class UserCodeTimeTracker {
  private static class WorkItemInfo {
    final Counter<Long> counter;

    public WorkItemInfo(String counterPrefix, AddCounterMutator mutator) {
      counter = mutator.addCounter(
          Counter.longs(counterPrefix + "user-code-msecs", Counter.AggregationKind.SUM));
    }
  }

  /**
   * Mapping from item id to WorkItemInfo.
   */
  private final Map<Long, WorkItemInfo> itemMap = new ConcurrentHashMap<Long, WorkItemInfo>();

  /**
   * Set of the item ids in states with the kind StateSampler.StateKind.USER.
   */
  private final Set<Long> itemsInUserState = new ConcurrentHashSet<Long>();

  /**
   * Records the start of the work.
   * @param counterPrefix counter prefix associated with this work item.
   * @param itemId id of the work.
   * @param mutator counter mutator associated with this work item.
   */
  public void workStarted(String counterPrefix, long itemId, AddCounterMutator mutator) {
    if (itemMap.put(itemId, new WorkItemInfo(counterPrefix, mutator)) != null) {
      throw new IllegalArgumentException("Item " + itemId + " already started.");
    }
  }

  /**
   * Records the finish of the work.
   * @param itemId id of the work.
   */
  public void workFinished(long itemId) {
    if (itemMap.remove(itemId) == null) {
      throw new IllegalArgumentException("Item " + itemId + " never started.");
    }
    itemsInUserState.remove(itemId);
  }

  /**
   * Records the observation that the work with {@code itemId} has been in a state with {@code kind}
   * for {@code elapsedMs} milliseconds.
   * It is supposed to be called in a callback of a StateSampler.
   * @param itemId the id of the work
   * @param kind kind of the associated state
   * @param elapsedMs time duration in milliseconds since the previous observation of this work
   *        item's state
   */
  public void workObservedInState(
      long itemId, StateKind kind, long elapsedMs) {
    if (kind == StateSampler.StateKind.USER) {
      itemsInUserState.add(itemId);
    } else {
      itemsInUserState.remove(itemId);
      return;
    }

    WorkItemInfo info = itemMap.get(itemId);
    if (info == null) {
      throw new NoSuchElementException("Item " + itemId + " doesn't exist.");
    }
    int numProcessors = getNumProcessors();
    int numActives = itemsInUserState.size();
    long userCodeMsecs = (long) (elapsedMs * Math.min(1.0, 1.0 * numProcessors / numActives));

    info.counter.addValue(userCodeMsecs);
  }

  /**
   * Returns an AutoCloseable that will call {@link #workStarted} at first and will automatically
   * call {@link #workFinished} upon closing.
   * @param counterPrefix counter prefix associated with this work item.
   * @param itemId id of the work.
   * @param mutator counter mutator associated with this work item.
   * @return an AutoCloseable that automatically call {@link #workFinished} upon closing.
   */
  public AutoCloseable scopedWork(
      String counterPrefix, final long itemId, final CounterSet.AddCounterMutator mutator) {
    workStarted(counterPrefix, itemId, mutator);
    return new AutoCloseable() {
      @Override
      public void close() throws Exception {
        workFinished(itemId);
      }
    };
  }

  protected int getNumProcessors() {
    return Runtime.getRuntime().availableProcessors();
  }

  /**
   * A simple callback to be used to invoke {@code UserCodeTimeTracker.workObservedInState} from
   * the StateSampler.
   */
  public static class StateSamplerCallback implements StateSampler.SamplingCallback {
    private final UserCodeTimeTracker tracker;

    private final long itemId;

    StateSamplerCallback(UserCodeTimeTracker tracker, long itemId) {
      this.tracker = tracker;
      this.itemId = itemId;
    }

    @Override
    public void run(int state, StateKind kind, long elapsedMs) {
      tracker.workObservedInState(itemId, kind, elapsedMs);
    }
  }
}
