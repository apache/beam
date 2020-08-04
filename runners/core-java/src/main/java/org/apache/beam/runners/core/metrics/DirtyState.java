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
package org.apache.beam.runners.core.metrics;

import java.io.Serializable;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Atomically tracks the dirty-state of a metric.
 *
 * <p>Reporting an update is split into two parts such that only changes made before the call to
 * {@link #beforeCommit()} are committed when {@link #afterCommit()} is invoked. This allows for a
 * two-step commit process of gathering all the dirty updates (calling {#link beforeCommit()})
 * followed by committing and calling {#link afterCommit()}.
 *
 * <p>The tracking of dirty states is done conservatively -- sometimes {@link #beforeCommit()} will
 * return true (indicating a dirty metric) even if there have been no changes since the last commit.
 *
 * <p>There is also a possible race when the underlying metric is modified but the call to {@link
 * #afterModification()} hasn't happened before the call to {@link #beforeCommit()}. In this case
 * the next round of metric updating will see the changes. If this was for the final commit, then
 * the metric updates shouldn't be extracted until all possible user modifications have completed.
 */
public class DirtyState implements Serializable {
  private enum State {
    /** Indicates that there have been changes to the MetricCell since last commit. */
    DIRTY,
    /** Indicates that there have been no changes to the MetricCell since last commit. */
    CLEAN,
    /** Indicates that a commit of the current value is in progress. */
    COMMITTING
  }

  private final AtomicReference<State> dirty = new AtomicReference<>(State.DIRTY);

  /**
   * Indicate that changes have been made to the metric being tracked by this {@link DirtyState}.
   *
   * <p>Should be called <b>after</b> modification of the value.
   */
  public void afterModification() {
    dirty.set(State.DIRTY);
  }

  /**
   * Check the dirty state and mark the metric as committing.
   *
   * <p>If the state was {@code CLEAN}, this returns {@code false}. If the state was {@code DIRTY}
   * or {@code COMMITTING} this returns {@code true} and sets the state to {@code COMMITTING}.
   *
   * @return {@code false} if the state is clean and {@code true} otherwise.
   */
  public boolean beforeCommit() {
    // After this loop, we want the state to be either CLEAN or COMMITTING.
    // If the state was CLEAN, we don't need to do anything (and exit the loop early)
    // If the state was DIRTY, we will attempt to do a CAS(DIRTY, COMMITTING). This will only
    // fail if another thread is getting updates which generally shouldn't be the case.
    // If the state was COMMITTING, we will attempt to do a CAS(COMMITTING, COMMITTING). This will
    // fail if another thread commits updates (which shouldn't be the case) or if the user code
    // updates the metric, in which case it will transition to DIRTY and the next iteration will
    // successfully update it.
    State state;
    do {
      state = dirty.get();
    } while (state != State.CLEAN && !dirty.compareAndSet(state, State.COMMITTING));

    return state != State.CLEAN;
  }

  /**
   * Mark any changes up to the most recently call to {@link #beforeCommit()}} as committed. The
   * next call to {@link #beforeCommit()} will return {@code false} unless there have been changes
   * made since the previous call to {@link #beforeCommit()}.
   */
  public void afterCommit() {
    dirty.compareAndSet(State.COMMITTING, State.CLEAN);
  }

  @Override
  public boolean equals(@Nullable Object object) {
    if (object instanceof DirtyState) {
      DirtyState dirtyState = (DirtyState) object;
      return Objects.equals(dirty.get(), dirtyState.dirty.get());
    }

    return false;
  }

  @Override
  public int hashCode() {
    return dirty.get().hashCode();
  }
}
