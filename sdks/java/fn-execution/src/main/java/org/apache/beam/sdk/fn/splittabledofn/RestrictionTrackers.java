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
package org.apache.beam.sdk.fn.splittabledofn;

import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.transforms.splittabledofn.Backlog;
import org.apache.beam.sdk.transforms.splittabledofn.Backlogs;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

/** Support utilities for interacting with {@link RestrictionTracker RestrictionTrackers}. */
public class RestrictionTrackers {

  /** Interface allowing a runner to observe the calls to {@link RestrictionTracker#tryClaim}. */
  public interface ClaimObserver<PositionT> {
    /** Called when {@link RestrictionTracker#tryClaim} returns true. */
    void onClaimed(PositionT position);

    /** Called when {@link RestrictionTracker#tryClaim} returns false. */
    void onClaimFailed(PositionT position);
  }

  /**
   * A {@link RestrictionTracker} which forwards all calls to the delegate {@link
   * RestrictionTracker}.
   */
  @ThreadSafe
  private static class RestrictionTrackerObserver<RestrictionT, PositionT>
      extends RestrictionTracker<RestrictionT, PositionT> {
    protected final RestrictionTracker<RestrictionT, PositionT> delegate;
    private final ClaimObserver<PositionT> claimObserver;

    protected RestrictionTrackerObserver(
        RestrictionTracker<RestrictionT, PositionT> delegate,
        ClaimObserver<PositionT> claimObserver) {
      this.delegate = delegate;
      this.claimObserver = claimObserver;
    }

    @Override
    public synchronized boolean tryClaim(PositionT position) {
      if (delegate.tryClaim(position)) {
        claimObserver.onClaimed(position);
        return true;
      } else {
        claimObserver.onClaimFailed(position);
        return false;
      }
    }

    @Override
    public synchronized RestrictionT currentRestriction() {
      return delegate.currentRestriction();
    }

    @Override
    public synchronized RestrictionT checkpoint() {
      return delegate.checkpoint();
    }

    @Override
    public synchronized void checkDone() throws IllegalStateException {
      delegate.checkDone();
    }
  }

  /**
   * A {@link RestrictionTracker} which forwards all calls to the delegate backlog reporting {@link
   * RestrictionTracker}.
   */
  @ThreadSafe
  private static class RestrictionTrackerObserverWithBacklog<RestrictionT, PositionT>
      extends RestrictionTrackerObserver<RestrictionT, PositionT> implements Backlogs.HasBacklog {

    protected RestrictionTrackerObserverWithBacklog(
        RestrictionTracker<RestrictionT, PositionT> delegate,
        ClaimObserver<PositionT> claimObserver) {
      super(delegate, claimObserver);
    }

    @Override
    public synchronized Backlog getBacklog() {
      return ((Backlogs.HasBacklog) delegate).getBacklog();
    }
  }

  /**
   * A {@link RestrictionTracker} which forwards all calls to the delegate partitioned backlog
   * reporting {@link RestrictionTracker}.
   */
  @ThreadSafe
  private static class RestrictionTrackerObserverWithPartitionedBacklog<RestrictionT, PositionT>
      extends RestrictionTrackerObserverWithBacklog<RestrictionT, PositionT>
      implements Backlogs.HasPartitionedBacklog {

    protected RestrictionTrackerObserverWithPartitionedBacklog(
        RestrictionTracker<RestrictionT, PositionT> delegate,
        ClaimObserver<PositionT> claimObserver) {
      super(delegate, claimObserver);
    }

    @Override
    public synchronized byte[] getBacklogPartition() {
      return ((Backlogs.HasPartitionedBacklog) delegate).getBacklogPartition();
    }
  }

  /**
   * Returns a thread safe {@link RestrictionTracker} which reports all claim attempts to the
   * specified {@link ClaimObserver}.
   */
  public static <RestrictionT, PositionT> RestrictionTracker<RestrictionT, PositionT> observe(
      RestrictionTracker<RestrictionT, PositionT> restrictionTracker,
      ClaimObserver<PositionT> claimObserver) {
    if (restrictionTracker instanceof Backlogs.HasPartitionedBacklog) {
      return new RestrictionTrackerObserverWithPartitionedBacklog<>(
          restrictionTracker, claimObserver);
    } else if (restrictionTracker instanceof Backlogs.HasBacklog) {
      return new RestrictionTrackerObserverWithBacklog<>(restrictionTracker, claimObserver);
    } else {
      return new RestrictionTrackerObserver<>(restrictionTracker, claimObserver);
    }
  }
}
