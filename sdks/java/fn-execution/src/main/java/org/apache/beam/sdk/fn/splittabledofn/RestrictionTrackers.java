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
  private static class ForwardingRestrictionTracker<RestrictionT, PositionT>
      extends RestrictionTracker<RestrictionT, PositionT> {
    private final RestrictionTracker<RestrictionT, PositionT> delegate;

    protected ForwardingRestrictionTracker(RestrictionTracker<RestrictionT, PositionT> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean tryClaim(PositionT position) {
      return delegate.tryClaim(position);
    }

    @Override
    public RestrictionT currentRestriction() {
      return delegate.currentRestriction();
    }

    @Override
    public RestrictionT checkpoint() {
      return delegate.checkpoint();
    }

    @Override
    public void checkDone() throws IllegalStateException {
      delegate.checkDone();
    }
  }

  /**
   * A {@link RestrictionTracker} which notifies the {@link ClaimObserver} if a claim succeeded or
   * failed.
   */
  private static class RestrictionTrackerObserver<RestrictionT, PositionT>
      extends ForwardingRestrictionTracker<RestrictionT, PositionT> {
    private final ClaimObserver<PositionT> claimObserver;

    private RestrictionTrackerObserver(
        RestrictionTracker<RestrictionT, PositionT> delegate,
        ClaimObserver<PositionT> claimObserver) {
      super(delegate);
      this.claimObserver = claimObserver;
    }

    @Override
    public boolean tryClaim(PositionT position) {
      if (super.tryClaim(position)) {
        claimObserver.onClaimed(position);
        return true;
      } else {
        claimObserver.onClaimFailed(position);
        return false;
      }
    }
  }

  /**
   * Returns a {@link RestrictionTracker} which reports all claim attempts to the specified {@link
   * ClaimObserver}.
   */
  public static <RestrictionT, PositionT> RestrictionTracker<RestrictionT, PositionT> observe(
      RestrictionTracker<RestrictionT, PositionT> restrictionTracker,
      ClaimObserver<PositionT> claimObserver) {
    return new RestrictionTrackerObserver<RestrictionT, PositionT>(
        restrictionTracker, claimObserver);
  }
}
