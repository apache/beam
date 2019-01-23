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
package org.apache.beam.sdk.transforms.splittabledofn;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Manages concurrent access to the restriction and keeps track of its claimed part for a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
 */
public abstract class RestrictionTracker<RestrictionT, PositionT> {
  /** Internal interface allowing a runner to observe the calls to {@link #tryClaim}. */
  @Internal
  public interface ClaimObserver<PositionT> {
    /** Called when {@link #tryClaim} returns true. */
    void onClaimed(PositionT position);

    /** Called when {@link #tryClaim} returns false. */
    void onClaimFailed(PositionT position);
  }

  @Nullable private ClaimObserver<PositionT> claimObserver;

  /**
   * Sets a {@link ClaimObserver} to be invoked on every call to {@link #tryClaim}. Internal:
   * intended only for runner authors.
   */
  @Internal
  public void setClaimObserver(ClaimObserver<PositionT> claimObserver) {
    checkNotNull(claimObserver, "claimObserver");
    checkState(this.claimObserver == null, "A claim observer has already been set");
    this.claimObserver = claimObserver;
  }

  /**
   * Attempts to claim the block of work in the current restriction identified by the given
   * position.
   *
   * <p>If this succeeds, the DoFn MUST execute the entire block of work. If this fails:
   *
   * <ul>
   *   <li>{@link DoFn.ProcessElement} MUST return {@link DoFn.ProcessContinuation#stop} without
   *       performing any additional work or emitting output (note that emitting output or
   *       performing work from {@link DoFn.ProcessElement} is also not allowed before the first
   *       call to this method).
   *   <li>{@link RestrictionTracker#checkDone} MUST succeed.
   * </ul>
   *
   * <p>Under the hood, calls {@link #tryClaimImpl} and notifies {@link ClaimObserver} of the
   * result.
   */
  public final boolean tryClaim(PositionT position) {
    if (tryClaimImpl(position)) {
      if (claimObserver != null) {
        claimObserver.onClaimed(position);
      }
      return true;
    } else {
      if (claimObserver != null) {
        claimObserver.onClaimFailed(position);
      }
      return false;
    }
  }

  /** Tracker-specific implementation of {@link #tryClaim}. */
  @Internal
  protected abstract boolean tryClaimImpl(PositionT position);

  /**
   * Returns a restriction accurately describing the full range of work the current {@link
   * DoFn.ProcessElement} call will do, including already completed work.
   */
  public abstract RestrictionT currentRestriction();

  /**
   * Signals that the current {@link DoFn.ProcessElement} call should terminate as soon as possible:
   * after this method returns, the tracker MUST refuse all future claim calls, and {@link
   * #checkDone} MUST succeed.
   *
   * <p>Modifies {@link #currentRestriction}. Returns a restriction representing the rest of the
   * work: the old value of {@link #currentRestriction} is equivalent to the new value and the
   * return value of this method combined.
   *
   * <p>Must be called at most once on a given object. Must not be called before the first
   * successful {@link #tryClaim} call.
   */
  public abstract RestrictionT checkpoint();

  /**
   * Called by the runner after {@link DoFn.ProcessElement} returns.
   *
   * <p>Must throw an exception with an informative error message, if there is still any unclaimed
   * work remaining in the restriction.
   */
  public abstract void checkDone() throws IllegalStateException;

  // TODO: Add the more general splitRemainderAfterFraction() and other methods.
}
