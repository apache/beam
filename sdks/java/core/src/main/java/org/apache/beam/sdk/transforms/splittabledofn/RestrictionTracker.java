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

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Manages access to the restriction and keeps track of its claimed part for a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
 *
 * <p>Restriction trackers which can provide an estimate for the known amount of outstanding work
 * should implement {@link Backlogs.HasBacklog} to provide information that can be used during
 * progress reporting and splitting by runners to improve the performance of the pipeline and
 * increase resource utilization. See <a
 * href="https://s.apache.org/beam-bundles-backlog-splitting">Bundles w/ SplittableDoFns: Backlog
 * &amp; Splitting</a> for further details.
 */
public abstract class RestrictionTracker<RestrictionT, PositionT> {
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
   */
  public abstract boolean tryClaim(PositionT position);

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
}
