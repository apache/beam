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

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Manages access to the restriction and keeps track of its claimed part for a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
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
   * Splits current restriction based on {@code fractionOfRemainder}.
   *
   * <p>If splitting the current restriction is possible, the current restriction is split into a
   * primary and residual restriction pair. This invocation updates the {@link
   * #currentRestriction()} to be the primary restriction effectively having the current {@link
   * DoFn.ProcessElement} execution responsible for performing the work that the primary restriction
   * represents. The residual restriction will be executed in a separate {@link DoFn.ProcessElement}
   * invocation (likely in a different process). The work performed by executing the primary and
   * residual restrictions as separate {@link DoFn.ProcessElement} invocations MUST be equivalent to
   * the work performed as if this split never occurred.
   *
   * <p>The {@code fractionOfRemainder} should be used in a best effort manner to choose a primary
   * and residual restriction based upon the fraction of the remaining work that the current {@link
   * DoFn.ProcessElement} invocation is responsible for. For example, if a {@link
   * DoFn.ProcessElement} was reading a file with a restriction representing the offset range {@code
   * [100, 200)} and has processed up to offset 130 with a {@code fractionOfRemainder} of {@code
   * 0.7}, the primary and residual restrictions returned would be {@code [100, 179), [179, 200)}
   * (note: {@code currentOffset + fractionOfRemainder * remainingWork = 130 + 0.7 * 70 = 179}).
   *
   * <p>{@code fractionOfRemainder = 0} means a checkpoint is required.
   *
   * <p>The API is recommended to be implemented for a batch pipeline to improve parallel processing
   * performance.
   *
   * <p>The API is required to be implemented for a streaming pipeline.
   *
   * @param fractionOfRemainder A hint as to the fraction of work the primary restriction should
   *     represent based upon the current known remaining amount of work.
   * @return a {@link SplitResult} if a split was possible, otherwise returns {@code null}.
   */
  public abstract SplitResult<RestrictionT> trySplit(double fractionOfRemainder);

  /**
   * Called by the runner after {@link DoFn.ProcessElement} returns.
   *
   * <p>Must throw an exception with an informative error message, if there is still any unclaimed
   * work remaining in the restriction.
   */
  public abstract void checkDone() throws IllegalStateException;
}
