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

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Manages access to the restriction and keeps track of its claimed part for a <a
 * href="https://s.apache.org/splittable-do-fn">splittable</a> {@link DoFn}.
 *
 * <p>{@link RestrictionTracker}s should implement {@link HasProgress} otherwise poor auto-scaling
 * of workers and/or splitting may result if the progress is an inaccurate representation of the
 * known amount of completed and remaining work.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public abstract class RestrictionTracker<RestrictionT, PositionT> {
  /**
   * Attempts to claim the block of work in the current restriction identified by the given
   * position. Each claimed position MUST be a valid split point.
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
   * @return a {@link SplitResult} if a split was possible, otherwise returns {@code null}. If the
   *     {@code fractionOfRemainder == 0}, a {@code null} result MUST imply that the restriction
   *     tracker is done and there is no more work left to do.
   */
  @Nullable
  public abstract SplitResult<RestrictionT> trySplit(double fractionOfRemainder);

  /**
   * Called by the runner after {@link DoFn.ProcessElement} returns.
   *
   * <p>Must throw an exception with an informative error message, if there is still any unclaimed
   * work remaining in the restriction.
   */
  public abstract void checkDone() throws IllegalStateException;

  public enum IsBounded {
    /** Indicates that a {@code Restriction} represents a bounded amount of work. */
    BOUNDED,
    /** Indicates that a {@code Restriction} represents an unbounded amount of work. */
    UNBOUNDED
  }

  /**
   * Return the boundedness of the current restriction. If the current restriction represents a
   * finite amount of work, it should return {@link IsBounded#BOUNDED}. Otherwise, it should return
   * {@link IsBounded#UNBOUNDED}.
   *
   * <p>It is valid to return {@link IsBounded#BOUNDED} after returning {@link IsBounded#UNBOUNDED} once
   * the end of a restriction is discovered. It is not valid to return {@link IsBounded#UNBOUNDED} after returning {@link IsBounded#BOUNDED}.
   */
  public abstract IsBounded isBounded();

  /**
   * All {@link RestrictionTracker}s SHOULD implement this interface to improve auto-scaling and
   * splitting performance.
   */
  public interface HasProgress {
    /**
     * A representation for the amount of known completed and known remaining work.
     *
     * <p>It is up to each restriction tracker to convert between their natural representation of
     * completed and remaining work and the {@code double} representation. For example:
     *
     * <ul>
     *   <li>Block based file source (e.g. Avro): The number of bytes from the beginning of the
     *       restriction to the current block and the number of bytes from the current block to the
     *       end of the restriction.
     *   <li>Pull based queue based source (e.g. Pubsub): The local/global size available in number
     *       of messages or number of {@code message bytes} that have processed and the number of
     *       messages or number of {@code message bytes} that are outstanding.
     *   <li>Key range based source (e.g. BigQuery, Bigtable, ...): Scale the start key to be one
     *       and end key to be zero and interpolate the position of the next splittable key as a
     *       position. If information about the probability density function or cumulative
     *       distribution function is available, work completed and work remaining interpolation can
     *       be improved. Alternatively, if the number of encoded bytes for the keys and values is
     *       known for the key range, the number of completed and remaining bytes can be used.
     * </ul>
     *
     * <p>The work completed and work remaining must be of the same scale whether that be number of
     * messages or number of bytes and should never represent two distinct unit types.
     */
    Progress getProgress();
  }

  /**
   * A representation for the amount of known completed and remaining work. See {@link
   * HasProgress#getProgress()} for details.
   */
  @AutoValue
  public abstract static class Progress {

    /**
     * A representation for the amount of known completed and remaining work. See {@link
     * HasProgress#getProgress()} for details.
     *
     * @param workCompleted Must be {@code >= 0}.
     * @param workRemaining Must be {@code >= 0}.
     */
    public static Progress from(double workCompleted, double workRemaining) {
      if (workCompleted < 0 || workRemaining < 0) {
        throw new IllegalArgumentException(
            String.format(
                "Work completed and work remaining must be greater than or equal to zero but were %s and %s.",
                workCompleted, workRemaining));
      }
      return new AutoValue_RestrictionTracker_Progress(workCompleted, workRemaining);
    }

    /** The known amount of completed work. */
    public abstract double getWorkCompleted();

    /** The known amount of work remaining. */
    public abstract double getWorkRemaining();
  }

  /** A representation of the truncate result. */
  @AutoValue
  public abstract static class TruncateResult<RestrictionT> {
    /** Returns a {@link TruncateResult} for the given restriction. */
    public static <RestrictionT> TruncateResult of(RestrictionT restriction) {
      return new AutoValue_RestrictionTracker_TruncateResult(restriction);
    }

    @Nullable
    public abstract RestrictionT getTruncatedRestriction();
  }
}
