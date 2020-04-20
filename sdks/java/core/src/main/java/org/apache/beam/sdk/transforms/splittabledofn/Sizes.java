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
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;

/**
 * Definitions and convenience methods for reporting progress for SplittableDoFns.
 *
 * <p>{@link RestrictionTracker}s which work on restrictions which have a fixed known size should
 * implement {@link HasSize} otherwise {@link RestrictionTracker}s which handle restrictions that
 * can grow or shrink over time should implement {@link HasProgress}.
 */
@Experimental(Kind.SPLITTABLE_DO_FN)
public final class Sizes {

  /**
   * {@link RestrictionTracker}s which work on restrictions which have a fixed size should implement
   * this interface.
   *
   * <p>By default, the initial amount of work will be captured from {@link #getSize()} before
   * processing of the restriction begins. Afterwards progress will be reported as:
   *
   * <ul>
   *   <li>{@code work_remaining = getSize()}
   *   <li>{@code work_completed = max(initial_amount_of_work - work_remaining, 0)}
   * </ul>
   */
  public interface HasSize {
    /**
     * A representation for the amount of known work represented as a size. Size {@code double}
     * representations should preferably represent a linear space.
     *
     * <p>It is up to each restriction tracker to convert between their natural representation of
     * outstanding work and this representation. For example:
     *
     * <ul>
     *   <li>Block based file source (e.g. Avro): From the end of the current block, the remaining
     *       number of bytes to the end of the restriction.
     *   <li>Key range based source (e.g. BigQuery, Bigtable, ...): Scale the start key to be one
     *       and end key to be zero and interpolate the position of the next splittable key as the
     *       size. If information about the probability density function or cumulative distribution
     *       function is available, size interpolation can be improved. Alternatively, if the number
     *       of encoded bytes for the keys and values is known for the key range, the number of
     *       remaining bytes can be used.
     * </ul>
     */
    double getSize();
  }

  /**
   * {@link RestrictionTracker}s which handle restrictions that can grow or shrink over time should
   * implement this interface.
   */
  public interface HasProgress {
    /**
     * A representation for the amount of known completed and known remaining work.
     *
     * <p>It is up to each restriction tracker to convert between their natural representation of
     * completed and remaining work and the {@code double} representation. For example:
     *
     * <ul>
     *   <li>Pull based queue based source (e.g. Pubsub): The local/global size available in number
     *       of messages or number of {@code message bytes} that have processed and the number of
     *       messages or number of {@code message bytes} that are outstanding.
     *   <li>Claim set: The local/global number of objects that have been claimed and the number of
     *       known objects that remain to be claimed.
     * </ul>
     *
     * <p>The work completed and work remaining must be of the same scale whether that be number of
     * messages or number of bytes and should never represent two distinct unit types.
     */
    Progress getProgress();
  }

  /** A representation for the amount of known completed and remaining work. */
  @AutoValue
  public abstract static class Progress {
    public static Progress from(double workCompleted, double workRemaining) {
      return new AutoValue_Sizes_Progress(workCompleted, workRemaining);
    }

    /** The known amount of completed work. */
    public abstract double getWorkCompleted();

    /** The known amount of work remaining. */
    public abstract double getWorkRemaining();
  }
}
