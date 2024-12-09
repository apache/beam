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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import java.util.function.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;

/** An interrupter for restriction tracker of type T. */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class RestrictionInterrupter<T> {
  private T lastAttemptedPosition;

  private Supplier<Instant> timeSupplier;
  private Instant softDeadline;
  private boolean hasInterrupted = true;

  /**
   * Sets a soft timeout from now for processing new positions. After the timeout the tryInterrupt
   * will start returning true indicating an early exit from processing.
   */
  public static <T> RestrictionInterrupter<T> withSoftTimeout(Duration timeout) {
    return new RestrictionInterrupter<T>(() -> Instant.now(), timeout);
  }

  RestrictionInterrupter(Supplier<Instant> timeSupplier, Duration timeout) {
    this.timeSupplier = timeSupplier;
    this.softDeadline = this.timeSupplier.get().plus(timeout);
    hasInterrupted = false;
  }

  @VisibleForTesting
  void setTimeSupplier(Supplier<Instant> timeSupplier) {
    this.timeSupplier = timeSupplier;
  }

  /**
   * Returns true if the restriction tracker should be interrupted in claiming new positions.
   *
   * <ol>
   *   <li>If soft deadline hasn't been reached always returns false.
   *   <li>If soft deadline has been reached but we haven't processed any positions returns false.
   *   <li>If soft deadline has been reached but the new position is the same as the last attempted
   *       position returns false.
   *   <li>If soft deadline has been reached and the new position differs from the last attempted
   *       position returns true.
   * </ol>
   *
   * @return {@code true} if the position processing should continue, {@code false} if the soft
   *     deadline has been reached and we have fully processed the previous position.
   */
  public boolean tryInterrupt(T position) {
    if (hasInterrupted) {
      return true;
    }
    if (lastAttemptedPosition == null) {
      lastAttemptedPosition = position;
      return false;
    }

    hasInterrupted |=
        timeSupplier.get().isAfter(softDeadline) && !position.equals(lastAttemptedPosition);
    lastAttemptedPosition = position;
    return hasInterrupted;
  }
}
