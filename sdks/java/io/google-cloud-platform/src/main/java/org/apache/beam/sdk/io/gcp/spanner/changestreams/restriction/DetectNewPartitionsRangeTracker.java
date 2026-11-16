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

import com.google.cloud.Timestamp;
import java.util.Objects;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This restriction tracker delegates most of its behavior to an internal {@link
 * TimestampRangeTracker}. It has a different logic for {@link #tryClaim} and {@link #trySplit}: it
 * ignores claims for the same timestamp multiple times, and refuses non-checkpoint dynamic splits
 * so only a single active restriction polls and schedules new partitions.
 */
public class DetectNewPartitionsRangeTracker extends TimestampRangeTracker {

  public DetectNewPartitionsRangeTracker(TimestampRange range) {
    super(range);
  }

  /**
   * Attempts to claim the given position.
   *
   * <p>Must be equal or larger than the last successfully claimed position.
   *
   * @return {@code true} if the position was successfully claimed, {@code false} if it is outside
   *     the current {@link TimestampRange} of this tracker (in that case this operation is a
   *     no-op).
   */
  @Override
  public boolean tryClaim(Timestamp position) {
    if (Objects.equals(position, lastAttemptedPosition)) {
      return true;
    }

    return super.tryClaim(position);
  }

  /**
   * Refuses non-checkpoint splits ({@code fractionOfRemainder != 0}) because only one active
   * restriction should poll and schedule new partitions at a time. Checkpoints ({@code
   * fractionOfRemainder == 0}) are delegated to {@link TimestampRangeTracker#trySplit(double)}.
   */
  @Override
  public @Nullable SplitResult<TimestampRange> trySplit(double fractionOfRemainder) {
    if (fractionOfRemainder != 0) {
      return null;
    }

    return super.trySplit(fractionOfRemainder);
  }
}
