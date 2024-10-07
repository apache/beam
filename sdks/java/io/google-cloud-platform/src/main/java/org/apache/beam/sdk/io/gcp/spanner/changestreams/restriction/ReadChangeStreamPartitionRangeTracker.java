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
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This restriction tracker delegates most of its behavior to an internal {@link
 * TimestampRangeTracker}. It has a different logic for tryClaim and trySplit methods. It ignores
 * claims for the same timestamp multiple times. This is because several change stream records might
 * have the same timestamp, thus leading to multiple claims of the same {@link Timestamp}. Other
 * than that, it always denies splits for the {@link InitialPartition#PARTITION_TOKEN}, since we
 * only need to perform this query once.
 */
@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class ReadChangeStreamPartitionRangeTracker extends TimestampRangeTracker
    implements Interruptible {

  private final PartitionMetadata partition;
  private Instant softDeadline;
  private boolean continueProcessing = true;

  /**
   * Receives the partition that will be queried and the timestamp range that belongs to it.
   *
   * @param partition the partition that will use the tracker
   * @param range closed / open range interval for the start / end times of the given partition
   */
  public ReadChangeStreamPartitionRangeTracker(PartitionMetadata partition, TimestampRange range) {
    super(range);
    this.partition = partition;
  }

  /**
   * Sets a soft timeout from now for processing new positions. After the timeout the shouldContinue
   * will start returning false indicating an early exit from processing.
   */
  @Override
  public void setSoftTimeout(Duration duration) {
    softDeadline = new Instant(timeSupplier.get().toSqlTimestamp()).plus(duration);
    continueProcessing = true;
  }

  /**
   * Returns true if the restriction tracker can claim new positions.
   *
   * <p>If soft timeout isn't set always returns true. Otherwise:
   *
   * <ol>
   *   <li>If soft deadline hasn't been reached always returns true.
   *   <li>If soft deadline has been reached but we haven't processed any positions returns true.
   *   <li>If soft deadline has been reached but the new position is the same as the last attempted
   *       position returns true.
   *   <li>If soft deadline has been reached and the new position differs from the last attempted
   *       position returns false.
   * </ol>
   *
   * @return {@code true} if the position processing should continue, {@code false} if the soft
   *     deadline has been reached and we have fully processed the previous position.
   */
  @Override
  public boolean shouldContinue(Timestamp position) {
    if (!continueProcessing) {
      return false;
    }
    if (softDeadline == null || lastAttemptedPosition == null) {
      return true;
    }

    continueProcessing &=
        new Instant(timeSupplier.get().toSqlTimestamp()).isBefore(softDeadline)
            || position.equals(lastAttemptedPosition);
    return continueProcessing;
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
    if (position.equals(lastAttemptedPosition)) {
      return true;
    }

    return super.tryClaim(position, this.partition);
  }

  /**
   * If the partition token is the {@link InitialPartition#PARTITION_TOKEN}, it does not allow for
   * splits (returns null).
   *
   * <p>If a split is successful (non-null), then the restriction is updated to the result of the
   * primary.
   */
  @Override
  public @Nullable SplitResult<TimestampRange> trySplit(double fractionOfRemainder) {
    if (InitialPartition.isInitialPartition(partition.getPartitionToken())) {
      return null;
    }

    return super.trySplit(fractionOfRemainder);
  }
}
