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

import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;

/**
 * This restriction tracker is a decorator on top of the {@link OffsetRangeTracker}. It modifies the
 * behaviour of {@link OffsetRangeTracker#tryClaim(Long)} to ignore claims for the same long
 * multiple times. This is because several change stream records might have the same timestamp, thus
 * leading to multiple claims of the same {@link Long}. Other than that, it modifies the {@link
 * OffsetRangeTracker#trySplit(double)} method to always deny splits for the {@link
 * InitialPartition#PARTITION_TOKEN}, since we only need to perform this query once.
 */
@SuppressWarnings({
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class ReadChangeStreamPartitionRangeTracker extends OffsetRangeTracker {

  private final PartitionMetadata partition;

  /**
   * Receives the partition that will be queried and be using this tracker, alongside the range
   * itself.
   *
   * @param partition the partition that will use the tracker
   * @param range closed / open range interval representing the start / end times for a partition
   */
  public ReadChangeStreamPartitionRangeTracker(PartitionMetadata partition, OffsetRange range) {
    super(range);
    this.partition = partition;
  }

  /**
   * Attempts to claim the given offset.
   *
   * <p>Must be equal or larger than the last successfully claimed offset.
   *
   * @return {@code true} if the offset was successfully claimed, {@code false} if it is outside the
   *     current {@link OffsetRange} of this tracker (in that case this operation is a no-op).
   */
  @Override
  public boolean tryClaim(Long i) {
    if (i.equals(lastAttemptedOffset)) {
      return true;
    }
    return super.tryClaim(i);
  }

  /**
   * If the partition token is the {@link InitialPartition#PARTITION_TOKEN}, it does not allow for
   * splits (returns null).
   */
  @Override
  public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
    if (InitialPartition.isInitialPartition(partition.getPartitionToken())) {
      return null;
    }
    return super.trySplit(fractionOfRemainder);
  }
}
