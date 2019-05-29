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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.splittabledofn.Backlog;
import org.apache.beam.sdk.transforms.splittabledofn.Backlogs;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v20_0.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.Partition;

/** Restriction tracker to claim ranges for partitions in a monotonically increasing fashion. */
public class PartitionRangeTracker extends RestrictionTracker<PartitionRange, Partition>
    implements Backlogs.HasBacklog {

  private PartitionRange range;
  private SerializableComparator<Partition> comparator;

  @Nullable private Partition lastClaimedPartition = null;
  @Nullable private Partition lastAttemptedPartition = null;

  @Override
  public boolean tryClaim(Partition partition) {
    lastAttemptedPartition = partition;
    if (lastClaimedPartition == null) {
      // We are claiming a partition for the first time.
      lastClaimedPartition = partition;
      return true;
    }

    if (comparator.compare(lastClaimedPartition, partition) > 0) {
      return false;
    }
    lastClaimedPartition = partition;
    return true;
  }

  public PartitionRangeTracker(
      PartitionRange partitionRange, SerializableComparator<Partition> comparator) {
    this.range = checkNotNull(partitionRange);
    this.comparator = checkNotNull(comparator);
  }

  @Override
  public PartitionRange currentRestriction() {
    return range;
  }

  @Override
  public PartitionRange checkpoint() {
    checkState(
        lastClaimedPartition != null,
        "Can't checkpoint before any partition was successfully claimed");
    final ImmutableList<Partition> allPartitions = range.getPartitions();
    List<Partition> forSort = new ArrayList<>(allPartitions);
    Collections.sort(forSort, comparator);
    final int lastClaimedPartitionIndex = forSort.indexOf(lastClaimedPartition);
    final List<Partition> unprocessedPartitions =
        forSort.subList(lastClaimedPartitionIndex + 1, forSort.size());
    this.range =
        new PartitionRange(
            ImmutableList.copyOf(forSort.subList(0, lastClaimedPartitionIndex + 1)),
            comparator,
            lastClaimedPartition);
    return new PartitionRange(
        ImmutableList.copyOf(unprocessedPartitions), comparator, lastClaimedPartition);
  }

  @Override
  public void checkDone() throws IllegalStateException {
    final int indexOfLastClaimed = range.getPartitions().indexOf(lastClaimedPartition);
    final ImmutableList<Partition> partitions = range.getPartitions();
    final ArrayList<Partition> partitionsCopy = new ArrayList<>(partitions);

    Collections.sort(partitionsCopy, comparator);
    checkState(
        indexOfLastClaimed <= partitions.size(),
        "Last attempted partition was %s in range %s, claiming work in [%s) was not attempted",
        lastAttemptedPartition,
        range,
        range.getPartitions());
  }

  @Override
  public Backlog getBacklog() {
    // If we have never attempted a partition, we return the length of the entire range.
    if (lastAttemptedPartition == null) {
      return Backlog.of(BigDecimal.valueOf(range.getPartitions().size()));
    }

    // Otherwise we return the length from where we are to where we are attempting to get to
    // with a minimum of zero in case we have claimed beyond the end of the partition range.
    return Backlog.of(
        BigDecimal.valueOf(
            Math.max(
                range.getPartitions().size()
                    - range.getPartitions().indexOf(lastAttemptedPartition),
                0)));
  }

  public PartitionRange getRange() {
    return range;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("range", range)
        .add("lastClaimedPartition", lastClaimedPartition)
        .add("lastAttemptedPartition", lastAttemptedPartition)
        .toString();
  }
}
