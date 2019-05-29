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

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.transforms.SerializableComparator;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.vendor.guava.v20_0.com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.Partition;

/** Defines a custom partition range. */
public class PartitionRange
    implements Serializable, HasDefaultTracker<PartitionRange, PartitionRangeTracker> {

  private final ImmutableList<Partition> partitions;

  public SerializableComparator<Partition> getComparator() {
    return comparator;
  }

  private SerializableComparator<Partition> comparator;

  private Partition lastCompletedPartition;

  public PartitionRange(
      ImmutableList<Partition> partitions,
      SerializableComparator<Partition> comparator,
      Partition lastCompleted) {
    this.comparator = comparator;
    this.partitions = partitions;
    this.lastCompletedPartition = lastCompleted;
  }

  @Override
  public PartitionRangeTracker newTracker() {
    return new PartitionRangeTracker(this, comparator);
  }

  public ImmutableList<Partition> getPartitions() {
    return partitions;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PartitionRange)) {
      return false;
    }

    PartitionRange that = (PartitionRange) o;
    return Objects.equals(partitions, that.partitions);
  }

  public Partition getLastCompletedPartition() {
    return lastCompletedPartition;
  }

  @Override
  public int hashCode() {
    return partitions != null ? partitions.hashCode() : 0;
  }
}
