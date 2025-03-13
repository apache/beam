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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.reconciler;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.doPartitionsOverlap;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.joda.time.Duration;

@Internal
public class OrphanedMetadataCleaner {
  private static final Duration ORPHANED_NEW_PARTITION_WAIT = Duration.standardMinutes(15);
  List<NewPartition> newPartitions = new ArrayList<>();
  @Nullable List<ByteStringRange> missingPartitions = null;

  /**
   * Add NewPartition if it hasn't been updated for 15 minutes.
   *
   * @param newPartition new partition to clean up.
   */
  public void addIncompleteNewPartitions(NewPartition newPartition) {
    if (newPartition.getLastUpdated().plus(ORPHANED_NEW_PARTITION_WAIT).isBeforeNow()) {
      this.newPartitions.add(newPartition);
    }
  }

  /**
   * Add all the missingPartitions. This must be called in order evaluate orphaned new partition.
   *
   * @param missingPartitions add missingPartitions.
   */
  public void addMissingPartitions(List<ByteStringRange> missingPartitions) {
    if (this.missingPartitions == null) {
      this.missingPartitions = new ArrayList<>();
    }
    this.missingPartitions.addAll(missingPartitions);
  }

  /**
   * Returns a list of NewPartition that have been around for a while and do not overlap with any
   * missing partition.
   *
   * <p>Must call <code>addMissingPartitions</code> before otherwise no orphan can be cleaned up.
   *
   * @return NewPartitions safe to clean up.
   */
  public List<NewPartition> getOrphanedNewPartitions() {
    List<NewPartition> orphanedNewPartitions = new ArrayList<>();
    List<ByteStringRange> missingPartitions = this.missingPartitions;
    if (missingPartitions == null) {
      return orphanedNewPartitions;
    }
    for (NewPartition newPartition : this.newPartitions) {
      boolean hasOverlap = false;
      for (ByteStringRange missingPartition : missingPartitions) {
        if (doPartitionsOverlap(newPartition.getPartition(), missingPartition)) {
          hasOverlap = true;
          break;
        }
      }
      if (!hasOverlap) {
        orphanedNewPartitions.add(newPartition);
      }
    }
    return orphanedNewPartitions;
  }
}
