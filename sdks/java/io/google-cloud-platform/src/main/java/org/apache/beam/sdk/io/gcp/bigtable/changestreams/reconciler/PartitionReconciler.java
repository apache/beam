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
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.isValidPartition;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.joda.time.Instant;

/**
 * There can be a race when many splits and merges happen to a single partition in quick succession.
 * It's possible that some CloseStream merge messages may be missed. This can lead to an
 * inconsistent state in the metadata table causing some partitions to not be streamed at all. This
 * class tries to reconcile the problem by ensuring that all partitions are streamed. If any
 * partitions are missing for an extended period of time, we fix it by creating a new
 * partitionRecord to stream the partition.
 *
 * <p>Example of race condition:
 *
 * <ol>
 *   <li>Bigtable: decides to merge A-B and B-C to A-C
 *   <li>Beam A-B: receives CloseStream to merge into Partition A-C . Creates entry in metadata and
 *       terminates the stream.
 *   <li>Beam B-C: is not currently streaming because it just check pointed and hasn't restarted
 *       yet.
 *   <li>Bigtable: decides maybe merge wasn't good, splits A-C back into A-B and B-C
 *   <li>Beam B-C: restarts now, but it never receives the 1st CloseStream merge message and it
 *       never will because CloseStream messages are not queued and because when requesting Change
 *       Stream for B-C, Bigtable recognizes that B-C does exist, so it's happy to start the stream.
 *   <li>Beam A-B: doesn't exist... it's in the metadata table waiting for B-C to merge into A-C.
 * </ol>
 *
 * <p>To reconcile this, we identify partitions that haven't been streamed for at least 5 minutes.
 * This is probably an indication that there were some races of CloseStream merge messages.
 */
@Internal
public class PartitionReconciler {
  HashMap<ByteStringRange, Set<ByteString>> partitionsToReconcile = new HashMap<>();
  HashMap<ByteStringRange, ByteString> newPartitions = new HashMap<>();
  MetadataTableDao metadataTableDao;

  // The amount of delay allowed before we consider a partition to be probably missing.
  private static final long MISSING_PARTITION_DELAY_MILLI = 5 * 60 * 1000L;

  public PartitionReconciler(MetadataTableDao metadataTableDao) {
    this.metadataTableDao = metadataTableDao;
  }

  /**
   * Capture partitions that are not currently being streamed. This should be the result of
   * observing the metadata table to identify missing StreamPartition rows. All the StreamPartitions
   * rows combined should form continuous, non-overlapping partitions covering all row keys.
   *
   * <p>Combine existing missing partitions and current (newly added) missing partitions. If missing
   * partitions have been missing for more than allotted time, it will be reconciled.
   *
   * <p>It is possible that a missing partition's boundary can change frequently, such that it can
   * take a long time to realize a partition is truly missing. For example, if [C, D) is missing,
   * but there are a lot of splits and merges around [C, D), we may see that sometimes [B,D) is
   * missing, or at other times [C-E) is missing due to split and merge activities of [B-C) and
   * [D-E), while [C-D) is truly missing. The moving boundaries would reset the timer leading to
   * slower reconciliation of the missing partition.
   *
   * @param missingPartitions partitions not being streamed.
   */
  public void addMissingPartitions(List<ByteStringRange> missingPartitions) {
    HashMap<ByteStringRange, Long> alreadyMissingPartitions =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    HashMap<ByteStringRange, Long> missingPartitionDuration = new HashMap<>();
    long now = Instant.now().getMillis();

    for (ByteStringRange missingPartition : missingPartitions) {
      if (!isValidPartition(missingPartition)) {
        continue;
      }
      if (alreadyMissingPartitions.containsKey(missingPartition)) {
        missingPartitionDuration.put(
            missingPartition, alreadyMissingPartitions.get(missingPartition));
        if (alreadyMissingPartitions.get(missingPartition) + MISSING_PARTITION_DELAY_MILLI < now) {
          partitionsToReconcile.put(missingPartition, new HashSet<>());
        }
      } else {
        missingPartitionDuration.put(missingPartition, now);
      }
    }
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDuration);
  }

  /**
   * Capture NewPartition row that's waiting to be created. If any of these NewPartition row
   * overlaps with partition we notice are missing and needs to be reconciled, we will need to clean
   * up these NewPartition to avoid future conflicts and inconsistencies.
   *
   * @param partition new partitions waiting to be created.
   * @param rowKey the full row key of the new partition.
   */
  public void addNewPartition(ByteStringRange partition, ByteString rowKey) {
    newPartitions.put(partition, rowKey);
  }

  /**
   * Find overlapping partitions between partitionToReconcile and newPartitions. This is to support
   * the effort of identifying the new partition rows that are stuck and needs to be cleaned up
   * because we have successfully reconciled the problem and created a new partition.
   *
   * @param partitionToReconcile partition that will be created
   * @return a set of new partitions that overlaps with partitionToReconcile
   */
  private Set<ByteString> findOverlappingNewPartitions(ByteStringRange partitionToReconcile) {
    Set<ByteString> overlappingRowKey = new HashSet<>();
    for (ByteStringRange newPartition : newPartitions.keySet()) {
      if (doPartitionsOverlap(newPartition, partitionToReconcile)) {
        overlappingRowKey.add(newPartitions.get(newPartition));
      }
    }
    return overlappingRowKey;
  }

  /**
   * Match partitions that have been missing for a while and need to be reconciled with NewPartition
   * row key. Find NewPartition row key that overlaps with the reconciled partitions to clean them
   * up.
   *
   * @return missing partitions and related NewPartition rows keys to delete.
   */
  public HashMap<ByteStringRange, Set<ByteString>> getPartitionsToReconcile() {
    partitionsToReconcile.replaceAll((r, v) -> findOverlappingNewPartitions(r));
    return partitionsToReconcile;
  }
}
