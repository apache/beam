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

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.coverSameKeySpace;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.doPartitionsOverlap;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.getMissingPartitionsFrom;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  private static final Logger LOG = LoggerFactory.getLogger(PartitionReconciler.class);

  private HashMap<ByteStringRange, Instant> missingPartitionDurations = new HashMap<>();
  private final List<NewPartition> newPartitions = new ArrayList<>();
  private final MetadataTableDao metadataTableDao;
  private final ChangeStreamMetrics metrics;
  // Ensure that we added the missing partitions before writing the missing partitions to the
  // metadata table.
  private boolean hasAddedMissingPartitions = false;

  // The amount of delay allowed before we consider a partition to be probably missing.
  private static final Duration MISSING_PARTITION_SHORT_DELAY = Duration.standardMinutes(2);
  private static final Duration MISSING_PARTITION_LONG_DELAY = Duration.standardMinutes(20);

  public PartitionReconciler(MetadataTableDao metadataTableDao, ChangeStreamMetrics metrics) {
    this.metadataTableDao = metadataTableDao;
    this.metrics = metrics;
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
    hasAddedMissingPartitions = true;
    HashMap<ByteStringRange, Instant> alreadyMissingPartitionDurations =
        metadataTableDao.readDetectNewPartitionMissingPartitions();
    missingPartitionDurations = new HashMap<>();
    Instant now = Instant.now();

    for (ByteStringRange missingPartition : missingPartitions) {
      missingPartitionDurations.put(
          missingPartition, alreadyMissingPartitionDurations.getOrDefault(missingPartition, now));
    }
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);
  }

  /**
   * Capture NewPartition row that cannot merge on its own. If any of these NewPartition row
   * overlaps with partition we notice are missing and needs to be reconciled, we will need to clean
   * up these NewPartition to avoid future conflicts and inconsistencies.
   *
   * @param newPartition new partition waiting to be created.
   */
  public void addIncompleteNewPartitions(NewPartition newPartition) {
    newPartitions.add(newPartition);
  }

  /**
   * Get a list of single token new partitions that have parent partition overlapping
   * missingPartitions. This is to support the effort of identifying the new partition rows that are
   * stuck and needs to be cleaned up because we have successfully reconciled the problem and
   * created a new partition.
   *
   * @param missingPartition partition that will be created
   * @return a set of new partitions that overlaps with partitionToReconcile
   */
  private List<NewPartition> findOverlappingNewPartitions(ByteStringRange missingPartition) {
    // TODO: Possibly precompute a map from parentPartitions to newPartitions on the first call to
    //  this function so we don't have to repeat it.
    List<NewPartition> overlappingNewPartitions = new ArrayList<>();
    for (NewPartition newPartition : newPartitions) {
      for (ByteStringRange parentPartition : newPartition.getParentPartitions()) {
        if (doPartitionsOverlap(parentPartition, missingPartition)) {
          // Return a NewPartition that only contains the parentPartition.
          NewPartition splicedNewPartition =
              newPartition.getSingleTokenNewPartition(parentPartition);
          if (splicedNewPartition == null) {
            continue;
          }
          overlappingNewPartitions.add(splicedNewPartition);
        }
      }
    }
    return overlappingNewPartitions;
  }

  /**
   * For missing partitions, try to organize the mismatched parent tokens in a way to fill the
   * missing partitions.
   *
   * <p>Must call {@link #addMissingPartitions(List)} before this.
   *
   * <p>If there are parent tokens that when combined form a missing partition, it can be outputted
   * as a merge of the missing partition.
   *
   * <p>If there are no parent tokens for a missing partition, it will need to be reconciled with
   * adjusted low watermark. This is a catch-all solution. We don't expect to ever get into this
   * situation. Missing partitions should all be mismatched merges that can be reconciled by
   * organizing them correctly.
   *
   * @param lowWatermark watermark that all reconciled partition should have
   * @param startTime to help compute optimal reconcile point
   * @return reconciled PartitionRecord.
   */
  public List<PartitionRecord> getPartitionsToReconcile(Instant lowWatermark, Instant startTime) {
    // We update the metadata table with the partitions that are still missing after reconciliation.
    // So we must ensure that we have already added the missing partitions, otherwise, we will
    // update the metadata table with an empty list of missing partitions.
    if (!hasAddedMissingPartitions) {
      return Collections.emptyList();
    }
    // Reset to ensure we get an updated list of missing partitions before reconciling again.
    hasAddedMissingPartitions = false;
    // This value is calculated in case that we reconcile without continuation tokens, we will use
    // an hour prior to low watermark because low watermark is only an estimate. By reading back 1
    // hour, it should cover any changes missed. We also want to make sure that the reconcile time
    // isn't before the start time of the pipeline.
    Instant reconciledTime = lowWatermark.minus(Duration.standardMinutes(60));
    if (reconciledTime.compareTo(startTime) < 0) {
      reconciledTime = startTime;
    }
    // PartitionRecord formed using mismatched partitions by the reconciler
    List<PartitionRecord> partitionsToReconcile = new ArrayList<>();
    // Partitions from the list of missing partitions that are reconciled and no longer missing
    List<ByteStringRange> missingPartitionsToRemove = new ArrayList<>();
    for (Map.Entry<ByteStringRange, Instant> partitionDuration :
        missingPartitionDurations.entrySet()) {
      // The partition hasn't been missing for even the short duration.
      if (!partitionDuration.getValue().plus(MISSING_PARTITION_SHORT_DELAY).isBeforeNow()) {
        continue;
      }
      ByteStringRange missingPartition = partitionDuration.getKey();
      List<NewPartition> overlappingNewPartitions = findOverlappingNewPartitions(missingPartition);
      List<ByteStringRange> overlappingParentPartitions = new ArrayList<>();
      for (NewPartition newPartition : overlappingNewPartitions) {
        overlappingParentPartitions.add(newPartition.getParentPartitions().get(0));
      }
      // If the parents are equal to the missing partition, a new partition can be formed.
      if (coverSameKeySpace(overlappingParentPartitions, missingPartition)) {
        List<ChangeStreamContinuationToken> allTokens = new ArrayList<>();
        for (NewPartition newPartition : overlappingNewPartitions) {
          allTokens.add(newPartition.getChangeStreamContinuationTokens().get(0));
        }

        metrics.incPartitionReconciledWithTokenCount();
        PartitionRecord record =
            new PartitionRecord(
                missingPartition, allTokens, lowWatermark, overlappingNewPartitions);
        // Remove this partition from the list of missing partitions.
        missingPartitionsToRemove.add(missingPartition);
        partitionsToReconcile.add(record);
        continue;
      }
      // The parents are not equal to the missing partition. If the missing partition has been
      // around for more than 20 minutes, it needs to be reconciled.
      if (partitionDuration.getValue().plus(MISSING_PARTITION_LONG_DELAY).isBeforeNow()) {
        // Try outputting the parent partitions as their own new partition, so they can get more
        // recent merge targets.
        for (NewPartition newPartition : overlappingNewPartitions) {
          metrics.incPartitionReconciledWithTokenCount();
          PartitionRecord record =
              new PartitionRecord(
                  newPartition.getChangeStreamContinuationTokens().get(0).getPartition(),
                  newPartition.getChangeStreamContinuationTokens(),
                  lowWatermark,
                  Collections.singletonList(newPartition));
          partitionsToReconcile.add(record);
        }
        // Also output partition not overlapped by new partitions.
        // Get the missing partition from parentPartitions and missingPartition.
        List<ByteStringRange> missingPartitionsFromParents =
            getMissingPartitionsFrom(
                overlappingParentPartitions,
                missingPartition.getStart(),
                missingPartition.getEnd());
        for (ByteStringRange missing : missingPartitionsFromParents) {
          // This partition is truly missing. There are no tokens representing this. We restart at
          // the calculated reconcile time.
          metrics.incPartitionReconciledWithoutTokenCount();
          PartitionRecord record =
              new PartitionRecord(missing, reconciledTime, lowWatermark, Collections.emptyList());
          partitionsToReconcile.add(record);
          LOG.error("DNP: Reconciling partition because we're missing a token {}", record);
        }

        // Remove this partition from the list of missing partitions.
        missingPartitionsToRemove.add(missingPartition);
      }
    }

    // Write to the metadata table without the reconciled partitions.
    for (ByteStringRange missingPartitionToRemove : missingPartitionsToRemove) {
      missingPartitionDurations.remove(missingPartitionToRemove);
    }
    metadataTableDao.writeDetectNewPartitionMissingPartitions(missingPartitionDurations);

    return partitionsToReconcile;
  }
}
