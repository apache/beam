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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.action;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State.CREATED;

import com.google.cloud.Timestamp;
import java.util.HashSet;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is part of the process for {@link
 * org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF. It is
 * responsible for processing {@link ChildPartitionsRecord}s. The new child partitions will be
 * stored in the Connector's metadata tables in order to be scheduled for future querying by the
 * {@link org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.DetectNewPartitionsDoFn} SDF.
 */
public class InitialChildPartitionsRecordAction {

  private static final Logger LOG =
      LoggerFactory.getLogger(InitialChildPartitionsRecordAction.class);
  private final PartitionMetadataDao partitionMetadataDao;

  /**
   * Constructs an action class for handling {@link ChildPartitionsRecord}s.
   *
   * @param partitionMetadataDao DAO class to access the Connector's metadata tables
   * @param metrics metrics gathering class
   */
  InitialChildPartitionsRecordAction(PartitionMetadataDao partitionMetadataDao) {
    this.partitionMetadataDao = partitionMetadataDao;
  }

  /**
   * This is the main processing function for a {@link ChildPartitionsRecord}. It returns an {@link
   * Optional} of {@link ProcessContinuation} to indicate if the calling function should stop or
   * not. If the {@link Optional} returned is empty, it means that the calling function can continue
   * with the processing. If an {@link Optional} of {@link ProcessContinuation#stop()} is returned,
   * it means that this function was unable to claim the timestamp of the {@link
   * ChildPartitionsRecord}, so the caller should stop.
   *
   * <p>When processing the {@link ChildPartitionsRecord} the following procedure is applied:
   *
   * <ol>
   *   <li>We try to claim the child partition record timestamp. If it is not possible, we stop here
   *       and return.
   *   <li>We update the watermark to the child partition record timestamp.
   *   <li>For each child partition, we try to insert them in the metadata tables if they do not
   *       exist.
   *   <li>For each child partition, we check if they originate from a split or a merge and
   *       increment the corresponding metric.
   * </ol>
   *
   * Dealing with partition splits and merge cases is detailed below:
   *
   * <ul>
   *   <li>Partition Splits: child partition tokens should not exist in the partition metadata
   *       table, so new rows are just added to such table. In case of a bundle retry, we silently
   *       ignore duplicate entries.
   *   <li>Partition Merges: the first parent partition that receives the child token should succeed
   *       in inserting it. The remaining parents will silently ignore and skip the insertion.
   * </ul>
   *
   * @param partition the current partition being processed
   * @param record the change stream child partition record received
   * @param tracker the restriction tracker of the {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   * @param watermarkEstimator the watermark estimator of the {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   * @return {@link Optional#empty()} if the caller can continue processing more records. A non
   *     empty {@link Optional} with {@link ProcessContinuation#stop()} if this function was unable
   *     to claim the {@link ChildPartitionsRecord} timestamp
   */
  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      HashSet<ChildPartitionsRecord> records,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    final String token = partition.getPartitionToken();
    LOG.info("HERE IN InitialPartitionRecord Action");

    if (records.isEmpty()) {
      throw new IllegalStateException("Partition " + token + " not found in metadata table");
    }
    Timestamp startTimestamp = Timestamp.now();
    for (ChildPartitionsRecord record : records) {
      startTimestamp = record.getStartTimestamp();
      break;
    }

    final Instant startInstant = new Instant(startTimestamp.toSqlTimestamp().getTime());
    if (!tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))) {
      LOG.info(
          "[" + token + "] Could not claim queryChangeStream(" + startTimestamp + "), stopping");
      return Optional.of(ProcessContinuation.stop());
    }
    watermarkEstimator.setWatermark(startInstant);

    final Boolean insertedChildTokens =
        partitionMetadataDao
            .runInTransaction(
                transaction -> {
                  HashSet<String> childTokens = new HashSet<String>();
                  for (ChildPartitionsRecord record : records) {
                    for (ChildPartition childPartition : record.getChildPartitions()) {
                      if (transaction.getPartition(childPartition.getToken()) == null) {
                        final PartitionMetadata row =
                            toPartitionMetadata(
                                record.getStartTimestamp(),
                                partition.getEndTimestamp(),
                                partition.getHeartbeatMillis(),
                                childPartition);
                        childTokens.add(childPartition.getToken());
                        transaction.insert(row);
                      }
                    }
                  }
                  LOG.info("Inserted initial child tokens: " + childTokens.toString());
                  // We want to insert all these child tokens under the original token.
                  HashSet<String> childPartitionTokens = transaction.getChildTokens(token);
                  if (childPartitionTokens == null) {
                    throw new IllegalStateException(
                        "Partition " + token + " not found in metadata table");
                  }
                  childPartitionTokens.addAll(childTokens);
                  transaction.insertChildTokens(token, childTokens);
                  LOG.info("[" + token + "] Added child tokens " + childTokens.toString());
                  return true;
                })
            .getResult();

    LOG.debug(
        "[" + token + "] Child partitions action completed successfully: " + insertedChildTokens);
    return Optional.empty();
  }

  private PartitionMetadata toPartitionMetadata(
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      long heartbeatMillis,
      ChildPartition childPartition) {
    return PartitionMetadata.newBuilder()
        .setPartitionToken(childPartition.getToken())
        .setChildTokens(Sets.newHashSet())
        .setStartTimestamp(startTimestamp)
        .setEndTimestamp(endTimestamp)
        .setHeartbeatMillis(heartbeatMillis)
        .setState(CREATED)
        .setWatermark(startTimestamp)
        .build();
  }
}
