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
package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.PARTITION_MERGE_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.PARTITION_SPLIT_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.CREATED;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;

import com.google.cloud.Timestamp;
import java.util.Collections;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class ChildPartitionsRecordAction {

  private static final Logger LOG = LoggerFactory.getLogger(ChildPartitionsRecordAction.class);
  private final PartitionMetadataDao partitionMetadataDao;

  public ChildPartitionsRecordAction(PartitionMetadataDao partitionMetadataDao) {
    this.partitionMetadataDao = partitionMetadataDao;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      ChildPartitionsRecord record,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    final String token = partition.getPartitionToken();
    LOG.debug("[" + token + "] Processing child partition record " + record);

    final Timestamp startTimestamp = record.getStartTimestamp();
    if (!tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))) {
      LOG.debug(
          "[" + token + "] Could not claim queryChangeStream(" + startTimestamp + "), stopping");
      return Optional.of(ProcessContinuation.stop());
    }
    watermarkEstimator.setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));

    for (ChildPartition childPartition : record.getChildPartitions()) {
      if (isSplit(childPartition)) {
        LOG.debug("[" + token + "] Processing child partition split event");
        PARTITION_SPLIT_COUNTER.inc();

        final PartitionMetadata row =
            toPartitionMetadata(
                record.getStartTimestamp(),
                partition.getEndTimestamp(),
                partition.getHeartbeatMillis(),
                childPartition);
        // Updates the metadata table
        // FIXME: Figure out what to do if this throws an exception
        // TODO: Make sure this does not fail if the rows already exist
        partitionMetadataDao.insert(row);
      } else {
        LOG.debug("[" + token + "] Processing child partition merge event");
        PARTITION_MERGE_COUNTER.inc();

        partitionMetadataDao.runInTransaction(
            transaction -> {
              final long finishedParents =
                  transaction.countPartitionsInStates(
                      childPartition.getParentTokens(), Collections.singletonList(FINISHED));

              if (finishedParents == childPartition.getParentTokens().size() - 1) {
                LOG.debug(
                    "["
                        + token
                        + "] All parents are finished, inserting child partition "
                        + childPartition);
                transaction.insert(
                    toPartitionMetadata(
                        record.getStartTimestamp(),
                        partition.getEndTimestamp(),
                        partition.getHeartbeatMillis(),
                        childPartition));
              } else {
                LOG.debug(
                    "["
                        + token
                        + "] At least one parent is not finished ("
                        + "finishedParents = "
                        + finishedParents
                        + ", "
                        + "expectedToBeFinished = "
                        + (childPartition.getParentTokens().size() - 1)
                        + "), skipping child partition insertion");
              }

              return null;
            });
      }
    }

    LOG.debug("[" + token + "] Child partitions action completed successfully");
    return Optional.empty();
  }

  private boolean isSplit(ChildPartition childPartition) {
    return childPartition.getParentTokens().size() == 1;
  }

  private PartitionMetadata toPartitionMetadata(
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      long heartbeatMillis,
      ChildPartition childPartition) {
    return PartitionMetadata.newBuilder()
        .setPartitionToken(childPartition.getToken())
        .setParentTokens(childPartition.getParentTokens())
        .setStartTimestamp(startTimestamp)
        .setInclusiveStart(true)
        .setEndTimestamp(endTimestamp)
        .setInclusiveEnd(false)
        .setHeartbeatMillis(heartbeatMillis)
        .setState(CREATED)
        .build();
  }
}
