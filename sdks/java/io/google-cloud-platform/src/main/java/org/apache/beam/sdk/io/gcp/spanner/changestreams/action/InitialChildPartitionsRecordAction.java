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

public class InitialChildPartitionsRecordAction {

  private static final Logger LOG =
      LoggerFactory.getLogger(InitialChildPartitionsRecordAction.class);
  private final PartitionMetadataDao partitionMetadataDao;

  InitialChildPartitionsRecordAction(PartitionMetadataDao partitionMetadataDao) {
    this.partitionMetadataDao = partitionMetadataDao;
  }

  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      HashSet<ChildPartitionsRecord> records,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    final String token = partition.getPartitionToken();

    Optional<ChildPartitionsRecord> firstRecord = records.stream().findFirst();
    if (firstRecord.isPresent()) {
      Timestamp startTimestamp = firstRecord.get().getStartTimestamp();

      final Instant startInstant = new Instant(startTimestamp.toSqlTimestamp().getTime());
      if (!tracker.tryClaim(PartitionPosition.queryChangeStream(startTimestamp))) {
        LOG.debug(
            "[" + token + "] Could not claim queryChangeStream(" + startTimestamp + "), stopping");
        return Optional.of(ProcessContinuation.stop());
      }
      watermarkEstimator.setWatermark(startInstant);
    } else {
      throw new IllegalStateException("Partition " + token + " not found in metadata table");
    }

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
                  LOG.debug(
                      "[" + token + "] Inserted initial child tokens: " + childTokens.toString());

                  // We want to insert all these child tokens under the original token.
                  HashSet<String> childPartitionTokens = transaction.getChildTokens(token);
                  if (childPartitionTokens == null) {
                    throw new IllegalStateException(
                        "Partition " + token + " not found in metadata table");
                  }
                  childPartitionTokens.addAll(childTokens);
                  transaction.insertChildTokens(token, childTokens);
                  LOG.debug("[" + token + "] Added child tokens " + childTokens.toString());
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
