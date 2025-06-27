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
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionStartRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.RestrictionInterrupter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is part of the process for {@link
 * org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn..ReadChangeStreamPartitionDoFn} SDF. It is
 * responsible for processing {@link PartitionStartRecord}s. The new partition start records will be
 * stored in the Connector's metadata tables in order to be scheduled for future querying by the
 * {@link org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.dofn.DetectNewPartitionsDoFn} SDF.
 */
public class PartitionStartRecordAction {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStartRecordAction.class);
  private final PartitionMetadataDao partitionMetadataDao;
  private final ChangeStreamMetrics metrics;

  /**
   * Constructs an action class for handling {@link PartitionStartRecord}s.
   *
   * @param partitionMetadataDao DAO class to access the Connector's metadata tables
   * @param metrics metrics gathering class
   */
  PartitionStartRecordAction(
      PartitionMetadataDao partitionMetadataDao, ChangeStreamMetrics metrics) {
    this.partitionMetadataDao = partitionMetadataDao;
    this.metrics = metrics;
  }

  /**
   * This is the main processing function for a {@link PartitionStartRecord}. It returns an {@link
   * Optional} of {@link ProcessContinuation} to indicate if the calling function should stop or
   * not. If the {@link Optional} returned is empty, it means that the calling function can continue
   * with the processing. If an {@link Optional} of {@link ProcessContinuation#stop()} is returned,
   * it means that this function was unable to claim the timestamp of the {@link
   * PartitionStartRecord}, so the caller should stop.
   *
   * <p>When processing the {@link PartitionStartRecord} the following procedure is applied:
   *
   * <ol>
   *   <li>We try to claim the partition start record timestamp. If it is not possible, we stop here
   *       and return.
   *   <li>We update the watermark to the partition start record timestamp.
   *   <li>For each partition start record, we try to insert them in the metadata tables if they do
   *       not exist.
   *   <li>For each partition start record, we increment the corresponding metric.
   * </ol>
   *
   * @param partition the current partition being processed
   * @param record the change stream partition start record received
   * @param tracker the restriction tracker of the {@link
   *     com.google.cloud.teleport.spanner.spannerio.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   *     SDF
   * @param interrupter the restriction interrupter suggesting early termination of the processing
   * @param watermarkEstimator the watermark estimator of the {@link
   *     com.google.cloud.teleport.spanner.spannerio.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   *     SDF
   * @return {@link Optional#empty()} if the caller can continue processing more records. A non
   *     empty {@link Optional} with {@link ProcessContinuation#stop()} if this function was unable
   *     to claim the {@link ChildPartitionsRecord} timestamp. A non empty {@link Optional} with
   *     {@link ProcessContinuation#resume()} if this function should commit what has already been
   *     processed and resume.
   */
  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      PartitionStartRecord record,
      RestrictionTracker<TimestampRange, Timestamp> tracker,
      RestrictionInterrupter<Timestamp> interrupter,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    final String token = partition.getPartitionToken();

    LOG.debug("[{}] Processing partition start record {}", token, record);

    final Timestamp startTimestamp = record.getStartTimestamp();
    if (interrupter.tryInterrupt(startTimestamp)) {
      LOG.debug(
          "[{}] Soft deadline reached with partition start records at {}, rescheduling",
          token,
          startTimestamp);
      return Optional.of(ProcessContinuation.resume());
    }
    if (!tracker.tryClaim(startTimestamp)) {
      LOG.debug("[{}] Could not claim queryChangeStream({}), stopping", token, startTimestamp);
      return Optional.of(ProcessContinuation.stop());
    }
    watermarkEstimator.setWatermark(new Instant(startTimestamp.toSqlTimestamp().getTime()));
    for (String startPartitionToken : record.getPartitionTokens()) {
      processStartPartition(partition, record, startPartitionToken);
    }

    LOG.debug("[{}] partition start action completed successfully", token);
    return Optional.empty();
  }

  // Unboxing of runInTransaction result will not produce a null value, we can ignore it
  @SuppressWarnings("nullness")
  private void processStartPartition(
      PartitionMetadata partition, PartitionStartRecord record, String startPartitionToken) {
    LOG.debug("Processing start partition event {}", startPartitionToken);

    final PartitionMetadata row =
        PartitionMetadata.newBuilder()
            .setPartitionToken(startPartitionToken)
            .setStartTimestamp(record.getStartTimestamp())
            .setEndTimestamp(partition.getEndTimestamp())
            .setHeartbeatMillis(partition.getHeartbeatMillis())
            .setState(CREATED)
            .setWatermark(record.getStartTimestamp())
            .build();
    LOG.debug("Inserting start partition token {}", startPartitionToken);
    final Boolean insertedRow =
        partitionMetadataDao
            .runInTransaction(
                transaction -> {
                  if (transaction.getPartition(startPartitionToken) == null) {
                    transaction.insert(row);
                    return true;
                  }
                  return false;
                },
                "InsertStartPartition")
            .getResult();
    if (insertedRow) {
      metrics.incPartitionStartRecordCount();
    } else {
      LOG.debug("Partition start token {} already exists, skipping...", startPartitionToken);
    }
  }
}
