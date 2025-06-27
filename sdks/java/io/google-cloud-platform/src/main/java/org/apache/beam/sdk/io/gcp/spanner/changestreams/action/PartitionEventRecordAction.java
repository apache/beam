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

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionEventRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
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
 * org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF. It is
 * responsible for processing {@link PartitionEventRecord}s. The records will be used to progress
 * the watermark for the current element (partition). Note that because of the returned list of data
 * change record by the connector is not ordered, so the MoveInEvent, MoveOutEvent are not processed
 * by the connector.
 */
public class PartitionEventRecordAction {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionEventRecordAction.class);
  private final ChangeStreamMetrics metrics;

  /**
   * Constructs an action class for handling {@link PartitionEventRecord}s.
   *
   * @param metrics metrics gathering class
   */
  PartitionEventRecordAction(ChangeStreamMetrics metrics) {
    this.metrics = metrics;
  }

  /**
   * This is the main processing function for a {@link PartitionEventRecord}. It returns an {@link
   * Optional} of {@link ProcessContinuation} to indicate if the calling function should stop or
   * not. If the {@link Optional} returned is empty, it means that the calling function can continue
   * with the processing. If an {@link Optional} of {@link ProcessContinuation#stop()} is returned,
   * it means that this function was unable to claim the timestamp of the {@link
   * PartitionEventRecord}, so the caller should stop. If an {@link Optional} of {@link
   * ProcessContinuation#resume()} is returned, it means that this function should not attempt to
   * claim further timestamps of the {@link PartitionEventRecord}, but instead should commit what it
   * has processed so far.
   *
   * <p>When processing the {@link PartitionEventRecord} the following procedure is applied:
   *
   * <ol>
   *   <li>We try to claim the partition end record timestamp. If it is not possible, we stop here
   *       and return.
   *   <li>We update the necessary metrics.
   *   <li>We update the watermark to the partition end record timestamp.
   * </ol>
   */
  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      PartitionEventRecord record,
      RestrictionTracker<TimestampRange, Timestamp> tracker,
      RestrictionInterrupter<Timestamp> interrupter,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    final String token = partition.getPartitionToken();
    LOG.debug("[{}] Processing partition event record {}", token, record);

    final Timestamp timestamp = record.getCommitTimestamp();
    final Instant timestampInstant = new Instant(timestamp.toSqlTimestamp().getTime());
    if (interrupter.tryInterrupt(timestamp)) {
      LOG.debug(
          "[{}] Soft deadline reached with partition event record at {}, rescheduling",
          token,
          timestamp);
      return Optional.of(ProcessContinuation.resume());
    }
    if (!tracker.tryClaim(timestamp)) {
      LOG.debug("[{}] Could not claim queryChangeStream({}), stopping", token, timestamp);
      return Optional.of(ProcessContinuation.stop());
    }
    metrics.incPartitionEventRecordCount();
    watermarkEstimator.setWatermark(timestampInstant);

    LOG.debug("[{}] Partition event record action completed successfully", token);
    return Optional.empty();
  }
}
