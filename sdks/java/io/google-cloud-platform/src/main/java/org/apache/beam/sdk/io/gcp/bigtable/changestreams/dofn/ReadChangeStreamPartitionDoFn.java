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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ChangeStreamAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.action.ReadChangeStreamPartitionAction;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.CoderSizeEstimator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.NullSizeEstimator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.SizeEstimator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.ReadChangeStreamPartitionProgressTracker;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.apache.beam.sdk.util.SerializableSupplier;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Allows for readChangeStreamPartitionAction setup
@SuppressWarnings({"initialization.fields.uninitialized", "dereference.of.nullable"})
@Internal
@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn
    extends DoFn<PartitionRecord, KV<ByteString, ChangeStreamRecord>> {
  private static final long serialVersionUID = 4418739381635104479L;
  private static final BigDecimal MAX_DOUBLE = BigDecimal.valueOf(Double.MAX_VALUE);
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);
  private static final Duration HEARTBEAT_DURATION = Duration.standardSeconds(1);

  private final DaoFactory daoFactory;
  private final ChangeStreamMetrics metrics;
  private final ActionFactory actionFactory;
  private final Duration backlogReplicationAdjustment;
  private SizeEstimator<KV<ByteString, ChangeStreamRecord>> sizeEstimator;
  private ReadChangeStreamPartitionAction readChangeStreamPartitionAction;
  private final SerializableSupplier<Instant> clock;

  public ReadChangeStreamPartitionDoFn(
      DaoFactory daoFactory,
      ActionFactory actionFactory,
      ChangeStreamMetrics metrics,
      Duration backlogReplicationAdjustment) {
    this(daoFactory, actionFactory, metrics, backlogReplicationAdjustment, Instant::now);
  }

  @VisibleForTesting
  ReadChangeStreamPartitionDoFn(
      DaoFactory daoFactory,
      ActionFactory actionFactory,
      ChangeStreamMetrics metrics,
      Duration backlogReplicationAdjustment,
      SerializableSupplier<Instant> clock) {
    this.daoFactory = daoFactory;
    this.metrics = metrics;
    this.actionFactory = actionFactory;
    this.backlogReplicationAdjustment = backlogReplicationAdjustment;
    this.sizeEstimator = new NullSizeEstimator<>();
    this.clock = clock;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Element PartitionRecord partitionRecord) {
    return partitionRecord.getParentLowWatermark();
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new Manual(watermarkEstimatorState);
  }

  @GetInitialRestriction
  public StreamProgress initialRestriction() {
    metrics.incPartitionStreamCount();
    return new StreamProgress();
  }

  @NewTracker
  public ReadChangeStreamPartitionProgressTracker restrictionTracker(
      @Restriction StreamProgress restriction) {
    return new ReadChangeStreamPartitionProgressTracker(restriction);
  }

  @GetSize
  public double getSize(@Restriction StreamProgress streamProgress) {
    if (streamProgress == null) {
      return 0d;
    }
    Instant lowWatermark = streamProgress.getEstimatedLowWatermark();
    BigDecimal estimatedThroughput = streamProgress.getThroughputEstimate();
    Instant lastRunTimestamp = streamProgress.getLastRunTimestamp();
    // This should only be null if:
    // 1) We've failed to lock for the partition in which case we expect 0 throughput
    // 2) We've received a CloseStream in which case we won't process any more data for
    //    this partition
    // 3) RCSP has just started and hasn't completed a checkpoint yet, in which case we can't
    //    estimate throughput yet
    if (lowWatermark == null || estimatedThroughput == null || lastRunTimestamp == null) {
      return 0;
    }

    String partition = "";
    if (streamProgress.getCurrentToken() != null) {
      partition =
          ByteStringRangeHelper.formatByteStringRange(
              Preconditions.checkNotNull(streamProgress.getCurrentToken()).getPartition());
    }

    // Heartbeat lowWatermark takes up to a minute to update on the server. We don't want
    // this to count against the backlog and prevent scaling down, so we estimate heartbeat backlog
    // using the time we most recently processed a heartbeat. Otherwise, (for mutations) we use the
    // watermark.
    long processingTimeLagMillis =
        clock.get().getMillis() - streamProgress.getLastRunTimestamp().getMillis();
    Duration watermarkLag = Duration.millis(clock.get().getMillis() - lowWatermark.getMillis());
    // Remove the backlogReplicationAdjustment from watermarkLag to allow replicated instances to
    // downscale more easily.
    long adjustedWatermarkLagMillis =
        Math.max(0, watermarkLag.minus(backlogReplicationAdjustment).getMillis());
    long lagInMillis =
        streamProgress.isHeartbeat() ? processingTimeLagMillis : adjustedWatermarkLagMillis;
    // Return the estimated bytes per second throughput multiplied by the amount of known work
    // outstanding (watermark lag). Cap at max double to avoid overflow.
    double estimatedSize =
        estimatedThroughput
            .multiply(BigDecimal.valueOf(lagInMillis))
            .divide(BigDecimal.valueOf(1000), 3, RoundingMode.DOWN)
            .min(MAX_DOUBLE)
            // Lag can be negative from clock skew. We treat that as caught up, so
            // it should return zero.
            .max(BigDecimal.ZERO)
            .doubleValue();

    LOG.debug(
        "Estimated size (per second): partition: {}, isHeartbeat: {}, throughputBytes: {} x watermarkLagMillis {} = {}, lastRun = {}",
        partition,
        streamProgress.isHeartbeat(),
        estimatedThroughput,
        lagInMillis,
        estimatedSize,
        streamProgress.getLastRunTimestamp());
    return estimatedSize;
  }

  @Setup
  public void setup() throws IOException {
    MetadataTableDao metadataTableDao = daoFactory.getMetadataTableDao();
    ChangeStreamDao changeStreamDao = daoFactory.getChangeStreamDao();
    ChangeStreamAction changeStreamAction = actionFactory.changeStreamAction(this.metrics);
    readChangeStreamPartitionAction =
        actionFactory.readChangeStreamPartitionAction(
            metadataTableDao,
            changeStreamDao,
            metrics,
            changeStreamAction,
            HEARTBEAT_DURATION,
            sizeEstimator);
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionRecord partitionRecord,
      RestrictionTracker<StreamProgress, StreamProgress> tracker,
      OutputReceiver<KV<ByteString, ChangeStreamRecord>> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator)
      throws InterruptedException, IOException {
    return readChangeStreamPartitionAction.run(
        partitionRecord, tracker, receiver, watermarkEstimator);
  }

  /**
   * Sets the estimator to track throughput for each DoFn instance.
   *
   * @param sizeEstimator an estimator to calculate the size of records for throughput estimates
   */
  public void setSizeEstimator(
      CoderSizeEstimator<KV<ByteString, ChangeStreamRecord>> sizeEstimator) {
    this.sizeEstimator = sizeEstimator;
  }
}
