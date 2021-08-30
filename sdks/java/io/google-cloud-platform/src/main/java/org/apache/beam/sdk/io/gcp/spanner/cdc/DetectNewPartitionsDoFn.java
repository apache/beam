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
package org.apache.beam.sdk.io.gcp.spanner.cdc;

import com.google.cloud.spanner.ResultSet;
import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SplittableDoFn which reads from {@link ChangeStreamSourceDescriptor} and outputs {@link
 * PartitionMetadata}.
 *
 * <p>{@link DetectNewPartitionsDoFn} implements the logic of querying the partition metadata table
 * from Cloud Spanner. The element is a {@link ChangeStreamSourceDescriptor}, and the restriction is
 * an {@link OffsetRange} which represents record offset. A {@link GrowableOffsetRangeTracker} is
 * used to track an {@link OffsetRange} ended with {@code Long.MAX_VALUE}. For a finite range, a
 * {@link OffsetRangeTracker} is created.
 */
@UnboundedPerElement
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class DetectNewPartitionsDoFn extends DoFn<ChangeStreamSourceDescriptor, PartitionMetadata> {

  private static final Logger LOG = LoggerFactory.getLogger(DetectNewPartitionsDoFn.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private static final long serialVersionUID = 1523712495885011374L;
  // TODO(hengfeng): Make this field configurable via constructor or spanner config.
  private final Duration resumeDuration;
  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  private final ChangeStreamMetrics metrics;
  private transient PartitionMetadataDao partitionMetadataDao;
  private transient PartitionMetadataMapper partitionMetadataMapper;

  public DetectNewPartitionsDoFn(
      DaoFactory daoFactory, MapperFactory mapperFactory, ChangeStreamMetrics metrics) {
    this(daoFactory, mapperFactory, metrics, Duration.millis(100L));
  }

  public DetectNewPartitionsDoFn(
      DaoFactory daoFactory,
      MapperFactory mapperFactory,
      ChangeStreamMetrics metrics,
      Duration resumeDuration) {
    this.daoFactory = daoFactory;
    this.mapperFactory = mapperFactory;
    this.metrics = metrics;
    this.resumeDuration = resumeDuration;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new Manual(watermarkEstimatorState);
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element ChangeStreamSourceDescriptor inputElement) {
    // TODO: Update this after https://issues.apache.org/jira/browse/BEAM-12756 is fixed.
    return new OffsetRange(0, Long.MAX_VALUE - 10000);
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(
      @Element ChangeStreamSourceDescriptor inputElement, @Restriction OffsetRange restriction) {
    // FIXME: The end of the range should probably be Long.MAX_VALUE
    return new OffsetRangeTracker(new OffsetRange(restriction.getFrom(), restriction.getTo()));
  }

  @Setup
  public void setup() {
    this.partitionMetadataDao = daoFactory.getPartitionMetadataDao();
    this.partitionMetadataMapper = mapperFactory.partitionMetadataMapper();
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element ChangeStreamSourceDescriptor desc,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<PartitionMetadata> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    try (Scope scope =
        TRACER
            .spanBuilder("DetectNewPartitionsDoFn.processElement")
            .setRecordEvents(true)
            .startScopedSpan()) {

      // Updates the current watermark as the min of the watermarks from all existing partitions
      final Instant minCurrentWatermark = getMinWatermark();
      if (minCurrentWatermark != null) {
        watermarkEstimator.setWatermark(minCurrentWatermark);
      }

      try (ResultSet resultSet = partitionMetadataDao.getPartitionsInState(State.CREATED)) {
        long currentIndex = tracker.currentRestriction().getFrom();

        while (resultSet.next()) {
          if (!tracker.tryClaim(currentIndex)) {
            return ProcessContinuation.stop();
          }

          final PartitionMetadata partition = partitionMetadataMapper.from(resultSet);
          final PartitionMetadata updatedPartition = updateToScheduled(partition);
          receiver.output(updatedPartition);

          metrics.incPartitionRecordCount();
          currentIndex++;
        }
      }

      // If there are no partitions in the table, we should stop this SDF function.
      final long numberOfPartitions = countPartitions();
      if (numberOfPartitions == 0) {
        if (!tracker.tryClaim(tracker.currentRestriction().getTo() - 1)) {
          LOG.warn("Failed to claim the end of range in DetectNewPartitionsDoFn.");
        }
        LOG.info("All partitions have been processed, stopping");
        return ProcessContinuation.stop();
      }
      return ProcessContinuation.resume().withResumeDelay(resumeDuration);
    }
  }

  private PartitionMetadata updateToScheduled(PartitionMetadata partition) {
    final String token = partition.getPartitionToken();
    LOG.info("Scheduling partition " + partition.getPartitionToken());
    final com.google.cloud.Timestamp createdAt = partition.getCreatedAt();
    final com.google.cloud.Timestamp scheduledAt = partitionMetadataDao.updateToScheduled(token);

    metrics.updatePartitionCreatedToScheduled(
        new Duration(createdAt.toDate().getTime(), scheduledAt.toSqlTimestamp().getTime()));
    LOG.debug("Updated the record:" + token);
    return partition.toBuilder().setScheduledAt(scheduledAt).build();
  }

  private long countPartitions() {
    final com.google.cloud.Timestamp countPartitionsStartedAt = com.google.cloud.Timestamp.now();
    final long numberOfPartitions = partitionMetadataDao.countPartitions();
    final com.google.cloud.Timestamp countPartitionsEndedAt = com.google.cloud.Timestamp.now();
    metrics.updateDaoCountPartitions(
        new Duration(
            countPartitionsStartedAt.toSqlTimestamp().getTime(),
            countPartitionsEndedAt.toSqlTimestamp().getTime()));

    return numberOfPartitions;
  }

  private Instant getMinWatermark() {
    final com.google.cloud.Timestamp minWatermarkStartedAt = com.google.cloud.Timestamp.now();
    final Instant minCurrentWatermark =
        Optional.ofNullable(partitionMetadataDao.getMinCurrentWatermark())
            .map(timestamp -> new Instant(timestamp.toSqlTimestamp().getTime()))
            .orElse(null);
    final com.google.cloud.Timestamp minWatermarkEndedAt = com.google.cloud.Timestamp.now();
    metrics.updateDaoGetMinCurrentWatermark(
        new Duration(
            minWatermarkStartedAt.toSqlTimestamp().getTime(),
            minWatermarkEndedAt.toSqlTimestamp().getTime()));

    return minCurrentWatermark;
  }
}
