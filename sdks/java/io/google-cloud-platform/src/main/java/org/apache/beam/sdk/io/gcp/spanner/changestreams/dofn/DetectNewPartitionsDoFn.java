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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import com.google.cloud.spanner.ResultSet;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A SplittableDoFn (SDF) that is responsible for scheduling partitions to be queried. This
 * component will periodically scan the partition metadata table looking for partitions in the
 * {@link State#CREATED}, update their state to {@link State#SCHEDULED} and output them to the next
 * stage in the pipeline.
 */
@UnboundedPerElement
@SuppressWarnings({
  "rawtypes", // TODO(https://issues.apache.org/jira/browse/BEAM-10556)
  "nullness" // TODO(https://issues.apache.org/jira/browse/BEAM-10402)
})
public class DetectNewPartitionsDoFn extends DoFn<byte[], PartitionMetadata> {

  private static final long serialVersionUID = 1523712495885011374L;
  private static final Duration DEFAULT_RESUME_DURATION = Duration.millis(100L);
  private static final Logger LOG = LoggerFactory.getLogger(DetectNewPartitionsDoFn.class);
  private static final Tracer TRACER = Tracing.getTracer();

  // TODO(hengfeng): Make this field configurable via constructor or spanner config.
  private final Duration resumeDuration;
  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  private final ChangeStreamMetrics metrics;
  private transient PartitionMetadataDao partitionMetadataDao;
  private transient PartitionMetadataMapper partitionMetadataMapper;

  /**
   * This class needs a {@link DaoFactory} to build DAOs to access the partition metadata tables. It
   * uses mappers to transform database rows into the {@link PartitionMetadata} model. It emits
   * metrics for the partitions read using the {@link ChangeStreamMetrics}. This constructors sets
   * the the periodic re-execution of the component to be scheduled using the {@link
   * DetectNewPartitionsDoFn#DEFAULT_RESUME_DURATION} duration (best effort).
   *
   * @param daoFactory the {@link DaoFactory} to construct {@link PartitionMetadataDao}s
   * @param mapperFactory the {@link MapperFactory} to construct {@link PartitionMetadataMapper}s
   * @param metrics the {@link ChangeStreamMetrics} to emit partition related metrics
   */
  public DetectNewPartitionsDoFn(
      DaoFactory daoFactory, MapperFactory mapperFactory, ChangeStreamMetrics metrics) {
    this(daoFactory, mapperFactory, metrics, DEFAULT_RESUME_DURATION);
  }

  /**
   * This class needs a {@link DaoFactory} to build DAOs to access the partition metadata tables. It
   * uses mappers to transform database rows into the {@link PartitionMetadata} model. It emits
   * metrics for the partitions read using the {@link ChangeStreamMetrics}. It re-schedules the
   * process element function to be executed according to the specified duration (best effort).
   *
   * @param daoFactory the {@link DaoFactory} to construct {@link PartitionMetadataDao}s
   * @param mapperFactory the {@link MapperFactory} to construct {@link PartitionMetadataMapper}s
   * @param metrics the {@link ChangeStreamMetrics} to emit partition related metrics
   * @param resumeDuration specifies the periodic schedule to re-execute this component
   */
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

  /**
   * Uses an {@link OffsetRange} with a max range. This is because it does not know before hand how
   * many partitions it will schedule.
   *
   * <p>In order to circumvent a bug in Apache Beam
   * (https://issues.apache.org/jira/browse/BEAM-12756) we don't use {@link Long#MAX_VALUE}, but
   * rather a value slightly smaller.
   *
   * @return the offset range for the component
   */
  @GetInitialRestriction
  public OffsetRange initialRestriction() {
    // TODO: Update this after https://issues.apache.org/jira/browse/BEAM-12756 is fixed.
    return new OffsetRange(0, Long.MAX_VALUE - 10_000L);
  }

  @NewTracker
  public OffsetRangeTracker restrictionTracker(@Restriction OffsetRange restriction) {
    return new OffsetRangeTracker(restriction);
  }

  /**
   * Obtains the instances of {@link PartitionMetadataDao} and {@link PartitionMetadataMapper} from
   * their respective factories.
   */
  @Setup
  public void setup() {
    this.partitionMetadataDao = daoFactory.getPartitionMetadataDao();
    this.partitionMetadataMapper = mapperFactory.partitionMetadataMapper();
  }

  /**
   * Main processing function for the {@link DetectNewPartitionsDoFn} function. It follows this
   * procedure periodically:
   *
   * <ol>
   *   <li>Fetches the min watermark from all the unfinished partitions in the metadata tables.
   *   <li>Updates the component's watermark to the min fetched.
   *   <li>Fetches all the partitions that are in {@link State#CREATED}.
   *   <li>Updates the state of the partitions to {@link State#SCHEDULED}.
   *   <li>Outputs the partitions to the next stage in the pipeline.
   *   <li>Schedule the function to resume after the configuration resume duration.
   * </ol>
   *
   * In the beginning of this function if there are no more unfinished partitions, it indicates that
   * the work is complete. Thus, this function will not be re-scheduled.
   *
   * @param tracker an instance of {@link OffsetRangeTracker}
   * @param receiver a {@link PartitionMetadata} {@link
   *     OutputReceiver}
   * @param watermarkEstimator a {@link ManualWatermarkEstimator} of {@link Instant}
   * @return a {@link ProcessContinuation#stop()} if there are no more partitions to process or
   *     {@link ProcessContinuation#resume()} to re-schedule the function after the configured
   *     interval.
   */
  @ProcessElement
  public ProcessContinuation processElement(
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<PartitionMetadata> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    try (Scope scope =
        TRACER
            .spanBuilder("DetectNewPartitionsDoFn.processElement")
            .setRecordEvents(true)
            .startScopedSpan()) {

      // Updates the current watermark as the min of the watermarks from all existing partitions
      final com.google.cloud.Timestamp minWatermark = getUnfinishedMinWatermark();
      if (minWatermark != null) {
        final Instant minWatermarkInstant = new Instant(minWatermark.toSqlTimestamp());
        watermarkEstimator.setWatermark(minWatermarkInstant);

        try (ResultSet resultSet = partitionMetadataDao.getPartitionsInState(State.CREATED)) {
          long currentIndex = tracker.currentRestriction().getFrom();

          while (resultSet.next()) {
            if (!tracker.tryClaim(currentIndex)) {
              LOG.debug("Could not claim " + currentIndex + ", stopping...");
              return ProcessContinuation.stop();
            }

            final PartitionMetadata partition =
                partitionMetadataMapper.from(resultSet.getCurrentRowAsStruct());
            final com.google.cloud.Timestamp scheduledAt = schedulePartition(partition);
            final PartitionMetadata updatedPartition =
                partition.toBuilder().setScheduledAt(scheduledAt).build();

            receiver.outputWithTimestamp(updatedPartition, minWatermarkInstant);

            metrics.incPartitionRecordCount();
            currentIndex++;
          }
        }
        return ProcessContinuation.resume().withResumeDelay(resumeDuration);
      } else {
        if (!tracker.tryClaim(tracker.currentRestriction().getTo())) {
          LOG.warn("Failed to claim the end of range in DetectNewPartitionsDoFn.");
        }
        LOG.info("All partitions have been processed, stopping");
        return ProcessContinuation.stop();
      }
    }
  }

  private com.google.cloud.Timestamp getUnfinishedMinWatermark() {
    try (Scope scope =
        TRACER
            .spanBuilder("DetectNewPartitionsDoFn.getUnfinishedMinWatermark")
            .setRecordEvents(true)
            .startScopedSpan()) {
      return partitionMetadataDao.getUnfinishedMinWatermark();
    }
  }

  private com.google.cloud.Timestamp schedulePartition(PartitionMetadata partition) {
    final String token = partition.getPartitionToken();
    try (Scope scope =
        TRACER
            .spanBuilder("DetectNewPartitionsDoFn.getUnfinishedMinWatermark")
            .setRecordEvents(true)
            .startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(token));

      final com.google.cloud.Timestamp createdAt = partition.getCreatedAt();
      LOG.debug("[" + token + "] Scheduling partition");
      final com.google.cloud.Timestamp scheduledAt = partitionMetadataDao.updateToScheduled(token);
      LOG.info(
          "["
              + token
              + "] Scheduled partition at "
              + scheduledAt
              + " with start time "
              + partition.getStartTimestamp()
              + " and end time "
              + partition.getEndTimestamp());
      metrics.updatePartitionCreatedToScheduled(
          new Duration(createdAt.toDate().getTime(), scheduledAt.toSqlTimestamp().getTime()));

      return scheduledAt;
    }
  }
}
