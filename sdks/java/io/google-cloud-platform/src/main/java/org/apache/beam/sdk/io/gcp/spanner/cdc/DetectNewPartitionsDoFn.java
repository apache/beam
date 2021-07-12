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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.INITIAL_PARTITION_CREATED_TO_SCHEDULED_MS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.PARTITIONS_DETECTED_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.CdcMetrics.PARTITION_CREATED_TO_SCHEDULED_MS;

import com.google.cloud.spanner.ResultSet;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.GrowableOffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.MonotonicallyIncreasing;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
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
  // TODO(hengfeng): Make this field configurable via constructor or spanner config.
  private Duration resumeDuration = Duration.millis(100L);
  private final DaoFactory daoFactory;
  private transient PartitionMetadataDao partitionMetadataDao;

  public DetectNewPartitionsDoFn(DaoFactory daoFactory) {
    this.daoFactory = daoFactory;
  }

  public DetectNewPartitionsDoFn(DaoFactory daoFactory, Duration resumeDuration) {
    this(daoFactory);
    this.resumeDuration = resumeDuration;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Timestamp Instant currentElementTimestamp) {
    return currentElementTimestamp;
  }

  @NewWatermarkEstimator
  public WatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new MonotonicallyIncreasing(watermarkEstimatorState);
  }

  @GetInitialRestriction
  public OffsetRange initialRestriction(@Element ChangeStreamSourceDescriptor inputElement) {
    return new OffsetRange(0, Long.MAX_VALUE);
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
  }

  @ProcessElement
  public ProcessContinuation processElement(
      @Element ChangeStreamSourceDescriptor inputElement,
      RestrictionTracker<OffsetRange, Long> tracker,
      WatermarkEstimator watermarkEstimator,
      OutputReceiver<PartitionMetadata> receiver) {
    Instant start = Instant.now();
    LOG.debug("Calling process element:" + start);

    try (ResultSet resultSet = partitionMetadataDao.getPartitionsInState(State.CREATED)) {
      long currentIndex = tracker.currentRestriction().getFrom();

      // Output the records.
      while (resultSet.next()) {
        // TODO(hengfeng): change the log level in this file.
        LOG.debug("Reading record currentIndex:" + currentIndex);
        PARTITIONS_DETECTED_COUNTER.inc();

        if (!tracker.tryClaim(currentIndex)) {
          return ProcessContinuation.stop();
        }
        PartitionMetadata metadata = buildPartitionMetadata(resultSet);
        if (InitialPartition.isInitialPartition(metadata.getPartitionToken())) {
          INITIAL_PARTITION_CREATED_TO_SCHEDULED_MS.update(
              new Duration(metadata.getCreatedAt().toDate().getTime(), Instant.now().getMillis())
                  .getMillis());
        } else {
          PARTITION_CREATED_TO_SCHEDULED_MS.update(
              new Duration(metadata.getCreatedAt().toDate().getTime(), Instant.now().getMillis())
                  .getMillis());
        }
        LOG.debug(
            String.format(
                "Get partition metadata currentIndex:%d meta:%s", currentIndex, metadata));

        currentIndex++;

        Instant now = Instant.now();
        LOG.debug("Read watermark:" + watermarkEstimator.currentWatermark() + " now:" + now);
        LOG.info("Scheduling partition: " + metadata);
        receiver.output(metadata);

        partitionMetadataDao.updateState(metadata.getPartitionToken(), State.SCHEDULED);
        LOG.debug("Updated the record:" + metadata.getPartitionToken());
      }
    }

    // If there are no partitions in the table, we should stop this SDF function.
    long numOfPartitions = partitionMetadataDao.countPartitions();
    if (numOfPartitions == 0) {
      if (!tracker.tryClaim(tracker.currentRestriction().getTo() - 1)) {
        LOG.warn("Failed to claim the end of range in DetectNewPartitionsDoFn.");
      }
      return ProcessContinuation.stop();
    }
    return ProcessContinuation.resume().withResumeDelay(resumeDuration);
  }

  private PartitionMetadata buildPartitionMetadata(ResultSet resultSet) {
    return new PartitionMetadata(
        resultSet.getString(PartitionMetadataDao.COLUMN_PARTITION_TOKEN),
        Sets.newHashSet(resultSet.getStringList(PartitionMetadataDao.COLUMN_PARENT_TOKENS)),
        resultSet.getTimestamp(PartitionMetadataDao.COLUMN_START_TIMESTAMP),
        resultSet.getBoolean(PartitionMetadataDao.COLUMN_INCLUSIVE_START),
        !resultSet.isNull(PartitionMetadataDao.COLUMN_END_TIMESTAMP)
            ? resultSet.getTimestamp(PartitionMetadataDao.COLUMN_END_TIMESTAMP)
            : null,
        resultSet.getBoolean(PartitionMetadataDao.COLUMN_INCLUSIVE_END),
        resultSet.getLong(PartitionMetadataDao.COLUMN_HEARTBEAT_MILLIS),
        PartitionMetadata.State.valueOf(resultSet.getString(PartitionMetadataDao.COLUMN_STATE)),
        resultSet.getTimestamp(PartitionMetadataDao.COLUMN_CREATED_AT),
        resultSet.getTimestamp(PartitionMetadataDao.COLUMN_UPDATED_AT));
  }
}
