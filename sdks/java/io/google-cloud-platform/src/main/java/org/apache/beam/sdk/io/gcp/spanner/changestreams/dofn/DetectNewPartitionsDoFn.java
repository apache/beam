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

import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.DetectNewPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.DetectNewPartitionsRangeTracker;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
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
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class DetectNewPartitionsDoFn extends DoFn<PartitionMetadata, PartitionMetadata> {

  private static final long serialVersionUID = 1523712495885011374L;
  private static final Logger LOG = LoggerFactory.getLogger(DetectNewPartitionsDoFn.class);
  private static final Duration DEFAULT_RESUME_DURATION = Duration.millis(100L);

  private final Duration resumeDuration;
  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  private final ActionFactory actionFactory;
  private final ChangeStreamMetrics metrics;
  private long averagePartitionBytesSize;
  private boolean averagePartitionBytesSizeSet;
  private transient DetectNewPartitionsAction detectNewPartitionsAction;

  /**
   * This class needs a {@link DaoFactory} to build DAOs to access the partition metadata tables. It
   * uses mappers to transform database rows into the {@link PartitionMetadata} model. It builds the
   * delegating action class using the {@link ActionFactory}. It emits metrics for the partitions
   * read using the {@link ChangeStreamMetrics}. It re-schedules the process element function to be
   * executed according to the default resume interval as in {@link
   * DetectNewPartitionsDoFn#DEFAULT_RESUME_DURATION} (best effort).
   *
   * @param daoFactory the {@link DaoFactory} to construct {@link PartitionMetadataDao}s
   * @param mapperFactory the {@link MapperFactory} to construct {@link PartitionMetadataMapper}s
   * @param actionFactory the {@link ActionFactory} to construct actions
   * @param metrics the {@link ChangeStreamMetrics} to emit partition related metrics
   */
  public DetectNewPartitionsDoFn(
      DaoFactory daoFactory,
      MapperFactory mapperFactory,
      ActionFactory actionFactory,
      ChangeStreamMetrics metrics) {
    this.daoFactory = daoFactory;
    this.mapperFactory = mapperFactory;
    this.actionFactory = actionFactory;
    this.metrics = metrics;
    this.resumeDuration = DEFAULT_RESUME_DURATION;
    this.averagePartitionBytesSizeSet = false;
  }

  @GetInitialWatermarkEstimatorState
  public Instant getInitialWatermarkEstimatorState(@Element PartitionMetadata partition) {
    return new Instant(partition.getStartTimestamp().toSqlTimestamp());
  }

  @NewWatermarkEstimator
  public ManualWatermarkEstimator<Instant> newWatermarkEstimator(
      @WatermarkEstimatorState Instant watermarkEstimatorState) {
    return new Manual(watermarkEstimatorState);
  }

  /**
   * Uses an {@link TimestampRange} with a max range. This is because it does not know beforehand
   * how many partitions it will schedule.
   *
   * @return the timestamp range for the component
   */
  @GetInitialRestriction
  public TimestampRange initialRestriction(@Element PartitionMetadata partition) {
    final com.google.cloud.Timestamp createdAt = partition.getCreatedAt();
    return TimestampRange.of(
        TimestampUtils.previous(createdAt), com.google.cloud.Timestamp.MAX_VALUE);
  }

  @GetSize
  public double getSize(@Restriction TimestampRange restriction) {
    if (!averagePartitionBytesSizeSet) {
      LOG.warn(
          "Average partition bytes size has not been initialized, GetSize will always return 0, which will interfere with autoscaling.");
    }
    final com.google.cloud.Timestamp readTimestamp = restriction.getFrom();
    final PartitionMetadataDao dao = daoFactory.getPartitionMetadataDao();
    final long partitionsToSchedule = dao.countPartitionsCreatedAfter(readTimestamp);
    final long sizeEstimate = partitionsToSchedule * averagePartitionBytesSize;

    LOG.debug(
        "getSize() = {} ({} partitionsToSchedule * {} averagePartitionBytesSize)",
        sizeEstimate,
        partitionsToSchedule,
        averagePartitionBytesSize);
    return sizeEstimate;
  }

  @NewTracker
  public DetectNewPartitionsRangeTracker newTracker(@Restriction TimestampRange restriction) {
    return new DetectNewPartitionsRangeTracker(restriction);
  }

  /** Obtains the instance of {@link DetectNewPartitionsAction}. */
  @Setup
  public void setup() {
    final PartitionMetadataDao partitionMetadataDao = daoFactory.getPartitionMetadataDao();
    final PartitionMetadataMapper partitionMetadataMapper = mapperFactory.partitionMetadataMapper();
    this.detectNewPartitionsAction =
        actionFactory.detectNewPartitionsAction(
            partitionMetadataDao, partitionMetadataMapper, metrics, resumeDuration);
  }

  /**
   * Main processing function for the {@link DetectNewPartitionsDoFn} function. It will delegate to
   * the {@link DetectNewPartitionsAction} class.
   */
  @ProcessElement
  public ProcessContinuation processElement(
      RestrictionTracker<TimestampRange, com.google.cloud.Timestamp> tracker,
      OutputReceiver<PartitionMetadata> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    return detectNewPartitionsAction.run(tracker, receiver, watermarkEstimator);
  }

  /**
   * Sets the average partition bytes size to estimate the backlog of this DoFn. Must be called
   * after the initialization of this DoFn.
   *
   * @param averagePartitionBytesSize the estimated average size of a partition record used in the
   *     backlog bytes calculation ({@link org.apache.beam.sdk.transforms.DoFn.GetSize})
   */
  public void setAveragePartitionBytesSize(long averagePartitionBytesSize) {
    this.averagePartitionBytesSize = averagePartitionBytesSize;
    this.averagePartitionBytesSizeSet = true;
  }
}
