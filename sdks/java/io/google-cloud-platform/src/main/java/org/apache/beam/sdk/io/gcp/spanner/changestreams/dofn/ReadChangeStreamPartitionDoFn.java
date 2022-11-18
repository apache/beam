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

import java.io.Serializable;
import java.math.BigDecimal;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.DataChangeRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.QueryChangeStreamAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.BytesThroughputEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.NullThroughputEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.estimator.ThroughputEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.ReadChangeStreamPartitionRangeTracker;
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
 * A SDF (Splittable DoFn) class which is responsible for performing a change stream query for a
 * given partition. A different action will be taken depending on the type of record received from
 * the query. This component will also reflect the partition state in the partition metadata tables.
 *
 * <p>The processing of a partition is delegated to the {@link QueryChangeStreamAction}.
 */
// Allows for transient QueryChangeStreamAction
@SuppressWarnings("initialization.fields.uninitialized")
@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn extends DoFn<PartitionMetadata, DataChangeRecord>
    implements Serializable {

  private static final long serialVersionUID = -7574596218085711975L;
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);
  private static final BigDecimal MAX_DOUBLE = BigDecimal.valueOf(Double.MAX_VALUE);

  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  private final ActionFactory actionFactory;
  private final ChangeStreamMetrics metrics;
  /**
   * Needs to be set through the {@link
   * ReadChangeStreamPartitionDoFn#setThroughputEstimator(BytesThroughputEstimator)} call.
   */
  private ThroughputEstimator<DataChangeRecord> throughputEstimator;

  private transient QueryChangeStreamAction queryChangeStreamAction;

  /**
   * This class needs a {@link DaoFactory} to build DAOs to access the partition metadata tables and
   * to perform the change streams query. It uses mappers to transform database rows into the {@link
   * org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord} model. It uses the
   * {@link ActionFactory} to construct the action dispatchers, which will perform the change stream
   * query and process each type of record received. It emits metrics for the partition using the
   * {@link ChangeStreamMetrics}.
   *
   * @param daoFactory the {@link DaoFactory} to construct {@link PartitionMetadataDao}s and {@link
   *     ChangeStreamDao}s
   * @param mapperFactory the {@link MapperFactory} to construct {@link ChangeStreamRecordMapper}s
   * @param actionFactory the {@link ActionFactory} to construct actions
   * @param metrics the {@link ChangeStreamMetrics} to emit partition related metrics
   */
  public ReadChangeStreamPartitionDoFn(
      DaoFactory daoFactory,
      MapperFactory mapperFactory,
      ActionFactory actionFactory,
      ChangeStreamMetrics metrics) {
    this.daoFactory = daoFactory;
    this.mapperFactory = mapperFactory;
    this.actionFactory = actionFactory;
    this.metrics = metrics;
    this.throughputEstimator = new NullThroughputEstimator<>();
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
   * The restriction for a partition will be defined from the start and end timestamp to query the
   * partition for. The {@link TimestampRange} restriction represents a closed-open interval, while
   * the start / end timestamps represent a closed-closed interval, so we add 1 nanosecond to the
   * end timestamp to convert it to closed-open.
   *
   * <p>In this function we also update the partition state to {@link
   * PartitionMetadata.State#RUNNING}.
   *
   * @param partition the partition to be queried
   * @return the timestamp range from the partition start timestamp to the partition end timestamp +
   *     1 nanosecond
   */
  @GetInitialRestriction
  public TimestampRange initialRestriction(@Element PartitionMetadata partition) {
    final String token = partition.getPartitionToken();
    final com.google.cloud.Timestamp startTimestamp = partition.getStartTimestamp();
    // Range represents closed-open interval
    final com.google.cloud.Timestamp endTimestamp =
        TimestampUtils.next(partition.getEndTimestamp());
    final com.google.cloud.Timestamp partitionScheduledAt = partition.getScheduledAt();
    final com.google.cloud.Timestamp partitionRunningAt =
        daoFactory.getPartitionMetadataDao().updateToRunning(token);

    if (partitionScheduledAt != null && partitionRunningAt != null) {
      metrics.updatePartitionScheduledToRunning(
          new Duration(
              partitionScheduledAt.toSqlTimestamp().getTime(),
              partitionRunningAt.toSqlTimestamp().getTime()));
    }

    metrics.incActivePartitionReadCounter();
    return TimestampRange.of(startTimestamp, endTimestamp);
  }

  @GetSize
  public double getSize(@Element PartitionMetadata partition, @Restriction TimestampRange range)
      throws Exception {
    final BigDecimal timeGapInSeconds =
        BigDecimal.valueOf(newTracker(partition, range).getProgress().getWorkRemaining());
    final BigDecimal throughput = BigDecimal.valueOf(this.throughputEstimator.get());
    final double size =
        timeGapInSeconds
            .multiply(throughput)
            // Cap it at Double.MAX_VALUE to avoid an overflow.
            .min(MAX_DOUBLE)
            .doubleValue();
    LOG.debug(
        "getSize() = {} ({} timeGapInSeconds * {} throughput)", size, timeGapInSeconds, throughput);
    return size;
  }

  @NewTracker
  public ReadChangeStreamPartitionRangeTracker newTracker(
      @Element PartitionMetadata partition, @Restriction TimestampRange range) {
    return new ReadChangeStreamPartitionRangeTracker(partition, range);
  }

  /**
   * Constructs instances for the {@link PartitionMetadataDao}, {@link ChangeStreamDao}, {@link
   * ChangeStreamRecordMapper}, {@link PartitionMetadataMapper}, {@link DataChangeRecordAction},
   * {@link HeartbeatRecordAction}, {@link ChildPartitionsRecordAction} and {@link
   * QueryChangeStreamAction}.
   */
  @Setup
  public void setup() {
    final PartitionMetadataDao partitionMetadataDao = daoFactory.getPartitionMetadataDao();
    final ChangeStreamDao changeStreamDao = daoFactory.getChangeStreamDao();
    final ChangeStreamRecordMapper changeStreamRecordMapper =
        mapperFactory.changeStreamRecordMapper();
    final PartitionMetadataMapper partitionMetadataMapper = mapperFactory.partitionMetadataMapper();
    final DataChangeRecordAction dataChangeRecordAction =
        actionFactory.dataChangeRecordAction(throughputEstimator);
    final HeartbeatRecordAction heartbeatRecordAction =
        actionFactory.heartbeatRecordAction(metrics);
    final ChildPartitionsRecordAction childPartitionsRecordAction =
        actionFactory.childPartitionsRecordAction(partitionMetadataDao, metrics);

    this.queryChangeStreamAction =
        actionFactory.queryChangeStreamAction(
            changeStreamDao,
            partitionMetadataDao,
            changeStreamRecordMapper,
            partitionMetadataMapper,
            dataChangeRecordAction,
            heartbeatRecordAction,
            childPartitionsRecordAction,
            metrics);
  }

  /**
   * Performs a change stream query for a given partition. A different action will be taken
   * depending on the type of record received from the query. This component will also reflect the
   * partition state in the partition metadata tables.
   *
   * <p>The processing of a partition is delegated to the {@link QueryChangeStreamAction}.
   *
   * @param partition the partition to be queried
   * @param tracker an instance of {@link ReadChangeStreamPartitionRangeTracker}
   * @param receiver a {@link DataChangeRecord} {@link OutputReceiver}
   * @param watermarkEstimator a {@link ManualWatermarkEstimator} of {@link Instant}
   * @param bundleFinalizer the bundle finalizer
   * @return a {@link ProcessContinuation#stop()} if a record timestamp could not be claimed or if
   *     the partition processing has finished
   */
  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata partition,
      RestrictionTracker<TimestampRange, com.google.cloud.Timestamp> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      BundleFinalizer bundleFinalizer) {

    final String token = partition.getPartitionToken();

    LOG.debug("[{}] Processing element with restriction {}", token, tracker.currentRestriction());

    return queryChangeStreamAction.run(
        partition, tracker, receiver, watermarkEstimator, bundleFinalizer);
  }

  /**
   * Sets the estimator to calculate the backlog of this function. Must be called after the
   * initialization of this DoFn.
   *
   * @param throughputEstimator an estimator to calculate local throughput.
   */
  public void setThroughputEstimator(
      BytesThroughputEstimator<DataChangeRecord> throughputEstimator) {
    this.throughputEstimator = throughputEstimator;
  }
}
