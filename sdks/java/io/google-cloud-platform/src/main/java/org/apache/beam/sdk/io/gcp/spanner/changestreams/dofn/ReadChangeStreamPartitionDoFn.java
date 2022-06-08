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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.UPDATE_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;

import java.io.Serializable;
import java.math.BigDecimal;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.DataChangeRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.DoneAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.InitialChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.QueryChangeStreamAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.UpdateStateAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.action.WaitForChildPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestriction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestrictionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestrictionTracker;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.ReadChangeStreamPartitionRangeTracker;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.ThroughputEstimator;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
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
  private static final double AUTOSCALING_SIZE_MULTIPLIER = 2.0D;

  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  private final ActionFactory actionFactory;
  private final ChangeStreamMetrics metrics;
  private final ThroughputEstimator throughputEstimator;

  private transient QueryChangeStreamAction queryChangeStreamAction;
  private transient WaitForChildPartitionsAction waitForChildPartitionsAction;
  private transient UpdateStateAction updateStateAction;
  private transient DoneAction doneAction;

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
   * @param throughputEstimator an estimator to calculate local throughput.
   */
  public ReadChangeStreamPartitionDoFn(
      DaoFactory daoFactory,
      MapperFactory mapperFactory,
      ActionFactory actionFactory,
      ChangeStreamMetrics metrics,
      ThroughputEstimator throughputEstimator) {
    this.daoFactory = daoFactory;
    this.mapperFactory = mapperFactory;
    this.actionFactory = actionFactory;
    this.metrics = metrics;
    this.throughputEstimator = throughputEstimator;
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
  public PartitionRestriction initialRestriction(@Element PartitionMetadata partition) {
    final String token = partition.getPartitionToken();
    final com.google.cloud.Timestamp startTimestamp = partition.getStartTimestamp();
    // Range represents closed-open interval
    final com.google.cloud.Timestamp endTimestamp =
        TimestampUtils.next(partition.getEndTimestamp());

    metrics.incActivePartitionReadCounter();
    final PartitionRestriction restriction =
        PartitionRestriction.updateState(startTimestamp, endTimestamp);
    final PartitionRestriction restrictionWithMetadata =
        restriction.withMetadata(
            PartitionRestrictionMetadata.newBuilder()
                .withPartitionToken(token)
                .withPartitionStartTimestamp(partition.getStartTimestamp())
                .withPartitionEndTimestamp(partition.getEndTimestamp())
                .build());
    return restrictionWithMetadata;
  }

  @GetSize
  public double getSize(
      @Element PartitionMetadata partition, @Restriction PartitionRestriction range)
      throws Exception {
    final BigDecimal timeGapInSeconds =
        BigDecimal.valueOf(newTracker(partition, range).getProgress().getWorkRemaining());
    final BigDecimal throughput = BigDecimal.valueOf(this.throughputEstimator.get());
    LOG.debug(
        "Reported getSize() - remaining work: " + timeGapInSeconds + " throughput:" + throughput);
    // Cap it at Double.MAX_VALUE to avoid an overflow.
    return timeGapInSeconds
        .multiply(throughput)
        // The multiplier is required because the job tries to reach the minimum number of workers
        // and this leads to a very high cpu utilization. The multiplier would increase the reported
        // size and help to reduce the cpu usage. In the future, this can become a custom parameter.
        .multiply(BigDecimal.valueOf(AUTOSCALING_SIZE_MULTIPLIER))
        .min(BigDecimal.valueOf(Double.MAX_VALUE))
        .doubleValue();
  }

  @NewTracker
  public PartitionRestrictionTracker newTracker(
      @Element PartitionMetadata partition, @Restriction PartitionRestriction restriction) {
    return new PartitionRestrictionTracker(restriction);
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
    final DataChangeRecordAction dataChangeRecordAction = actionFactory.dataChangeRecordAction();
    final HeartbeatRecordAction heartbeatRecordAction =
        actionFactory.heartbeatRecordAction(metrics);
    final ChildPartitionsRecordAction childPartitionsRecordAction =
        actionFactory.childPartitionsRecordAction(partitionMetadataDao, metrics);
    final InitialChildPartitionsRecordAction initialChildPartitionsRecordAction =
        actionFactory.initialChildPartitionsRecordAction(partitionMetadataDao);

    this.queryChangeStreamAction =
        actionFactory.queryChangeStreamAction(
            changeStreamDao,
            partitionMetadataDao,
            changeStreamRecordMapper,
            partitionMetadataMapper,
            dataChangeRecordAction,
            heartbeatRecordAction,
            childPartitionsRecordAction,
            initialChildPartitionsRecordAction,
            metrics,
            throughputEstimator);
    this.updateStateAction = actionFactory.updateStateAction(partitionMetadataDao, metrics);
    this.waitForChildPartitionsAction =
        actionFactory.waitForChildPartitionsAction(partitionMetadataDao);
    this.doneAction = actionFactory.doneAction();
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
   * @return a {@link ProcessContinuation#stop()} if a record timestamp could not be claimed or if
   *     the partition processing has finished
   */
  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    final String token = partition.getPartitionToken();

    LOG.debug(
        "[" + token + "] Processing element with restriction " + tracker.currentRestriction());

    final PartitionMode mode = tracker.currentRestriction().getMode();
    switch (mode) {
      case UPDATE_STATE:
        // First update the state, and then transition to querying change streams.
        return updateState(partition, tracker, receiver, watermarkEstimator);
      case QUERY_CHANGE_STREAM:
        // After querying change streams, transition to waiting for child partitions.
        return queryChangeStream(partition, tracker, receiver, watermarkEstimator);
      case WAIT_FOR_CHILD_PARTITIONS:
        // After waiting for child partitions, finish the SDF.
        return waitForChildPartitions(partition, tracker);
      case DONE:
        return done(partition, tracker);
      default:
        // TODO: Verify what to do here
        LOG.error("[" + token + "] Unknown mode " + mode);
        throw new IllegalArgumentException("Unknown mode " + mode);
    }
  }

  private ProcessContinuation updateState(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    return updateStateAction
        .run(partition, tracker)
        .orElseGet(() -> queryChangeStream(partition, tracker, receiver, watermarkEstimator));
  }

  private ProcessContinuation queryChangeStream(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    return queryChangeStreamAction
        .run(partition, tracker, receiver, watermarkEstimator)
        .orElseGet(() -> waitForChildPartitions(partition, tracker));
  }

  private ProcessContinuation waitForChildPartitions(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return waitForChildPartitionsAction
        .run(partition, tracker)
        .orElseGet(() -> done(partition, tracker));
  }

  private ProcessContinuation done(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {

    return doneAction.run(partition, tracker);
  }
}
