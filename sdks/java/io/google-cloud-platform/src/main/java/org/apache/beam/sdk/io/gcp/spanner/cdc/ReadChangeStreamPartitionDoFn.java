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
import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ActionFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.ChildPartitionsRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DataChangeRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DeletePartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.DonePartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.FinishPartitionAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.HeartbeatRecordAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForChildPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.actions.WaitForParentPartitionsAction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.DaoFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.cdc.mapper.MapperFactory;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestrictionTracker;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators.Manual;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

// TODO: Add java docs
@UnboundedPerElement
public class ReadChangeStreamPartitionDoFn extends DoFn<PartitionMetadata, DataChangeRecord>
    implements Serializable {

  private static final long serialVersionUID = -7574596218085711975L;
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionDoFn.class);

  private final SpannerConfig spannerConfig;
  private final DaoFactory daoFactory;
  private final MapperFactory mapperFactory;
  private final ActionFactory actionFactory;
  private transient ChangeStreamRecordMapper changeStreamRecordMapper;
  private transient ChangeStreamDao changeStreamDao;

  private transient WaitForChildPartitionsAction waitForChildPartitionsAction;
  private transient FinishPartitionAction finishPartitionAction;
  private transient WaitForParentPartitionsAction waitForParentPartitionsAction;
  private transient DeletePartitionAction deletePartitionAction;
  private transient DataChangeRecordAction dataChangeRecordAction;
  private transient HeartbeatRecordAction heartbeatRecordAction;
  private transient ChildPartitionsRecordAction childPartitionsRecordAction;
  private transient DonePartitionAction donePartitionAction;

  public ReadChangeStreamPartitionDoFn(
      SpannerConfig spannerConfig,
      DaoFactory daoFactory,
      MapperFactory mapperFactory,
      ActionFactory actionFactory) {
    this.spannerConfig = spannerConfig;
    this.daoFactory = daoFactory;
    this.mapperFactory = mapperFactory;
    this.actionFactory = actionFactory;
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
  public PartitionRestriction initialRestriction(@Element PartitionMetadata element) {
    return new PartitionRestriction(
        element.getStartTimestamp(),
        element.getEndTimestamp(),
        PartitionMode.QUERY_CHANGE_STREAM,
        null);
  }

  @NewTracker
  public PartitionRestrictionTracker newTracker(@Restriction PartitionRestriction restriction) {
    return new PartitionRestrictionTracker(restriction);
  }

  @Setup
  public void setup() {
    final PartitionMetadataDao partitionMetadataDao =
        daoFactory.partitionMetadataDaoFrom(spannerConfig);
    this.changeStreamDao = daoFactory.changeStreamDaoFrom(spannerConfig);
    this.changeStreamRecordMapper = mapperFactory.changeStreamRecordMapper();

    this.waitForChildPartitionsAction =
        actionFactory.waitForChildPartitionsAction(partitionMetadataDao, Duration.millis(100));
    this.finishPartitionAction = actionFactory.finishPartitionAction(partitionMetadataDao);
    this.waitForParentPartitionsAction =
        actionFactory.waitForParentPartitionsAction(partitionMetadataDao, Duration.millis(100));
    this.deletePartitionAction = actionFactory.deletePartitionAction(partitionMetadataDao);
    this.donePartitionAction = actionFactory.donePartitionAction();

    this.dataChangeRecordAction = actionFactory.dataChangeRecordAction();
    this.heartbeatRecordAction = actionFactory.heartbeatRecordAction();
    this.childPartitionsRecordAction =
        actionFactory.childPartitionsRecordAction(
            partitionMetadataDao, waitForChildPartitionsAction);
  }

  // TODO: Close DAOs on teardown

  @ProcessElement
  public ProcessContinuation processElement(
      @Element PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    MDC.put("partitionToken", partition.getPartitionToken());
    LOG.info(
        "Processing element "
            + partition.getPartitionToken()
            + " with restriction "
            + tracker.currentRestriction());

    switch (tracker.currentRestriction().getMode()) {
      case QUERY_CHANGE_STREAM:
        return queryChangeStream(partition, tracker, receiver, watermarkEstimator);
      case WAIT_FOR_CHILD_PARTITIONS:
        return waitForChildPartitions(partition, tracker);
      case FINISH_PARTITION:
        return finishPartition(partition, tracker);
      case WAIT_FOR_PARENT_PARTITIONS:
        return waitForParentPartitions(partition, tracker);
      case DELETE_PARTITION:
        return deletePartition(partition, tracker);
      case DONE:
        return done(partition, tracker);
      default:
        // TODO: Verify what to do here
        throw new IllegalArgumentException(
            "Unknown mode " + tracker.currentRestriction().getMode());
    }
  }

  // spotless:off
  /**
   * The following graph represents the state machine that will be followed by this function.
   * The states will be stored in the {@link PartitionRestriction} and claimed through the
   * {@link PartitionPosition}.
   *
   *                                               HEARTBEAT RECORD
   *                                             DATA CHANGE RECORD
   *                                         |---------------------|
   *                                         v                     |
   *                              +---------------------+          |
   *                              | QUERY_CHANGE_STREAM |----------|
   *                              +---------------------+
   *                                         |
   *                 CHILD_PARTITION_RECORD  |      NO MORE RECORDS
   *                |----------------------------------------------|
   *                v                                              v
   * +---------------------------+                       +------------------+
   * | WAIT_FOR_CHILD_PARTITIONS |---------------------->| FINISH_PARTITION |
   * +---------------------------+                       +------------------+
   *                                          |--------------------|
   *                                          v
   *                           +----------------------------+
   *                           | WAIT_FOR_PARENT_PARTITIONS |
   *                           +----------------------------+
   *                                          v
   *                                +------------------+
   *                                | DELETE_PARTITION |
   *                                +------------------+
   *                                          v
   *                                      +------+
   *                                      | DONE |
   *                                      +------+
   *
   */
  // spotless:on
  private ProcessContinuation queryChangeStream(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    try (ResultSet resultSet =
        changeStreamDao.changeStreamQuery(
            partition.getPartitionToken(),
            tracker.currentRestriction().getStartTimestamp(),
            partition.isInclusiveStart(),
            partition.getEndTimestamp(),
            partition.isInclusiveEnd(),
            partition.getHeartbeatMillis())) {
      while (resultSet.next()) {
        // TODO: Check what should we do if there is an error here
        final List<ChangeStreamRecord> records =
            changeStreamRecordMapper.toChangeStreamRecords(
                partition.getPartitionToken(), resultSet.getCurrentRowAsStruct());
        LOG.debug("Mapped records: " + records);

        Optional<ProcessContinuation> maybeContinuation;
        for (ChangeStreamRecord record : records) {
          if (record instanceof DataChangeRecord) {
            maybeContinuation =
                dataChangeRecordAction.run(
                    (DataChangeRecord) record, tracker, receiver, watermarkEstimator);
          } else if (record instanceof HeartbeatRecord) {
            maybeContinuation =
                heartbeatRecordAction.run((HeartbeatRecord) record, tracker, watermarkEstimator);
          } else if (record instanceof ChildPartitionsRecord) {
            maybeContinuation =
                childPartitionsRecordAction.run(
                    (ChildPartitionsRecord) record, partition, tracker, watermarkEstimator);
          } else {
            // FIXME: Check what should we do if the record is unknown
            throw new IllegalArgumentException("Unknown record type " + record.getClass());
          }
          if (maybeContinuation.isPresent()) {
            return maybeContinuation.get();
          }
        }
      }

      return finishPartition(partition, tracker);
    }
  }

  private ProcessContinuation waitForChildPartitions(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    final Long childPartitionsToWaitFor =
        tracker.currentRestriction().getChildPartitionsToWaitFor();
    return waitForChildPartitionsAction
        .run(partition, tracker, childPartitionsToWaitFor)
        .orElseGet(() -> finishPartition(partition, tracker));
  }

  private ProcessContinuation finishPartition(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return finishPartitionAction
        .run(partition, tracker)
        .orElseGet(() -> waitForParentPartitions(partition, tracker));
  }

  private ProcessContinuation waitForParentPartitions(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return waitForParentPartitionsAction
        .run(partition, tracker)
        .orElseGet(() -> deletePartition(partition, tracker));
  }

  private ProcessContinuation deletePartition(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return deletePartitionAction.run(partition, tracker).orElseGet(() -> done(partition, tracker));
  }

  private ProcessContinuation done(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    return donePartitionAction.run(partition, tracker);
  }
}
