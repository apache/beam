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
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSet;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.PartitionMetadataMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestriction;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.ThroughputEstimator;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main action class for querying a partition change stream. This class will perform the change
 * stream query and depending on the record type received, it will dispatch the processing of it to
 * one of the following: {@link ChildPartitionsRecordAction}, {@link HeartbeatRecordAction} or
 * {@link DataChangeRecordAction}.
 *
 * <p>This class will also make sure to mirror the current watermark (event timestamp processed) in
 * the Connector's metadata tables, by registering a bundle after commit action.
 *
 * <p>When the change stream query for the partition is finished, this class will update the state
 * of the partition in the metadata tables as FINISHED, indicating completion.
 */
public class QueryChangeStreamAction {

  private static final Logger LOG = LoggerFactory.getLogger(QueryChangeStreamAction.class);
  private static final String OUT_OF_RANGE_ERROR_MESSAGE = "Specified start_timestamp is invalid";

  private final ChangeStreamDao changeStreamDao;
  private final PartitionMetadataDao partitionMetadataDao;
  private final ChangeStreamRecordMapper changeStreamRecordMapper;
  private final PartitionMetadataMapper partitionMetadataMapper;
  private final DataChangeRecordAction dataChangeRecordAction;
  private final HeartbeatRecordAction heartbeatRecordAction;
  private final ChildPartitionsRecordAction childPartitionsRecordAction;
  private final InitialChildPartitionsRecordAction initialChildPartitionsRecordAction;
  private final ChangeStreamMetrics metrics;
  private final ThroughputEstimator throughputEstimator;

  /**
   * Constructs an action class for performing a change stream query for a given partition.
   *
   * @param changeStreamDao DAO class to perform a change stream query
   * @param partitionMetadataDao DAO class to access the Connector's metadata tables
   * @param changeStreamRecordMapper mapper class to transform change stream records into the
   *     Connector's domain models
   * @param partitionMetadataMapper mapper class to transform partition metadata rows into the
   *     Connector's domain models
   * @param dataChangeRecordAction action class to process {@link DataChangeRecord}s
   * @param heartbeatRecordAction action class to process {@link HeartbeatRecord}s
   * @param childPartitionsRecordAction action class to process {@link ChildPartitionsRecord}s
   * @param metrics metrics gathering class
   * @param throughputEstimator an estimator to calculate local throughput.
   */
  QueryChangeStreamAction(
      ChangeStreamDao changeStreamDao,
      PartitionMetadataDao partitionMetadataDao,
      ChangeStreamRecordMapper changeStreamRecordMapper,
      PartitionMetadataMapper partitionMetadataMapper,
      DataChangeRecordAction dataChangeRecordAction,
      HeartbeatRecordAction heartbeatRecordAction,
      ChildPartitionsRecordAction childPartitionsRecordAction,
      InitialChildPartitionsRecordAction initialChildPartitionsRecordAction,
      ChangeStreamMetrics metrics,
      ThroughputEstimator throughputEstimator) {
    this.changeStreamDao = changeStreamDao;
    this.partitionMetadataDao = partitionMetadataDao;
    this.changeStreamRecordMapper = changeStreamRecordMapper;
    this.partitionMetadataMapper = partitionMetadataMapper;
    this.dataChangeRecordAction = dataChangeRecordAction;
    this.heartbeatRecordAction = heartbeatRecordAction;
    this.childPartitionsRecordAction = childPartitionsRecordAction;
    this.initialChildPartitionsRecordAction = initialChildPartitionsRecordAction;
    this.metrics = metrics;
    this.throughputEstimator = throughputEstimator;
  }

  /**
   * This method will dispatch a change stream query for the given partition, it delegate the
   * processing of the records to one of the corresponding action classes registered and it will
   * keep the state of the partition up to date in the Connector's metadata table.
   *
   * <p>The algorithm is as follows:
   *
   * <ol>
   *   <li>A change stream query for the partition is performed.
   *   <li>For each record, we check the type of the record and dispatch the processing to one of
   *       the actions registered.
   *   <li>If an {@link Optional} with a {@link ProcessContinuation#stop()} is returned from the
   *       actions, we stop processing and return.
   *   <li>When a change stream query finishes successfully (no more records) we update the
   *       partition state to FINISHED.
   * </ol>
   *
   * There might be cases where due to a split at the exact end timestamp of a partition's change
   * stream query, this function could process a residual with an invalid timestamp. In this case,
   * the error is ignored and no work is done for the residual.
   *
   * @param partition the current partition being processed
   * @param tracker the restriction tracker of the {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   * @param receiver the output receiver of the {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   * @param watermarkEstimator the watermark estimator of the {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   * @return a {@link ProcessContinuation#stop()} if a record timestamp could not be claimed or if
   *     the partition processing has finished
   */
  @SuppressWarnings("nullness")
  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {
    final String token = partition.getPartitionToken();
    final Timestamp startTimestamp = tracker.currentRestriction().getStartTimestamp();
    final Timestamp endTimestamp =
        tracker.currentRestriction().getMetadata().getPartitionEndTimestamp();

    // TODO: Potentially we can avoid this fetch, by enriching the runningAt timestamp when the
    // ReadChangeStreamPartitionDoFn#processElement is called
    final PartitionMetadata updatedPartition =
        Optional.ofNullable(partitionMetadataDao.getPartition(token))
            .map(partitionMetadataMapper::from)
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Partition " + token + " not found in metadata table"));

    try (ChangeStreamResultSet resultSet =
        changeStreamDao.changeStreamQuery(
            token, startTimestamp, endTimestamp, partition.getHeartbeatMillis())) {

      metrics.incQueryCounter();
      HashSet<ChildPartitionsRecord> initialRecords = new HashSet<ChildPartitionsRecord>();
      Optional<ProcessContinuation> maybeContinuation;
      while (resultSet.next()) {
        final List<ChangeStreamRecord> records =
            changeStreamRecordMapper.toChangeStreamRecords(
                updatedPartition, resultSet.getCurrentRowAsStruct(), resultSet.getMetadata());

        for (final ChangeStreamRecord record : records) {
          LOG.info("Returned records");
          if (record instanceof DataChangeRecord) {
            maybeContinuation =
                dataChangeRecordAction.run(
                    updatedPartition,
                    (DataChangeRecord) record,
                    tracker,
                    receiver,
                    watermarkEstimator);
          } else if (record instanceof HeartbeatRecord) {
            maybeContinuation =
                heartbeatRecordAction.run(
                    updatedPartition, (HeartbeatRecord) record, tracker, watermarkEstimator);
          } else if (record instanceof ChildPartitionsRecord) {
            if (partition.getPartitionToken().equals(InitialPartition.PARTITION_TOKEN)) {
              initialRecords.add((ChildPartitionsRecord) record);
              maybeContinuation = Optional.empty();
            } else {
              maybeContinuation =
                  childPartitionsRecordAction.run(
                      updatedPartition,
                      (ChildPartitionsRecord) record,
                      tracker,
                      watermarkEstimator);
            }
          } else {
            LOG.error("[" + token + "] Unknown record type " + record.getClass());
            throw new IllegalArgumentException("Unknown record type " + record.getClass());
          }

          // The size of a record is represented by the number of bytes needed for the
          // string representation of the record. Here, we only try to achieve an estimate
          // instead of an accurate throughput.
          this.throughputEstimator.update(
              Timestamp.now(), record.toString().getBytes(StandardCharsets.UTF_8).length);

          if (maybeContinuation.isPresent()) {
            return maybeContinuation;
          }
        }
      }
      if (!initialRecords.isEmpty()) {
        maybeContinuation =
            initialChildPartitionsRecordAction.run(
                updatedPartition, initialRecords, tracker, watermarkEstimator);
        if (maybeContinuation.isPresent()) {
          return maybeContinuation;
        }
      }
    } catch (SpannerException e) {
      /*
      If there is a split when a partition is supposed to be finished, the residual will try
      to perform a change stream query for an out of range interval. We ignore this error
      here, and the residual should be able to claim the end of the timestamp range, finishing
      the partition.
      */
      if (isTimestampOutOfRange(e)) {
        LOG.debug(
            "["
                + token
                + "] query change stream is out of range for "
                + startTimestamp
                + " to "
                + endTimestamp
                + ", finishing stream");
      } else {
        LOG.debug("[" + token + "] error while processing stream " + e);
        throw e;
      }
    }

    LOG.debug("[" + token + "] change stream completed successfully");
    if (tracker.tryClaim(PartitionPosition.queryChangeStream(endTimestamp))) {
      LOG.debug("[" + token + "] Finishing partition");
      partitionMetadataDao.updateToFinished(token);
      metrics.decActivePartitionReadCounter();
      return Optional.empty();
    }
    return Optional.of(ProcessContinuation.stop());
  }

  private boolean isTimestampOutOfRange(SpannerException e) {
    return (e.getErrorCode() == ErrorCode.INVALID_ARGUMENT
            || e.getErrorCode() == ErrorCode.OUT_OF_RANGE)
        && e.getMessage() != null
        && e.getMessage().contains(OUT_OF_RANGE_ERROR_MESSAGE);
  }
}
