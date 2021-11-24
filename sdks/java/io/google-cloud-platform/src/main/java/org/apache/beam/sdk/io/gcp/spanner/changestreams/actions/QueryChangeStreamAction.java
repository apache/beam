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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.actions;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.TimestampConverter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.ChangeStreamResultSet;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.mapper.ChangeStreamRecordMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChangeStreamRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
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
  private static final Tracer TRACER = Tracing.getTracer();
  private static final Duration BUNDLE_FINALIZER_TIMEOUT = Duration.standardMinutes(5);
  private static final String OUT_OF_RANGE_ERROR_MESSAGE = "Specified start_timestamp is invalid";

  private final ChangeStreamDao changeStreamDao;
  private final PartitionMetadataDao partitionMetadataDao;
  private final ChangeStreamRecordMapper changeStreamRecordMapper;
  private final DataChangeRecordAction dataChangeRecordAction;
  private final HeartbeatRecordAction heartbeatRecordAction;
  private final ChildPartitionsRecordAction childPartitionsRecordAction;

  /**
   * Constructs an action class for performing a change stream query for a given partition.
   *
   * @param changeStreamDao DAO class to perform a change stream query
   * @param partitionMetadataDao DAO class to access the Connector's metadata tables
   * @param changeStreamRecordMapper mapper class to transform change stream records into the
   *     Connector's domain models
   * @param dataChangeRecordAction action class to process {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord}s
   * @param heartbeatRecordAction action class to process {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord}s
   * @param childPartitionsRecordAction action class to process {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord}s
   */
  QueryChangeStreamAction(
      ChangeStreamDao changeStreamDao,
      PartitionMetadataDao partitionMetadataDao,
      ChangeStreamRecordMapper changeStreamRecordMapper,
      DataChangeRecordAction dataChangeRecordAction,
      HeartbeatRecordAction heartbeatRecordAction,
      ChildPartitionsRecordAction childPartitionsRecordAction) {
    this.changeStreamDao = changeStreamDao;
    this.partitionMetadataDao = partitionMetadataDao;
    this.changeStreamRecordMapper = changeStreamRecordMapper;
    this.dataChangeRecordAction = dataChangeRecordAction;
    this.heartbeatRecordAction = heartbeatRecordAction;
    this.childPartitionsRecordAction = childPartitionsRecordAction;
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
   *   <li>Before returning we register a bundle finalizer callback to update the watermark of the
   *       partition in the metadata tables to the latest processed timestamp.
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
   * @param bundleFinalizer the bundle finalizer for {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   *     bundles
   * @return a {@link ProcessContinuation#stop()} if a record timestamp could not be claimed or if
   *     the partition processing has finished
   */
  @SuppressWarnings("nullness")
  @VisibleForTesting
  public ProcessContinuation run(
      PartitionMetadata partition,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<DataChangeRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      BundleFinalizer bundleFinalizer) {
    final String token = partition.getPartitionToken();
    final Timestamp endTimestamp = partition.getEndTimestamp();

    /*
     * FIXME(b/202802422): Workaround until the backend is fixed.
     * The change stream API returns invalid argument if we try to use a child partition start
     * timestamp for a previously returned query. If we split at that exact time, we won't be able
     * to obtain the child partition on the residual restriction, since it will start at the child
     * partition start time.
     * To circumvent this, we always start querying one microsecond before the restriction start
     * time, and ignore any records that are before the restriction start time. This way the child
     * partition should be returned within the query.
     */
    final Timestamp restrictionStartTimestamp =
        Timestamp.ofTimeMicroseconds(tracker.currentRestriction().getFrom());
    final Timestamp previousStartTimestamp =
        Timestamp.ofTimeMicroseconds(
            TimestampConverter.timestampToMicros(restrictionStartTimestamp) - 1);
    final boolean isFirstRun =
        restrictionStartTimestamp.compareTo(partition.getStartTimestamp()) == 0;
    final Timestamp startTimestamp =
        isFirstRun ? restrictionStartTimestamp : previousStartTimestamp;

    try (Scope scope =
        TRACER.spanBuilder("QueryChangeStreamAction").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(token));

      final PartitionMetadata updatedPartition =
          partitionMetadataDao
              .runInTransaction(
                  transaction -> {
                    transaction.updateQueryStartedAt(token);
                    return transaction.getPartition(token);
                  })
              .getResult();

      try (ChangeStreamResultSet resultSet =
          changeStreamDao.changeStreamQuery(
              token, startTimestamp, endTimestamp, partition.getHeartbeatMillis())) {

        long recordsProcessed = 0;
        while (resultSet.next()) {
          // TODO: Check what should we do if there is an error here
          final List<ChangeStreamRecord> records =
              changeStreamRecordMapper.toChangeStreamRecords(
                  updatedPartition, resultSet.getCurrentRowAsStruct(), resultSet.getMetadata());

          Optional<ProcessContinuation> maybeContinuation;
          for (final ChangeStreamRecord record : records) {
            if (record.getRecordTimestamp().compareTo(restrictionStartTimestamp) < 0) {
              continue;
            }

            recordsProcessed++;
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
              maybeContinuation =
                  childPartitionsRecordAction.run(
                      updatedPartition,
                      (ChildPartitionsRecord) record,
                      tracker,
                      watermarkEstimator);
            } else {
              LOG.error("[" + token + "] Unknown record type " + record.getClass());
              // FIXME: Check what should we do if the record is unknown
              throw new IllegalArgumentException("Unknown record type " + record.getClass());
            }
            if (maybeContinuation.isPresent()) {
              LOG.debug("[" + token + "] Continuation present, returning " + maybeContinuation);
              partitionMetadataDao.updateRecordsProcessed(token, recordsProcessed);
              bundleFinalizer.afterBundleCommit(
                  Instant.now().plus(BUNDLE_FINALIZER_TIMEOUT),
                  updateWatermarkCallback(token, watermarkEstimator));
              return maybeContinuation.get();
            }
          }
        }

        partitionMetadataDao.updateRecordsProcessed(token, recordsProcessed);
        bundleFinalizer.afterBundleCommit(
            Instant.now().plus(BUNDLE_FINALIZER_TIMEOUT),
            updateWatermarkCallback(token, watermarkEstimator));

      } catch (SpannerException e) {
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
          throw e;
        }
      }
    }

    final long endMicros = TimestampConverter.timestampToMicros(endTimestamp);
    LOG.debug("[" + token + "] change stream completed successfully");
    if (tracker.tryClaim(endMicros)) {
      LOG.debug("[" + token + "] Finishing partition");
      partitionMetadataDao.updateToFinished(token);
      LOG.info("[" + token + "] Partition finished");
    }
    return ProcessContinuation.stop();
  }

  private BundleFinalizer.Callback updateWatermarkCallback(
      String token, WatermarkEstimator<Instant> watermarkEstimator) {
    return () -> {
      final Instant watermark = watermarkEstimator.currentWatermark();
      LOG.debug("[" + token + "] Updating current watermark to " + watermark);
      try {
        partitionMetadataDao.updateWatermark(
            token, TimestampConverter.timestampFromMillis(watermark.getMillis()));
      } catch (SpannerException e) {
        if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
          LOG.debug("[" + token + "] Unable to update the current watermark, partition NOT FOUND");
        } else {
          LOG.error("[" + token + "] Error updating the current watermark: " + e.getMessage(), e);
        }
      }
    };
  }

  private boolean isTimestampOutOfRange(SpannerException e) {
    return (e.getErrorCode() == ErrorCode.INVALID_ARGUMENT
            || e.getErrorCode() == ErrorCode.OUT_OF_RANGE)
        && e.getMessage() != null
        && e.getMessage().contains(OUT_OF_RANGE_ERROR_MESSAGE);
  }
}
