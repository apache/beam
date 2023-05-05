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
package org.apache.beam.sdk.io.gcp.bigtable.changestreams.action;

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.getMissingAndOverlappingPartitionsFromKeySpace;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.isSuperset;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.partitionsToString;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.reconciler.PartitionReconciler;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.primitives.Longs;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class processes {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn}.
 */
// checkstyle bug is causing an issue with '@throws InvalidProtocolBufferException'
// Allows for transient fields to be initialized later
@SuppressWarnings({"checkstyle:JavadocMethod", "initialization.fields.uninitialized"})
@Internal
public class DetectNewPartitionsAction {
  private static final Logger LOG = LoggerFactory.getLogger(DetectNewPartitionsAction.class);

  private static final Duration DEBUG_WATERMARK_DELAY = Duration.standardMinutes(5);

  private final ChangeStreamMetrics metrics;
  private final MetadataTableDao metadataTableDao;
  private final GenerateInitialPartitionsAction generateInitialPartitionsAction;

  private transient PartitionReconciler partitionReconciler;

  public DetectNewPartitionsAction(
      ChangeStreamMetrics metrics,
      MetadataTableDao metadataTableDao,
      GenerateInitialPartitionsAction generateInitialPartitionsAction) {
    this.metrics = metrics;
    this.metadataTableDao = metadataTableDao;
    this.generateInitialPartitionsAction = generateInitialPartitionsAction;
  }

  /**
   * Periodically advances DetectNewPartition's (DNP) watermark based on the watermark of all the
   * partitions recorded in the metadata table. We don't advance DNP's watermark on every run to
   * "now" because of a possible inconsistent state. DNP's watermark is used to hold the entire
   * pipeline's watermark back.
   *
   * <p>The low watermark is important to determine when to terminate the pipeline. During splits
   * and merges, the watermark step may appear to be higher than it actually is. If a partition, at
   * watermark 100, splits, it considered completed. If all other partitions including DNP have
   * watermark beyond 100, the low watermark of the pipeline is higher than 100. However, the split
   * partitions will have a watermark of 100 because they will resume from where the parent
   * partition has stopped. But this would mean the low watermark of the pipeline needs to move
   * backwards in time, which is not possible.
   *
   * <p>We "fix" this by using DNP's watermark to hold the pipeline's watermark down. DNP will
   * periodically scan the metadata table for all the partitions watermarks. It only advances its
   * watermark forward to the low watermark of the partitions. So in the case of a partition
   * split/merge the low watermark of the pipeline is held back by DNP. We guarantee correctness by
   * ensuring all the partitions exists in the metadata table in order to calculate the low
   * watermark. It is possible that some partitions might be missing in between split and merges.
   *
   * @param tracker restriction tracker to guide how frequently watermark should be advanced
   * @param watermarkEstimator watermark estimator to advance the watermark
   */
  private void advanceWatermark(
      RestrictionTracker<OffsetRange, Long> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator)
      throws InvalidProtocolBufferException {
    // We currently choose to update the watermark every 10 runs. We want to choose a number that is
    // frequent, so the watermark isn't lagged behind too far. Also not too frequent so we do not
    // overload the table with full table scans.
    if (tracker.currentRestriction().getFrom() % 10 == 0) {
      // Get partitions with a watermark set but skip rows w a lock and no watermark yet
      ServerStream<Row> rows = metadataTableDao.readFromMdTableStreamPartitionsWithWatermark();
      List<ByteStringRange> partitions = new ArrayList<>();
      HashMap<ByteStringRange, Instant> slowPartitions = new HashMap<>();
      Instant lowWatermark = Instant.ofEpochMilli(Long.MAX_VALUE);
      for (Row row : rows) {
        Instant watermark = MetadataTableEncoder.parseWatermarkFromRow(row);
        if (watermark == null) {
          continue;
        }
        // Update low watermark if watermark < low watermark.
        if (watermark.compareTo(lowWatermark) < 0) {
          lowWatermark = watermark;
        }
        ByteStringRange partition =
            metadataTableDao.convertStreamPartitionRowKeyToPartition(row.getKey());
        partitions.add(partition);
        if (watermark.plus(DEBUG_WATERMARK_DELAY).isBeforeNow()) {
          slowPartitions.put(partition, watermark);
        }
      }
      List<ByteStringRange> missingAndOverlappingPartitions =
          getMissingAndOverlappingPartitionsFromKeySpace(partitions);
      if (missingAndOverlappingPartitions.isEmpty()) {
        watermarkEstimator.setWatermark(lowWatermark);
        LOG.info("DNP: Updating watermark: " + watermarkEstimator.currentWatermark());
      } else {
        LOG.warn(
            "DNP: Could not update watermark because missing {}",
            partitionsToString(missingAndOverlappingPartitions));
      }
      if (!slowPartitions.isEmpty()) {
        LOG.warn(
            "DNP: Watermark is being held back by the following partitions: {}",
            slowPartitions.entrySet().stream()
                .map(
                    e ->
                        ByteStringRangeHelper.formatByteStringRange(e.getKey())
                            + " => "
                            + e.getValue())
                .collect(Collectors.joining(", ", "{", "}")));
      }
      partitionReconciler.addMissingPartitions(missingAndOverlappingPartitions);
    }
  }

  /**
   * Process a single new partition. New partition resulting from split and merges need to be
   * outputted to be streamed. Regardless if it's a split or a merge, we have the same verification
   * process in order to ensure the new partition can actually be streamed.
   *
   * <p>When a parent partition splits, it receives two or more new partitions. It will write a new
   * row, with the new row ranges as row key, for each new partition. These new partitions can be
   * immediately streamed.
   *
   * <p>The complicated scenario is merges. Two or more parent partitions will merge into one new
   * partition. Each parent partition receives the same new partition (row range) but each parent
   * partition will have a different continuation token. The parent partitions will all write to the
   * same row key form by the new row range. Each parent will record its continuation token, and
   * watermark. Parent partitions may not receive the message to stop at the same time. So when we
   * try to process the new partition, we need to ensure that all the parent partitions have stopped
   * and recorded their metadata table. We do so by verifying that the row ranges of the parents
   * covers a contiguous block of row range that is same or wider (superset) of the new row range.
   *
   * <p>For example, partition1, A-B, and partition2, B-C, merges into partition3, A-C.
   *
   * <ol>
   *   <li>p1 writes to row A-C in metadata table
   *   <li>processNewPartition process A-C seeing that only A-B has been recorded and A-B does not
   *       cover A-C. Do Nothing
   *   <li>p2 writes to row A-C in metadata table
   *   <li>processNewPartition process A-C again, seeing that A-B and B-C has been recorded and
   *       outputs new partition A-C to be streamed.
   * </ol>
   *
   * <p>Note that, the algorithm to verify if a merge is valid, also correctly verifies if a split
   * is valid. A split is immediately valid as long as the row exists because there's only one
   * parent that needs to write to that row.
   *
   * @param row new partition to be processed
   * @param receiver to output new partitions
   * @param recordedPartitionRecords add the partition if it's been processed
   * @throws InvalidProtocolBufferException if partition can't be converted from row key.
   */
  private void processNewPartition(
      Row row, OutputReceiver<PartitionRecord> receiver, List<ByteString> recordedPartitionRecords)
      throws Exception {
    ByteStringRange partition = metadataTableDao.convertNewPartitionRowKeyToPartition(row.getKey());

    partitionReconciler.addNewPartition(partition, row.getKey());

    // Ensure all parent partitions have stopped and updated the metadata table with the new
    // partition continuation token.
    List<ByteStringRange> parentPartitions = new ArrayList<>();
    for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_PARENT_PARTITIONS)) {
      ByteStringRange parentPartition = ByteStringRange.toByteStringRange(cell.getQualifier());
      parentPartitions.add(parentPartition);
    }
    if (!isSuperset(parentPartitions, partition)) {
      LOG.warn(
          "DNP: New partition: {} does not have all the parents {}",
          ByteStringRangeHelper.formatByteStringRange(partition),
          partitionsToString(parentPartitions));
      return;
    }

    // Capture all the initial continuation tokens to request for the new partition.
    ImmutableList.Builder<ChangeStreamContinuationToken> changeStreamContinuationTokensBuilder =
        ImmutableList.builder();
    for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_INITIAL_TOKEN)) {
      ChangeStreamContinuationToken changeStreamContinuationToken =
          ChangeStreamContinuationToken.fromByteString(cell.getQualifier());
      changeStreamContinuationTokensBuilder.add(changeStreamContinuationToken);
    }
    ImmutableList<ChangeStreamContinuationToken> changeStreamContinuationTokens =
        changeStreamContinuationTokensBuilder.build();

    // If parent and continuation token count are not the same, it's possible the same parent wrote
    // multiple continuation tokens or there's an inconsistent state.
    if (parentPartitions.size() != changeStreamContinuationTokens.size()) {
      LOG.warn(
          "DNP: New partition {} parent partitions count {} != continuation token count {}",
          ByteStringRangeHelper.formatByteStringRange(partition),
          parentPartitions.size(),
          changeStreamContinuationTokens.size());
    }

    // Capture all the low watermark and calculate the low watermark among all the parents.
    List<Long> parentLowWatermark = new ArrayList<>();
    for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_PARENT_LOW_WATERMARKS)) {
      parentLowWatermark.add(Longs.fromByteArray(cell.getValue().toByteArray()));
    }
    Long lowWatermarkMilli = Collections.min(parentLowWatermark);
    Instant lowWatermark = Instant.ofEpochMilli(lowWatermarkMilli);

    String uid = UniqueIdGenerator.getNextId();
    PartitionRecord partitionRecord =
        new PartitionRecord(partition, changeStreamContinuationTokens, uid, lowWatermark);
    if (parentPartitions.size() > 1) {
      metrics.incPartitionMergeCount();
    } else {
      metrics.incPartitionSplitCount();
    }
    LOG.info(
        "DNP: Split/Merge {} into {}",
        parentPartitions.stream()
            .map(ByteStringRangeHelper::formatByteStringRange)
            .collect(Collectors.joining(",", "{", "}")),
        ByteStringRangeHelper.formatByteStringRange(partition));
    // We are outputting elements with timestamp of 0 to prevent reliance on event time. This limits
    // the ability to window on commit time of any data changes. It is still possible to window on
    // processing time.
    receiver.outputWithTimestamp(partitionRecord, Instant.EPOCH);
    recordedPartitionRecords.add(row.getKey());
  }

  /**
   * Uses PartitionReconciler to process any partitions that it has found to be missing for too long
   * and restarts them. For more details on why this is necessary see {@link PartitionReconciler}
   *
   * @param receiver used to output reconciled partitions
   * @param watermarkEstimator read the low watermark for all partitions
   * @param startTime startTime of the pipeline
   * @param recordedPartitionRecords the list of row keys that needs to be cleaned up
   */
  private void processReconcilerPartitions(
      OutputReceiver<PartitionRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      Instant startTime,
      List<ByteString> recordedPartitionRecords) {
    for (HashMap.Entry<ByteStringRange, Set<ByteString>> partitionToReconcile :
        partitionReconciler.getPartitionsToReconcile().entrySet()) {
      String uid = UniqueIdGenerator.getNextId();

      // When we reconcile, we start from 1h prior to effectively eliminate the possibility of
      // missing data.
      Instant reconciledTime =
          watermarkEstimator.currentWatermark().minus(Duration.standardMinutes(60));
      if (reconciledTime.compareTo(startTime) < 0) {
        reconciledTime = startTime;
      }

      PartitionRecord partitionRecord =
          new PartitionRecord(partitionToReconcile.getKey(), reconciledTime, uid, reconciledTime);
      receiver.outputWithTimestamp(partitionRecord, Instant.EPOCH);
      recordedPartitionRecords.addAll(partitionToReconcile.getValue());
      LOG.warn(
          "DNP: Reconciling missing partition: {} and cleaning up rows {}",
          partitionRecord,
          partitionToReconcile.getValue().stream()
              .map(
                  rowKey -> {
                    try {
                      return ByteStringRangeHelper.formatByteStringRange(
                          metadataTableDao.convertNewPartitionRowKeyToPartition(rowKey));
                    } catch (InvalidProtocolBufferException exception) {
                      return rowKey.toStringUtf8();
                    }
                  })
              .collect(Collectors.joining(", ", "{", "}")));
    }
  }

  /**
   * After processing new partitions and if it was outputted successfully, we need to clean up the
   * metadata table so that we don't try to process the same new partition again.
   *
   * <p>This is performed at a best effort basis. If some clean up is unsuccessful, the new
   * partition will be processed and output again in subsequent runs. This is OK because the DoFn
   * that stream partitions performs locking to prevent multiple DoFn from streaming the same
   * partition.
   *
   * @param bundleFinalizer finalizer that registered the callback to perform the cleanup
   * @param recordedPartitionRecords the list of row keys that needs to be cleaned up
   */
  private void cleanUpAfterCommit(
      BundleFinalizer bundleFinalizer, List<ByteString> recordedPartitionRecords) {
    bundleFinalizer.afterBundleCommit(
        Instant.ofEpochMilli(Long.MAX_VALUE),
        () -> {
          for (ByteString rowKey : recordedPartitionRecords) {
            metadataTableDao.deleteRowKey(rowKey);
          }
        });
  }

  /**
   * Perform the necessary steps to manage initial set of partitions and new partitions. Currently,
   * we set to process new partitions every second.
   *
   * <ol>
   *   <li>Look up the initial list of partitions to stream if it's the very first run.
   *   <li>On rest of the runs, try advancing watermark if needed.
   *   <li>Update the metadata table with info about this DoFn.
   *   <li>Process new partitions and output them.
   *   <li>Reconcile any Partitions that haven't been streaming for a long time
   *   <li>Register callback to clean up processed partitions after bundle has been finalized.
   * </ol>
   *
   * @param tracker offset tracker that simply increment by 1 every single run
   * @param receiver output new partitions
   * @param watermarkEstimator update watermark that is a representation of the low watermark of the
   *     entire beam pipeline
   * @param bundleFinalizer perform after bundle output actions to clean up metadata table
   * @return {@link ProcessContinuation#resume()} with 1-second delay if the stream continues,
   *     otherwise {@link ProcessContinuation#stop()}
   * @throws InvalidProtocolBufferException if failing to process new partitions
   */
  @VisibleForTesting
  public ProcessContinuation run(
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<PartitionRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      BundleFinalizer bundleFinalizer,
      Instant startTime)
      throws Exception {
    LOG.debug("DNP: Watermark: " + watermarkEstimator.getState());
    LOG.debug("DNP: CurrentTracker: " + tracker.currentRestriction().getFrom());
    if (tracker.currentRestriction().getFrom() == 0L) {
      return generateInitialPartitionsAction.run(receiver, tracker, watermarkEstimator, startTime);
    }

    // Create a new partition reconciler every run to reset the state each time.
    partitionReconciler = new PartitionReconciler(metadataTableDao);

    advanceWatermark(tracker, watermarkEstimator);

    if (!tracker.tryClaim(tracker.currentRestriction().getFrom())) {
      LOG.error(
          "DNP: Couldn't continue because we failed to claim tracker: "
              + tracker.currentRestriction());
      return ProcessContinuation.stop();
    }

    List<ByteString> recordedPartitionRecords = new ArrayList<>();

    ServerStream<Row> rows = metadataTableDao.readNewPartitions();
    for (Row row : rows) {
      processNewPartition(row, receiver, recordedPartitionRecords);
    }

    processReconcilerPartitions(receiver, watermarkEstimator, startTime, recordedPartitionRecords);

    cleanUpAfterCommit(bundleFinalizer, recordedPartitionRecords);

    return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
  }
}
