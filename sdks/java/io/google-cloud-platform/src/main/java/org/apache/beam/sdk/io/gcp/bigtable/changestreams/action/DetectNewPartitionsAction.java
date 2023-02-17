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

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.formatByteStringRange;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.isSuperset;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.partitionsToString;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableAdminDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
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
@SuppressWarnings({
  "checkstyle:JavadocMethod",
  "initialization.fields.uninitialized",
  "UnusedVariable",
  "UnusedMethod"
})
@Internal
public class DetectNewPartitionsAction {
  private static final Logger LOG = LoggerFactory.getLogger(DetectNewPartitionsAction.class);

  private static final Duration DEBUG_WATERMARK_DELAY = Duration.standardMinutes(5);

  private final ChangeStreamMetrics metrics;
  private final MetadataTableDao metadataTableDao;
  private final GenerateInitialPartitionsAction generateInitialPartitionsAction;

  public DetectNewPartitionsAction(
      ChangeStreamMetrics metrics,
      MetadataTableDao metadataTableDao,
      GenerateInitialPartitionsAction generateInitialPartitionsAction) {
    this.metrics = metrics;
    this.metadataTableDao = metadataTableDao;
    this.generateInitialPartitionsAction = generateInitialPartitionsAction;
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
    Range.ByteStringRange partition =
        metadataTableDao.convertNewPartitionRowKeyToPartition(row.getKey());

    // Ensure all parent partitions have stopped and updated the metadata table with the new
    // partition continuation token.
    List<Range.ByteStringRange> parentPartitions = new ArrayList<>();
    for (RowCell cell : row.getCells(MetadataTableAdminDao.CF_PARENT_PARTITIONS)) {
      Range.ByteStringRange parentPartition =
          Range.ByteStringRange.toByteStringRange(cell.getQualifier());
      parentPartitions.add(parentPartition);
    }
    if (!isSuperset(parentPartitions, partition)) {
      LOG.warn(
          "DNP: New partition: {} does not have all the parents {}",
          formatByteStringRange(partition),
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
          formatByteStringRange(partition),
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
        formatByteStringRange(partition));
    // We are outputting elements with timestamp of 0 to prevent reliance on event time. This limits
    // the ability to window on commit time of any data changes. It is still possible to window on
    // processing time.
    receiver.outputWithTimestamp(partitionRecord, Instant.EPOCH);
    recordedPartitionRecords.add(row.getKey());
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
    if (tracker.currentRestriction().getFrom() == 0L) {
      return generateInitialPartitionsAction.run(receiver, tracker, watermarkEstimator, startTime);
    }

    if (!tracker.tryClaim(tracker.currentRestriction().getFrom())) {
      return ProcessContinuation.stop();
    }

    List<ByteString> recordedPartitionRecords = new ArrayList<>();

    ServerStream<Row> rows = metadataTableDao.readNewPartitions();
    for (Row row : rows) {
      processNewPartition(row, receiver, recordedPartitionRecords);
    }

    cleanUpAfterCommit(bundleFinalizer, recordedPartitionRecords);

    return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
  }
}
