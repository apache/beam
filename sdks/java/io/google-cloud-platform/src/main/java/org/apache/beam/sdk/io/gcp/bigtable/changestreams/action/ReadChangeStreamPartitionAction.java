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

import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.coverSameKeySpace;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.formatByteStringRange;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.partitionsToString;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamContinuationTokenHelper.getTokenWithCorrectPartition;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.common.Status;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.ChangeStreamDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.DetectNewPartitionsDoFn;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.BytesThroughputEstimator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.SizeEstimator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is part of {@link
 * org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF.
 */
@Internal
public class ReadChangeStreamPartitionAction {
  private static final Logger LOG = LoggerFactory.getLogger(ReadChangeStreamPartitionAction.class);

  private final MetadataTableDao metadataTableDao;
  private final ChangeStreamDao changeStreamDao;
  private final ChangeStreamMetrics metrics;
  private final ChangeStreamAction changeStreamAction;
  private final Duration heartbeatDuration;
  private final SizeEstimator<KV<ByteString, ChangeStreamRecord>> sizeEstimator;

  public ReadChangeStreamPartitionAction(
      MetadataTableDao metadataTableDao,
      ChangeStreamDao changeStreamDao,
      ChangeStreamMetrics metrics,
      ChangeStreamAction changeStreamAction,
      Duration heartbeatDuration,
      SizeEstimator<KV<ByteString, ChangeStreamRecord>> sizeEstimator) {
    this.metadataTableDao = metadataTableDao;
    this.changeStreamDao = changeStreamDao;
    this.metrics = metrics;
    this.changeStreamAction = changeStreamAction;
    this.heartbeatDuration = heartbeatDuration;
    this.sizeEstimator = sizeEstimator;
  }

  /**
   * Streams changes from a specific partition. This function is responsible for maintaining the
   * lifecycle of streaming the partition. We delegate to {@link ChangeStreamAction} to process
   * individual response from the change stream.
   *
   * <p>Before we send a request to Cloud Bigtable to stream the partition, we need to perform a few
   * things.
   *
   * <ol>
   *   <li>Lock the partition. Due to the design of the change streams connector, it is possible
   *       that multiple DoFn are started trying to stream the same partition. However, only 1 DoFn
   *       should be streaming a partition. So we solve this by using the metadata table as a
   *       distributed lock. We attempt to lock the partition for this DoFn's UUID. If it is
   *       successful, it means this DoFn is the only one that can stream the partition and
   *       continue. Otherwise, terminate this DoFn because another DoFn is streaming this partition
   *       already.
   *   <li>Process CloseStream if it exists. In order to solve a possible inconsistent state
   *       problem, we do not process CloseStream after receiving it. We claim the CloseStream in
   *       the RestrictionTracker so it persists after a checkpoint. We checkpoint to flush all the
   *       DataChanges. Then on resume, we process the CloseStream. There are only 2 expected Status
   *       for CloseStream: OK and Out of Range.
   *       <ol>
   *         <li>OK status is returned when the predetermined endTime has been reached. In this
   *             case, we update the watermark and update the metadata table. {@link
   *             DetectNewPartitionsDoFn} aggregates the watermark from all the streams to ensure
   *             all the streams have reached beyond endTime so it can also terminate and end the
   *             beam job.
   *         <li>Out of Range is returned when the partition has either been split into more
   *             partitions or merged into a larger partition. In this case, we write to the
   *             metadata table the new partitions' information so that {@link
   *             DetectNewPartitionsDoFn} can read and output those new partitions to be streamed.
   *             We also need to ensure we clean up this partition's metadata to release the lock.
   *       </ol>
   *   <li>Update the metadata table with the watermark and additional debugging info.
   *   <li>Stream the partition.
   * </ol>
   *
   * @param partitionRecord partition information used to identify this stream
   * @param tracker restriction tracker of {@link
   *     org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   * @param receiver output receiver for {@link
   *     org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   * @param watermarkEstimator watermark estimator {@link
   *     org.apache.beam.sdk.io.gcp.bigtable.changestreams.dofn.ReadChangeStreamPartitionDoFn}
   * @return {@link ProcessContinuation#stop} if a checkpoint is required or the stream has
   *     completed. Or {@link ProcessContinuation#resume} if a checkpoint is required.
   * @throws IOException when stream fails.
   */
  public ProcessContinuation run(
      PartitionRecord partitionRecord,
      RestrictionTracker<StreamProgress, StreamProgress> tracker,
      OutputReceiver<KV<ByteString, ChangeStreamRecord>> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator)
      throws IOException {
    BytesThroughputEstimator<KV<ByteString, ChangeStreamRecord>> throughputEstimator =
        new BytesThroughputEstimator<>(sizeEstimator, Instant.now());
    // Lock the partition
    if (tracker.currentRestriction().isEmpty()) {
      boolean lockedPartition = metadataTableDao.lockAndRecordPartition(partitionRecord);
      // Clean up NewPartition on the first run regardless of locking result. If locking fails it
      // means this partition is being streamed, then cleaning up NewPartitions avoids lingering
      // NewPartitions.
      for (NewPartition newPartition : partitionRecord.getParentPartitions()) {
        metadataTableDao.deleteNewPartition(newPartition);
      }
      if (!lockedPartition) {
        LOG.info(
            "RCSP  {} : Could not acquire lock with uid: {}, because this is a "
                + "duplicate and another worker is working  on this partition already.",
            formatByteStringRange(partitionRecord.getPartition()),
            partitionRecord.getUuid());
        StreamProgress streamProgress = new StreamProgress();
        streamProgress.setFailToLock(true);
        metrics.decPartitionStreamCount();
        tracker.tryClaim(streamProgress);
        return ProcessContinuation.stop();
      }
    } else if (tracker.currentRestriction().getCloseStream() == null
        && !metadataTableDao.doHoldLock(
            partitionRecord.getPartition(), partitionRecord.getUuid())) {
      // We only verify the lock if we are not holding CloseStream because if this is a retry of
      // CloseStream we might have already cleaned up the lock in a previous attempt.
      // Failed correctness check on this worker holds the lock on this partition. This shouldn't
      // fail because there's a restriction tracker which means this worker has already acquired the
      // lock and once it has acquired the lock it shouldn't fail the lock check.
      LOG.warn(
          "RCSP  {} : Subsequent run that doesn't hold the lock {}. This is not unexpected and "
              + "should probably be reviewed.",
          formatByteStringRange(partitionRecord.getPartition()),
          partitionRecord.getUuid());
      StreamProgress streamProgress = new StreamProgress();
      streamProgress.setFailToLock(true);
      metrics.decPartitionStreamCount();
      tracker.tryClaim(streamProgress);
      return ProcessContinuation.stop();
    }

    // Process CloseStream if it exists
    CloseStream closeStream = tracker.currentRestriction().getCloseStream();
    if (closeStream != null) {
      LOG.debug("RCSP: Processing CloseStream");
      metrics.decPartitionStreamCount();
      if (closeStream.getStatus().getCode() == Status.Code.OK) {
        // We need to update watermark here. We're terminating this stream because we have reached
        // endTime. Instant.now is greater or equal to endTime. The goal here is
        // DNP will need to know this stream has passed the endTime so DNP can eventually terminate.
        Instant terminatingWatermark = Instant.ofEpochMilli(Long.MAX_VALUE);
        Instant endTime = partitionRecord.getEndTime();
        if (endTime != null) {
          terminatingWatermark = endTime;
        }
        watermarkEstimator.setWatermark(terminatingWatermark);
        metadataTableDao.updateWatermark(
            partitionRecord.getPartition(), watermarkEstimator.currentWatermark(), null);
        LOG.info(
            "RCSP {}: Reached end time, terminating...",
            formatByteStringRange(partitionRecord.getPartition()));
        return ProcessContinuation.stop();
      }
      if (closeStream.getStatus().getCode() != Status.Code.OUT_OF_RANGE) {
        LOG.error(
            "RCSP {}: Reached unexpected terminal state: {}",
            formatByteStringRange(partitionRecord.getPartition()),
            closeStream.getStatus());
        return ProcessContinuation.stop();
      }
      // Release the lock only if the uuid matches. In normal operation this doesn't change
      // anything. However, it's possible for this RCSP to crash while processing CloseStream but
      // after the side effects of writing the new partitions to the metadata table. New partitions
      // can be created while this RCSP restarts from the previous checkpoint and processes the
      // CloseStream again. In certain race scenarios the child partitions may merge back to this
      // partition, but as a new RCSP. The new partition (same as this partition) would write the
      // exact same content to the metadata table but with a different uuid. We don't want to
      // accidentally delete the StreamPartition because it now belongs to the new RCSP.
      // If the uuid is the same (meaning this race scenario did not take place) we release the lock
      // and mark the StreamPartition to be deleted, so we can delete it after we have written the
      // NewPartitions.
      metadataTableDao.releaseStreamPartitionLockForDeletion(
          partitionRecord.getPartition(), partitionRecord.getUuid());
      // The partitions in the continuation tokens must cover the same key space as this partition.
      // If there's only 1 token, then the token's partition is equals to this partition.
      // If there are more than 1 tokens, then the tokens form a continuous row range equals to this
      // partition.
      List<ByteStringRange> childPartitions = new ArrayList<>();
      List<ByteStringRange> tokenPartitions = new ArrayList<>();
      // Check if NewPartitions field exists, if not we default to using just the
      // ChangeStreamContinuationTokens.
      boolean useNewPartitionsField =
          closeStream.getNewPartitions().size()
              == closeStream.getChangeStreamContinuationTokens().size();
      for (int i = 0; i < closeStream.getChangeStreamContinuationTokens().size(); i++) {
        ByteStringRange childPartition;
        if (useNewPartitionsField) {
          childPartition = closeStream.getNewPartitions().get(i);
        } else {
          childPartition = closeStream.getChangeStreamContinuationTokens().get(i).getPartition();
        }
        childPartitions.add(childPartition);
        ChangeStreamContinuationToken token =
            getTokenWithCorrectPartition(
                partitionRecord.getPartition(),
                closeStream.getChangeStreamContinuationTokens().get(i));
        tokenPartitions.add(token.getPartition());
        metadataTableDao.writeNewPartition(
            new NewPartition(
                childPartition, Collections.singletonList(token), watermarkEstimator.getState()));
      }
      LOG.info(
          "RCSP {}: Split/Merge into {}",
          formatByteStringRange(partitionRecord.getPartition()),
          partitionsToString(childPartitions));
      if (!coverSameKeySpace(tokenPartitions, partitionRecord.getPartition())) {
        LOG.warn(
            "RCSP {}: CloseStream has tokens {} that don't cover the entire keyspace",
            formatByteStringRange(partitionRecord.getPartition()),
            partitionsToString(tokenPartitions));
      }
      // Perform the real cleanup. This step is no op if the race mentioned above occurs (splits and
      // merges results back to this partition again) because when we register the "new" partition,
      // we unset the deletion bit.
      metadataTableDao.deleteStreamPartitionRow(partitionRecord.getPartition());
      return ProcessContinuation.stop();
    }

    // Update the metadata table with the watermark
    metadataTableDao.updateWatermark(
        partitionRecord.getPartition(),
        watermarkEstimator.getState(),
        tracker.currentRestriction().getCurrentToken());

    // Start to stream the partition.
    ServerStream<ChangeStreamRecord> stream = null;
    try {
      stream =
          changeStreamDao.readChangeStreamPartition(
              partitionRecord,
              tracker.currentRestriction(),
              partitionRecord.getEndTime(),
              heartbeatDuration);
      for (ChangeStreamRecord record : stream) {
        Optional<ProcessContinuation> result =
            changeStreamAction.run(
                partitionRecord,
                record,
                tracker,
                receiver,
                watermarkEstimator,
                throughputEstimator);
        // changeStreamAction will usually return Optional.empty() except for when a checkpoint
        // (either runner or pipeline initiated) is required.
        if (result.isPresent()) {
          return result.get();
        }
      }
    } catch (Exception e) {
      throw e;
    } finally {
      if (stream != null) {
        stream.cancel();
      }
    }
    return ProcessContinuation.resume();
  }
}
