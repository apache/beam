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
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.getMissingPartitionsFromEntireKeySpace;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.getOverlappingPartitions;
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.partitionsToString;

import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.UniqueIdGenerator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.DetectNewPartitionsState;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.InitialPipelineState;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.NewPartition;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.StreamPartitionWithWatermark;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.reconciler.OrphanedMetadataCleaner;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.reconciler.PartitionReconciler;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
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
  @Nullable private final Instant endTime;
  private final ProcessNewPartitionsAction processNewPartitionsAction;
  private final GenerateInitialPartitionsAction generateInitialPartitionsAction;
  private final ResumeFromPreviousPipelineAction resumeFromPreviousPipelineAction;

  private transient PartitionReconciler partitionReconciler;
  private transient OrphanedMetadataCleaner orphanedMetadataCleaner;

  public DetectNewPartitionsAction(
      ChangeStreamMetrics metrics,
      MetadataTableDao metadataTableDao,
      @Nullable Instant endTime,
      GenerateInitialPartitionsAction generateInitialPartitionsAction,
      ResumeFromPreviousPipelineAction resumeFromPreviousPipelineAction,
      ProcessNewPartitionsAction processNewPartitionsAction) {
    this.metrics = metrics;
    this.metadataTableDao = metadataTableDao;
    this.endTime = endTime;
    this.generateInitialPartitionsAction = generateInitialPartitionsAction;
    this.resumeFromPreviousPipelineAction = resumeFromPreviousPipelineAction;
    this.processNewPartitionsAction = processNewPartitionsAction;
  }

  /**
   * Get the new watermark based on the watermark of StreamPartitions and NewPartitions.
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
   * @param streamPartitionsWithWatermark restriction tracker to guide how frequently watermark
   *     should be advanced
   * @param newPartitions watermark estimator to advance the watermark
   * @return low watermark if possible, otherwise empty.
   */
  private Optional<Instant> getNewWatermark(
      List<StreamPartitionWithWatermark> streamPartitionsWithWatermark,
      List<NewPartition> newPartitions) {
    // Get partitions with a watermark set but skip rows w a lock and no watermark yet
    List<StreamPartitionWithWatermark> slowPartitions = new ArrayList<>();
    Instant lowWatermark = Instant.ofEpochMilli(Long.MAX_VALUE);

    List<ByteStringRange> partitions = new ArrayList<>();
    for (StreamPartitionWithWatermark streamPartitionWithWatermark :
        streamPartitionsWithWatermark) {
      if (streamPartitionWithWatermark.getWatermark().plus(DEBUG_WATERMARK_DELAY).isBeforeNow()) {
        slowPartitions.add(streamPartitionWithWatermark);
      }
      if (streamPartitionWithWatermark.getWatermark().compareTo(lowWatermark) < 0) {
        lowWatermark = streamPartitionWithWatermark.getWatermark();
      }
      partitions.add(streamPartitionWithWatermark.getPartition());
    }
    if (!slowPartitions.isEmpty()) {
      LOG.warn(
          "DNP: Updating watermark is held back by {} partitions : {}",
          slowPartitions.size(),
          slowPartitions.stream()
              .map(e -> formatByteStringRange(e.getPartition()) + " => " + e.getWatermark())
              .collect(Collectors.joining(", ", "{", "}")));
    }

    // Only added StreamPartitions so far, check if there are any overlapping StreamPartitions. If
    // so, something is wrong, we should stop.
    List<ByteStringRange> overlappingStreamPartitions = getOverlappingPartitions(partitions);
    if (!overlappingStreamPartitions.isEmpty()) {
      LOG.warn(
          "DNP: Updating watermark failed due to overlapping: {}",
          partitionsToString(overlappingStreamPartitions));
      return Optional.empty();
    }

    for (NewPartition newPartition : newPartitions) {
      partitions.addAll(newPartition.getParentPartitions());
      if (newPartition.getLowWatermark().compareTo(lowWatermark) < 0) {
        lowWatermark = newPartition.getLowWatermark();
      }
    }

    List<ByteStringRange> missingPartitions = getMissingPartitionsFromEntireKeySpace(partitions);
    if (missingPartitions.isEmpty()) {
      LOG.info("DNP: Updating watermark: " + lowWatermark);
      return Optional.of(lowWatermark);
    }
    LOG.warn(
        "DNP: Updating watermark failed due to missing {} partitions : {}.",
        missingPartitions.size(),
        partitionsToString(missingPartitions));
    return Optional.empty();
  }

  /**
   * Uses PartitionReconciler to process any partitions that it has found to be missing for too long
   * and restarts them. For more details on why this is necessary see {@link PartitionReconciler}
   *
   * @param receiver used to output reconciled partitions
   * @param watermarkEstimator read the low watermark for all partitions
   */
  private void processReconcilerPartitions(
      OutputReceiver<PartitionRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      Instant startTime) {
    for (PartitionRecord reconciledPartition :
        partitionReconciler.getPartitionsToReconcile(
            watermarkEstimator.currentWatermark(), startTime)) {
      // Set the missing fields.
      reconciledPartition.setUuid(UniqueIdGenerator.getNextId());
      reconciledPartition.setEndTime(endTime);

      for (NewPartition parentPartition : reconciledPartition.getParentPartitions()) {
        metadataTableDao.markNewPartitionForDeletion(parentPartition);
      }
      receiver.outputWithTimestamp(reconciledPartition, Instant.EPOCH);
      LOG.warn("DNP: Reconciling missing partition: {}", reconciledPartition);
    }
  }

  private void cleanUpOrphanedMetadata() {
    for (NewPartition newPartitionToClean : orphanedMetadataCleaner.getOrphanedNewPartitions()) {
      metrics.incOrphanedNewPartitionCleanedCount();
      metadataTableDao.markNewPartitionForDeletion(newPartitionToClean);
      metadataTableDao.deleteNewPartition(newPartitionToClean);
    }
  }

  /**
   * Return true if watermark should be updated.
   *
   * <p>We update the watermark on even count after 10s since last update.
   *
   * <p>We chose to only update on even count, so we don't perform a full table scan on every DNP
   * run if DNP taking longer than 10s between each run.
   *
   * <p>We set a minimum of 10s to minimize frequency of full table scan. This means the watermark
   * will always lag behind by 10s. We didn't choose a larger minimum frequency because that would
   * increase the minimum watermark delay.
   */
  private boolean shouldUpdateWatermark(
      long count, @Nullable DetectNewPartitionsState detectNewPartitionsState) {
    return count % 2 == 0
        && (detectNewPartitionsState == null
            || detectNewPartitionsState
                .getWatermarkLastUpdated()
                .plus(Duration.standardSeconds(10))
                .isBeforeNow());
  }

  /**
   * Perform the necessary steps to manage initial set of partitions and new partitions. Currently,
   * we set to process new partitions every second.
   *
   * <ol>
   *   <li>Look up the initial list of partitions to stream if it's the very first run.
   *   <li>On rest of the runs, try advancing watermark if needed.
   *   <li>Update the metadata table with info about this DoFn.
   *   <li>Check if this pipeline has reached the end time. Terminate if it has.
   *   <li>Process new partitions and output them.
   *   <li>Reconcile any Partitions that haven't been streaming for a long time
   *   <li>Register callback to clean up processed partitions after bundle has been finalized.
   * </ol>
   *
   * @param tracker offset tracker that simply increment by 1 every single run
   * @param receiver output new partitions
   * @param watermarkEstimator update watermark that is a representation of the low watermark of the
   *     entire beam pipeline
   * @return {@link ProcessContinuation#resume()} with 1-second delay if the stream continues,
   *     otherwise {@link ProcessContinuation#stop()}
   * @throws InvalidProtocolBufferException if failing to process new partitions
   */
  @VisibleForTesting
  public ProcessContinuation run(
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<PartitionRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      InitialPipelineState initialPipelineState)
      throws Exception {
    LOG.debug("DNP: Watermark: " + watermarkEstimator.getState());
    LOG.debug("DNP: CurrentTracker: " + tracker.currentRestriction().getFrom());
    if (tracker.currentRestriction().getFrom() == 0L) {
      if (!tracker.tryClaim(0L)) {
        LOG.error(
            "Could not claim initial DetectNewPartition restriction. No partitions are outputted.");
        return ProcessContinuation.stop();
      }
      watermarkEstimator.setWatermark(initialPipelineState.getStartTime());
      if (initialPipelineState.isResume()) {
        resumeFromPreviousPipelineAction.run(receiver);
      } else {
        generateInitialPartitionsAction.run(receiver, initialPipelineState.getStartTime());
      }
      return ProcessContinuation.resume();
    }

    // Create a new partition reconciler every run to reset the state each time.
    partitionReconciler = new PartitionReconciler(metadataTableDao, metrics);
    orphanedMetadataCleaner = new OrphanedMetadataCleaner();

    // Calculating the new value of watermark is a resource intensive process. We have to do a full
    // scan of the metadata table and then ensure we're not missing partitions and then calculate
    // the low watermark. This is usually a fairly fast process even with thousands of partitions.
    // However, sometimes this may take so long that the runner checkpoints before the watermark is
    // calculated. Because the checkpoint takes place before tryClaim, this forces the DoFn to
    // restart, wasting the resources spent calculating the watermark. On restart, we will try to
    // calculate the watermark again. The problem causing the slow watermark calculation can persist
    // leading to a crash loop. In order to ensure we persist the calculated watermark, we calculate
    // the watermark after successful tryClaim. Then we write to the metadata table the new
    // watermark. On the start of each run we read the watermark and update the DoFn's watermark.
    DetectNewPartitionsState detectNewPartitionsState =
        metadataTableDao.readDetectNewPartitionsState();
    if (detectNewPartitionsState != null) {
      watermarkEstimator.setWatermark(detectNewPartitionsState.getWatermark());
    }

    // Terminate if endTime <= watermark that means all partitions have read up to or beyond
    // watermark. We no longer need to manage splits and merges, we can terminate.
    if (endTime != null && !watermarkEstimator.currentWatermark().isBefore(endTime)) {
      tracker.tryClaim(tracker.currentRestriction().getTo());
      return ProcessContinuation.stop();
    }

    if (!tracker.tryClaim(tracker.currentRestriction().getFrom())) {
      LOG.warn("DNP: Checkpointing, stopping this run: " + tracker.currentRestriction());
      return ProcessContinuation.stop();
    }

    // Read StreamPartitions to calculate watermark.
    List<StreamPartitionWithWatermark> streamPartitionsWithWatermark = null;
    if (shouldUpdateWatermark(tracker.currentRestriction().getFrom(), detectNewPartitionsState)) {
      streamPartitionsWithWatermark = metadataTableDao.readStreamPartitionsWithWatermark();
    }

    // Process NewPartitions and track the ones successfully outputted.
    List<NewPartition> newPartitions = metadataTableDao.readNewPartitions();
    List<ByteStringRange> outputtedNewPartitions = new ArrayList<>();
    for (NewPartition newPartition : newPartitions) {
      if (processNewPartitionsAction.processNewPartition(newPartition, receiver)) {
        outputtedNewPartitions.add(newPartition.getPartition());
      } else if (streamPartitionsWithWatermark != null) {
        // streamPartitionsWithWatermark is not null on runs that we update watermark. We only run
        // reconciliation when we update watermark. Only add incompleteNewPartitions if
        // reconciliation is being run
        partitionReconciler.addIncompleteNewPartitions(newPartition);
        orphanedMetadataCleaner.addIncompleteNewPartitions(newPartition);
      }
    }

    // Process the watermark using read StreamPartitions and NewPartitions.
    if (streamPartitionsWithWatermark != null) {
      Optional<Instant> maybeWatermark =
          getNewWatermark(streamPartitionsWithWatermark, newPartitions);
      maybeWatermark.ifPresent(metadataTableDao::updateDetectNewPartitionWatermark);
      // Only start reconciling after the pipeline has been running for a while.
      if (tracker.currentRestriction().getFrom() > 50) {
        // Using NewPartitions and StreamPartitions, evaluate partitions that are possibly not being
        // streamed. This isn't perfect because there may be partitions moving between
        // StreamPartitions and NewPartitions while scanning the metadata table. Also, this does not
        // include NewPartitions marked as deleted from a previous DNP run not yet processed by
        // RCSP.
        List<ByteStringRange> existingPartitions =
            streamPartitionsWithWatermark.stream()
                .map(StreamPartitionWithWatermark::getPartition)
                .collect(Collectors.toList());
        existingPartitions.addAll(outputtedNewPartitions);
        List<ByteStringRange> missingStreamPartitions =
            getMissingPartitionsFromEntireKeySpace(existingPartitions);
        orphanedMetadataCleaner.addMissingPartitions(missingStreamPartitions);
        partitionReconciler.addMissingPartitions(missingStreamPartitions);
        processReconcilerPartitions(
            receiver, watermarkEstimator, initialPipelineState.getStartTime());
        cleanUpOrphanedMetadata();
      }
    }

    return ProcessContinuation.resume().withResumeDelay(Duration.millis(100));
  }
}
