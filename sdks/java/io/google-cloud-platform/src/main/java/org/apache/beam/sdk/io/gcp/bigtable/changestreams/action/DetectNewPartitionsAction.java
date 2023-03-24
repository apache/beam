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
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper.partitionsToString;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ByteStringRangeHelper;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.encoder.MetadataTableEncoder;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.BundleFinalizer;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
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
        Range.ByteStringRange partition =
            metadataTableDao.convertStreamPartitionRowKeyToPartition(row.getKey());
        partitions.add(partition);
        if (watermark.plus(DEBUG_WATERMARK_DELAY).isBeforeNow()) {
          slowPartitions.put(partition, watermark);
        }
      }
      List<Range.ByteStringRange> missingAndOverlappingPartitions =
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
    }
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

    advanceWatermark(tracker, watermarkEstimator);

    return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
  }
}
