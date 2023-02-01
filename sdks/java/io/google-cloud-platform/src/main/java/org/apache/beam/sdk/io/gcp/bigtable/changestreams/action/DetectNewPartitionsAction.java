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

import com.google.cloud.Timestamp;
import com.google.protobuf.InvalidProtocolBufferException;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.dao.MetadataTableDao;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
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
public class DetectNewPartitionsAction {
  private static final Logger LOG = LoggerFactory.getLogger(DetectNewPartitionsAction.class);

  private static final Duration DEBUG_WATERMARK_DELAY = Duration.standardMinutes(5);

  private final ChangeStreamMetrics metrics;
  private final MetadataTableDao metadataTableDao;
  @Nullable private final com.google.cloud.Timestamp endTime;
  private final GenerateInitialPartitionsAction generateInitialPartitionsAction;

  public DetectNewPartitionsAction(
      ChangeStreamMetrics metrics,
      MetadataTableDao metadataTableDao,
      @Nullable Timestamp endTime,
      GenerateInitialPartitionsAction generateInitialPartitionsAction) {
    this.metrics = metrics;
    this.metadataTableDao = metadataTableDao;
    this.endTime = endTime;
    this.generateInitialPartitionsAction = generateInitialPartitionsAction;
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
      DoFn.OutputReceiver<PartitionRecord> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      DoFn.BundleFinalizer bundleFinalizer,
      Timestamp startTime)
      throws Exception {

    // Terminate if endTime <= watermark that means all partitions have read up to or beyond
    // watermark. We no longer need to manage splits and merges, we can terminate.
    if (endTime != null
        && endTime.compareTo(
                TimestampConverter.toCloudTimestamp(watermarkEstimator.currentWatermark()))
            <= 0) {
      tracker.tryClaim(tracker.currentRestriction().getTo());
      return ProcessContinuation.stop();
    }

    if (!tracker.tryClaim(tracker.currentRestriction().getFrom())) {
      return ProcessContinuation.stop();
    }

    return ProcessContinuation.resume().withResumeDelay(Duration.standardSeconds(1));
  }
}
