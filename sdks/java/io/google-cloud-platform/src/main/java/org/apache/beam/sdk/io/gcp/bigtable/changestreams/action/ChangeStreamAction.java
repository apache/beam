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
import static org.apache.beam.sdk.io.gcp.bigtable.changestreams.TimestampConverter.toJodaTime;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamContinuationToken;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamMutation;
import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecord;
import com.google.cloud.bigtable.data.v2.models.CloseStream;
import com.google.cloud.bigtable.data.v2.models.Heartbeat;
import com.google.cloud.bigtable.data.v2.models.Range;
import com.google.protobuf.ByteString;
import java.util.Optional;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.estimator.BytesThroughputEstimator;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.model.PartitionRecord;
import org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.StreamProgress;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This class is responsible for processing individual ChangeStreamRecord. */
@Internal
public class ChangeStreamAction {
  private static final Logger LOG = LoggerFactory.getLogger(ChangeStreamAction.class);
  private final ChangeStreamMetrics metrics;

  /**
   * Constructs ChangeStreamAction to process individual ChangeStreamRecord.
   *
   * @param metrics record beam metrics.
   */
  public ChangeStreamAction(ChangeStreamMetrics metrics) {
    this.metrics = metrics;
  }

  /**
   * This class processes ReadChangeStreamResponse from bigtable server. There are 3 possible
   * response types, Heartbeat, ChangeStreamMutation, CloseStream.
   *
   * <ul>
   *   <li>Heartbeat happens periodically based on the initial configuration set at the start of the
   *       beam pipeline. Heartbeat can advance the watermark forward and includes a continuation
   *       token that provides a point to resume from after a checkpoint.
   *   <li>ChangeStreamMutation includes the actual mutation that took place in the Bigtable.
   *       ChangeStreamMutation also includes watermark and continuation token. All
   *       ChangeStreamMutation are emitted to the outputReceiver with the timestamp of 0 (instead
   *       of the commit timestamp). Setting the timestamp to 0 discourages the use of windowing on
   *       this connector. All ChangeStreamMutations will be late data when windowing. This design
   *       decision prefers availability over consistency in the event that partitions are streamed
   *       slowly (due to an outages or other unavailabilities) the commit timestamp which drives
   *       the watermark may lag behind preventing windows from closing.
   *   <li>CloseStream indicates that the stream has come to an end. The CloseStream is not
   *       processed but stored in the RestrictionTracker to be processed later. This ensures that
   *       we successfully commit all pending ChangeStreamMutations.
   * </ul>
   *
   * CloseStream is the only response that type will initiate a resume. Other response type will
   * simply process the response and return empty. Returning empty signals to caller that we have
   * processed the response, and it does not require any additional action.
   *
   * <p>There are 2 cases that cause this function to return a non-empty ProcessContinuation.
   *
   * <ol>
   *   <li>We fail to claim a StreamProgress. This can happen for a runner-initiated checkpoint.
   *       When the runner initiates a checkpoint, we will stop and checkpoint pending
   *       ChangeStreamMutations and resume from the previous StreamProgress.
   *   <li>The response is a CloseStream. RestrictionTracker claims the CloseStream. We don't do any
   *       additional processing of the response. We return resume to signal to the caller that to
   *       checkpoint all pending ChangeStreamMutations. We expect the caller to check the
   *       RestrictionTracker includes a CloseStream and process it to close the stream.
   * </ol>
   *
   * @param partitionRecord the stream partition that generated the response
   * @param record the change stream record to be processed
   * @param tracker restrictionTracker that we use to claim next block and also to store CloseStream
   * @param receiver to output DataChange
   * @param watermarkEstimator manually progress watermark when processing responses with watermark
   * @return Optional.of(ProcessContinuation) if the run should be stopped or resumed, otherwise
   *     Optional.empty() to do nothing.
   */
  public Optional<DoFn.ProcessContinuation> run(
      PartitionRecord partitionRecord,
      ChangeStreamRecord record,
      RestrictionTracker<StreamProgress, StreamProgress> tracker,
      DoFn.OutputReceiver<KV<ByteString, ChangeStreamRecord>> receiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator,
      BytesThroughputEstimator<KV<ByteString, ChangeStreamRecord>> throughputEstimator) {
    if (record instanceof Heartbeat) {
      Heartbeat heartbeat = (Heartbeat) record;
      final Instant watermark = toJodaTime(heartbeat.getEstimatedLowWatermark());

      // These will be filtered so the key doesn't really matter but the most logical thing to
      // key a heartbeat by is the partition it corresponds to.
      ByteString heartbeatKey =
          Range.ByteStringRange.serializeToByteString(partitionRecord.getPartition());
      KV<ByteString, ChangeStreamRecord> outputRecord = KV.of(heartbeatKey, heartbeat);
      throughputEstimator.update(Instant.now(), outputRecord);
      StreamProgress streamProgress =
          new StreamProgress(
              heartbeat.getChangeStreamContinuationToken(),
              watermark,
              throughputEstimator.get(),
              Instant.now(),
              true);
      watermarkEstimator.setWatermark(watermark);

      // If the tracker fail to claim the streamProgress, it most likely means the runner initiated
      // a checkpoint. See {@link
      // org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.ReadChangeStreamPartitionProgressTracker}
      // for more information regarding runner initiated checkpoints.
      if (!tracker.tryClaim(streamProgress)) {
        return Optional.of(DoFn.ProcessContinuation.stop());
      }
      metrics.incHeartbeatCount();
      // We output heartbeats so that they are factored into throughput and can be used to
      // autoscale. These will be filtered in a downstream step and never returned to users. This is
      // to prevent autoscaler from scaling down when we have large tables with no throughput but
      // we need enough workers to keep up with heartbeats.

      // We are outputting elements with timestamp of 0 to prevent reliance on event time. This
      // limits the ability to window on commit time of any data changes. It is still possible to
      // window on processing time.
      receiver.outputWithTimestamp(outputRecord, Instant.EPOCH);
    } else if (record instanceof CloseStream) {
      CloseStream closeStream = (CloseStream) record;
      StreamProgress streamProgress = new StreamProgress(closeStream);

      // If the tracker fail to claim the streamProgress, it most likely means the runner initiated
      // a checkpoint. See {@link
      // org.apache.beam.sdk.io.gcp.bigtable.changestreams.restriction.ReadChangeStreamPartitionProgressTracker}
      // for more information regarding runner initiated checkpoints.
      if (!tracker.tryClaim(streamProgress)) {
        return Optional.of(DoFn.ProcessContinuation.stop());
      }
      metrics.incClosestreamCount();
      return Optional.of(DoFn.ProcessContinuation.resume());
    } else if (record instanceof ChangeStreamMutation) {
      ChangeStreamMutation changeStreamMutation = (ChangeStreamMutation) record;
      final Instant watermark = toJodaTime(changeStreamMutation.getEstimatedLowWatermark());
      watermarkEstimator.setWatermark(watermark);
      // Build a new StreamProgress with the continuation token to be claimed.
      ChangeStreamContinuationToken changeStreamContinuationToken =
          ChangeStreamContinuationToken.create(
              Range.ByteStringRange.create(
                  partitionRecord.getPartition().getStart(),
                  partitionRecord.getPartition().getEnd()),
              changeStreamMutation.getToken());

      KV<ByteString, ChangeStreamRecord> outputRecord =
          KV.of(changeStreamMutation.getRowKey(), changeStreamMutation);
      throughputEstimator.update(Instant.now(), outputRecord);
      StreamProgress streamProgress =
          new StreamProgress(
              changeStreamContinuationToken,
              watermark,
              throughputEstimator.get(),
              Instant.now(),
              false);
      // If the tracker fail to claim the streamProgress, it most likely means the runner initiated
      // a checkpoint. See ReadChangeStreamPartitionProgressTracker for more information regarding
      // runner initiated checkpoints.
      if (!tracker.tryClaim(streamProgress)) {
        return Optional.of(DoFn.ProcessContinuation.stop());
      }
      if (changeStreamMutation.getType() == ChangeStreamMutation.MutationType.GARBAGE_COLLECTION) {
        metrics.incChangeStreamMutationGcCounter();
      } else if (changeStreamMutation.getType() == ChangeStreamMutation.MutationType.USER) {
        metrics.incChangeStreamMutationUserCounter();
      }
      Instant delay = toJodaTime(changeStreamMutation.getCommitTimestamp());
      metrics.updateProcessingDelayFromCommitTimestamp(
          Instant.now().getMillis() - delay.getMillis());

      // We are outputting elements with timestamp of 0 to prevent reliance on event time. This
      // limits the ability to window on commit time of any data changes. It is still possible to
      // window on processing time.
      receiver.outputWithTimestamp(outputRecord, Instant.EPOCH);
    } else {
      LOG.warn(
          "RCSP {}: Invalid response type", formatByteStringRange(partitionRecord.getPartition()));
    }
    return Optional.empty();
  }
}
