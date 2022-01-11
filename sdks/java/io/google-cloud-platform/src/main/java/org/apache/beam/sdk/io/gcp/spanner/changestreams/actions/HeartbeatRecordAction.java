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
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.TimestampConverter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is part of the process for {@link
 * org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF. It is
 * responsible for processing {@link HeartbeatRecord}s. The records will be used to progress the
 * watermark for the current element (partition).
 */
public class HeartbeatRecordAction {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatRecordAction.class);
  private static final Tracer TRACER = Tracing.getTracer();
  private final ChangeStreamMetrics metrics;

  /**
   * Constructs an action class for handling {@link HeartbeatRecord}s.
   *
   * @param metrics metrics gathering class
   */
  HeartbeatRecordAction(ChangeStreamMetrics metrics) {
    this.metrics = metrics;
  }

  /**
   * This is the main processing function for a {@link HeartbeatRecord}. It returns an {@link
   * Optional} of {@link ProcessContinuation} to indicate if the calling function should stop or
   * not. If the {@link Optional} returned is empty, it means that the calling function can continue
   * with the processing. If an {@link Optional} of {@link ProcessContinuation#stop()} is returned,
   * it means that this function was unable to claim the timestamp of the {@link HeartbeatRecord},
   * so the caller should stop.
   *
   * <p>When processing the {@link HeartbeatRecord} the following procedure is applied:
   *
   * <ol>
   *   <li>We try to claim the heartbeat record timestamp. If it is not possible, we stop here and
   *       return.
   *   <li>We update the necessary metrics.
   *   <li>We update the watermark to the heartbeat record timestamp.
   * </ol>
   */
  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      HeartbeatRecord record,
      RestrictionTracker<OffsetRange, Long> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    try (Scope scope =
        TRACER.spanBuilder("HeartbeatRecordAction").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL,
              AttributeValue.stringAttributeValue(partition.getPartitionToken()));

      final String token = partition.getPartitionToken();
      LOG.debug("[" + token + "] Processing heartbeat record " + record);

      final Timestamp timestamp = record.getTimestamp();
      final Instant timestampInstant = new Instant(timestamp.toSqlTimestamp().getTime());
      final long timestampMicros = TimestampConverter.timestampToMicros(timestamp);
      if (!tracker.tryClaim(timestampMicros)) {
        LOG.debug("[" + token + "] Could not claim queryChangeStream(" + timestamp + "), stopping");
        return Optional.of(ProcessContinuation.stop());
      }
      metrics.incHeartbeatRecordCount();
      watermarkEstimator.setWatermark(timestampInstant);

      LOG.debug("[" + token + "] Heartbeat record action completed successfully");
      return Optional.empty();
    }
  }
}
