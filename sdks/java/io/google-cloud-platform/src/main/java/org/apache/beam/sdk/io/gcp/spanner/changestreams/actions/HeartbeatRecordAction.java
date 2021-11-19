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

// TODO: Add java docs
public class HeartbeatRecordAction {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatRecordAction.class);
  private static final Tracer TRACER = Tracing.getTracer();
  private final ChangeStreamMetrics metrics;

  HeartbeatRecordAction(ChangeStreamMetrics metrics) {
    this.metrics = metrics;
  }

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
      metrics.incHearbeatRecordCount();
      watermarkEstimator.setWatermark(timestampInstant);

      LOG.debug("[" + token + "] Heartbeat record action completed successfully");
      return Optional.empty();
    }
  }
}
