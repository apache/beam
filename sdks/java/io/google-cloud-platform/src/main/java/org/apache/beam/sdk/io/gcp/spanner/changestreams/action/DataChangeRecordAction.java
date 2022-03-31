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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.action;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import com.google.cloud.Timestamp;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampRange;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
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
 * responsible for processing {@link DataChangeRecord}s. The records will simply be emitted to the
 * received output receiver.
 */
public class DataChangeRecordAction {
  private static final Logger LOG = LoggerFactory.getLogger(DataChangeRecordAction.class);
  private static final Tracer TRACER = Tracing.getTracer();

  /**
   * This is the main processing function for a {@link DataChangeRecord}. It returns an {@link
   * Optional} of {@link ProcessContinuation} to indicate if the calling function should stop or
   * not. If the {@link Optional} returned is empty, it means that the calling function can continue
   * with the processing. If an {@link Optional} of {@link ProcessContinuation#stop()} is returned,
   * it means that this function was unable to claim the timestamp of the {@link DataChangeRecord},
   * so the caller should stop.
   *
   * <p>When processing the {@link DataChangeRecord} the following procedure is applied:
   *
   * <ol>
   *   <li>We try to cliam the data change record commit timestamp. If it is not possible, we stop
   *       here and return.
   *   <li>We emit the data change record through the {@link OutputReceiver}.
   *   <li>We update the watermark to the data change record commit timestamp.
   * </ol>
   *
   * @param partition the current partition being processed
   * @param record the change stream data record received
   * @param tracker the restriction tracker of the {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   * @param outputReceiver the output receiver of the {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   * @param watermarkEstimator the watermark estimator of the {@link
   *     org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF
   * @return {@link Optional#empty()} if the caller can continue processing more records. A non
   *     empty {@link Optional} with {@link ProcessContinuation#stop()} if this function was unable
   *     to claim the {@link ChildPartitionsRecord} timestamp
   */
  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      DataChangeRecord record,
      RestrictionTracker<TimestampRange, Timestamp> tracker,
      OutputReceiver<DataChangeRecord> outputReceiver,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    try (Scope scope =
        TRACER.spanBuilder("DataChangeRecordAction").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL,
              AttributeValue.stringAttributeValue(partition.getPartitionToken()));

      final String token = partition.getPartitionToken();
      LOG.debug("[" + token + "] Processing data record " + record.getCommitTimestamp());

      final Timestamp commitTimestamp = record.getCommitTimestamp();
      final Instant commitInstant = new Instant(commitTimestamp.toSqlTimestamp().getTime());
      if (!tracker.tryClaim(commitTimestamp)) {
        LOG.debug(
            "[" + token + "] Could not claim queryChangeStream(" + commitTimestamp + "), stopping");
        return Optional.of(ProcessContinuation.stop());
      }
      outputReceiver.outputWithTimestamp(record, commitInstant);
      watermarkEstimator.setWatermark(commitInstant);

      LOG.debug("[" + token + "] Data record action completed successfully");
      return Optional.empty();
    }
  }
}
