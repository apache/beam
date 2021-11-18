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
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State.CREATED;

import com.google.cloud.Timestamp;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.TimestampConverter;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class ChildPartitionsRecordAction {

  private static final Logger LOG = LoggerFactory.getLogger(ChildPartitionsRecordAction.class);
  private static final Tracer TRACER = Tracing.getTracer();
  private final PartitionMetadataDao partitionMetadataDao;
  private final ChangeStreamMetrics metrics;

  public ChildPartitionsRecordAction(
      PartitionMetadataDao partitionMetadataDao, ChangeStreamMetrics metrics) {
    this.partitionMetadataDao = partitionMetadataDao;
    this.metrics = metrics;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      ChildPartitionsRecord record,
      RestrictionTracker<OffsetRange, Long> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator) {

    final String token = partition.getPartitionToken();
    try (Scope scope =
        TRACER.spanBuilder("ChildPartitionsRecordAction").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(PARTITION_ID_ATTRIBUTE_LABEL, AttributeValue.stringAttributeValue(token));

      LOG.debug("[" + token + "] Processing child partition record " + record);

      final Timestamp startTimestamp = record.getStartTimestamp();
      final Instant startInstant = new Instant(startTimestamp.toSqlTimestamp().getTime());
      final long startMicros = TimestampConverter.timestampToMicros(startTimestamp);
      if (!tracker.tryClaim(startMicros)) {
        LOG.debug(
            "[" + token + "] Could not claim queryChangeStream(" + startTimestamp + "), stopping");
        return Optional.of(ProcessContinuation.stop());
      }
      watermarkEstimator.setWatermark(startInstant);

      for (ChildPartition childPartition : record.getChildPartitions()) {
        processChildPartition(partition, record, childPartition);
      }

      LOG.debug("[" + token + "] Child partitions action completed successfully");
      return Optional.empty();
    }
  }

  private void processChildPartition(
      PartitionMetadata partition, ChildPartitionsRecord record, ChildPartition childPartition) {

    try (Scope scope =
        TRACER
            .spanBuilder("ChildPartitionsRecordAction.processChildPartition")
            .setRecordEvents(true)
            .startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL,
              AttributeValue.stringAttributeValue(partition.getPartitionToken()));

      final String partitionToken = partition.getPartitionToken();
      final String childPartitionToken = childPartition.getToken();
      final boolean isSplit = isSplit(childPartition);
      LOG.debug(
          "["
              + partitionToken
              + "] Processing child partition"
              + (isSplit ? " split" : " merge")
              + " event");

      final PartitionMetadata row =
          toPartitionMetadata(
              record.getStartTimestamp(),
              partition.getEndTimestamp(),
              partition.getHeartbeatMillis(),
              childPartition);
      // FIXME: Figure out what to do if this throws an exception
      LOG.debug("[" + partitionToken + "] Inserting child partition token " + childPartitionToken);
      final Boolean insertedRow =
          partitionMetadataDao
              .runInTransaction(
                  transaction -> {
                    if (transaction.getPartition(childPartitionToken) == null) {
                      transaction.insert(row);
                      return true;
                    } else {
                      return false;
                    }
                  })
              .getResult();
      if (insertedRow && isSplit) {
        metrics.incPartitionRecordSplitCount();
      } else if (insertedRow) {
        metrics.incPartitionRecordMergeCount();
      } else {
        LOG.debug(
            "["
                + partitionToken
                + "] Child token "
                + childPartitionToken
                + " already exists, skipping...");
      }
    }
  }

  private boolean isSplit(ChildPartition childPartition) {
    return childPartition.getParentTokens().size() == 1;
  }

  private PartitionMetadata toPartitionMetadata(
      Timestamp startTimestamp,
      Timestamp endTimestamp,
      long heartbeatMillis,
      ChildPartition childPartition) {
    // FIXME: The backend only supports microsecond granularity. Remove when fixed.
    final Timestamp truncatedStartTimestamp = TimestampConverter.truncateNanos(startTimestamp);
    final Timestamp truncatedEndTimestamp =
        Optional.ofNullable(endTimestamp).map(TimestampConverter::truncateNanos).orElse(null);
    return PartitionMetadata.newBuilder()
        .setPartitionToken(childPartition.getToken())
        .setParentTokens(childPartition.getParentTokens())
        .setStartTimestamp(truncatedStartTimestamp)
        .setEndTimestamp(truncatedEndTimestamp)
        .setHeartbeatMillis(heartbeatMillis)
        .setState(CREATED)
        .build();
  }
}
