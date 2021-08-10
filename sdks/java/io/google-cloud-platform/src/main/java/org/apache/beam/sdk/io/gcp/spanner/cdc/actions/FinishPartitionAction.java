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
package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.PARTITIONS_RUNNING_COUNTER;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.PARTITION_RUNNING_TO_FINISHED_MS;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao.TransactionResult;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class FinishPartitionAction {

  private static final Logger LOG = LoggerFactory.getLogger(FinishPartitionAction.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private final PartitionMetadataDao partitionMetadataDao;

  public FinishPartitionAction(PartitionMetadataDao partitionMetadataDao) {
    this.partitionMetadataDao = partitionMetadataDao;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    try (Scope scope =
        TRACER.spanBuilder("FinishPartitionAction").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL,
              AttributeValue.stringAttributeValue(partition.getPartitionToken()));

      final String token = partition.getPartitionToken();
      LOG.debug("[" + token + "] Finishing partition");

      if (!tracker.tryClaim(PartitionPosition.finishPartition())) {
        LOG.info("[" + token + "] Could not claim finishPartition(), stopping");
        return Optional.of(ProcessContinuation.stop());
      }

      try {
        updateStateToFinished(token);
      } catch (SpannerException e) {
        if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
          LOG.info("[" + token + "] Partition does not exist, skipping");
        } else {
          throw e;
        }
      }

      LOG.debug("[" + token + "] Finish partition action completed successfully");
      return Optional.empty();
    }
  }

  private void updateStateToFinished(String token) {
    final TransactionResult<PartitionMetadata> result =
        partitionMetadataDao.runInTransaction(
            transaction -> {
              transaction.updateToFinished(token);
              return transaction.getPartition(token);
            });
    final PartitionMetadata updatedPartition = result.getResult();
    final Timestamp runningAt = updatedPartition.getRunningAt();
    final Timestamp finishedAt = result.getCommitTimestamp();
    PARTITIONS_RUNNING_COUNTER.dec();
    PARTITION_RUNNING_TO_FINISHED_MS.update(
        new Duration(runningAt.toSqlTimestamp().getTime(), finishedAt.toSqlTimestamp().getTime())
            .getMillis());
  }
}
