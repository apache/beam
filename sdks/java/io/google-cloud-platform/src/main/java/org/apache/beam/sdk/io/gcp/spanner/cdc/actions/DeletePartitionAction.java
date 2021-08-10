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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.ChangeStreamMetrics.PARTITION_ID_ATTRIBUTE_LABEL;

import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class DeletePartitionAction {

  private static final Logger LOG = LoggerFactory.getLogger(DeletePartitionAction.class);
  private static final Tracer TRACER = Tracing.getTracer();

  private final PartitionMetadataDao partitionMetadataDao;

  public DeletePartitionAction(PartitionMetadataDao partitionMetadataDao) {
    this.partitionMetadataDao = partitionMetadataDao;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    try (Scope scope =
        TRACER.spanBuilder("DeletePartitionAction").setRecordEvents(true).startScopedSpan()) {
      TRACER
          .getCurrentSpan()
          .putAttribute(
              PARTITION_ID_ATTRIBUTE_LABEL,
              AttributeValue.stringAttributeValue(partition.getPartitionToken()));

      final String token = partition.getPartitionToken();
      LOG.debug("[" + token + "] Deleting partition");

      if (!tracker.tryClaim(PartitionPosition.deletePartition())) {
        LOG.debug("[" + token + "] Could not claim deletePartition(), stopping");
        return Optional.of(ProcessContinuation.stop());
      }
      partitionMetadataDao.delete(token);

      LOG.debug("[" + token + "] Delete partition action completed successfully");
      return Optional.empty();
    }
  }
}
