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

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.ChangeStreamMetrics;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class UpdateStateAction {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateStateAction.class);

  private final PartitionMetadataDao partitionMetadataDao;
  private final ChangeStreamMetrics metrics;

  public UpdateStateAction(PartitionMetadataDao partitionMetadataDao, ChangeStreamMetrics metrics) {
    this.partitionMetadataDao = partitionMetadataDao;
    this.metrics = metrics;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    final String token = partition.getPartitionToken();
    LOG.info("[" + token + "] Updating state of partition to be running");

    if (!tracker.tryClaim(PartitionPosition.updateState())) {
      LOG.info("[" + token + "] Could not claim updateState(), stopping");
      return Optional.of(ProcessContinuation.stop());
    }

    try {
      final com.google.cloud.Timestamp partitionScheduledAt = partition.getScheduledAt();
      final com.google.cloud.Timestamp partitionRunningAt =
          partitionMetadataDao.updateToRunning(token);

      LOG.info("[" + token + "] Finished updating state of partition to be running");
      if (partitionScheduledAt != null && partitionRunningAt != null) {
        metrics.updatePartitionScheduledToRunning(
            new Duration(
                partitionScheduledAt.toSqlTimestamp().getTime(),
                partitionRunningAt.toSqlTimestamp().getTime()));
      }
    } catch (SpannerException e) {
      if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
        LOG.debug("[" + token + "] Partition does not exist, skipping");
      } else {
        throw e;
      }
    }

    LOG.debug("[" + token + "] Finish partition action completed successfully");
    return Optional.empty();
  }
}
