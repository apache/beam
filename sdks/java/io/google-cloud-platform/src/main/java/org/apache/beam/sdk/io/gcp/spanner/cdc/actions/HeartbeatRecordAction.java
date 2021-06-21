/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.gcp.spanner.cdc.actions;

import com.google.cloud.Timestamp;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class HeartbeatRecordAction {
  private static final Logger LOG = LoggerFactory.getLogger(HeartbeatRecordAction.class);

  public Optional<ProcessContinuation> run(
      HeartbeatRecord record,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      ManualWatermarkEstimator<Instant> watermarkEstimator
  ) {
    LOG.debug("Processing heartbeat record " + record);

    final Timestamp timestamp = record.getTimestamp();
    if (!tracker.tryClaim(PartitionPosition.queryChangeStream(timestamp))) {
      LOG.debug("Could not claim, stopping");
      return Optional.of(ProcessContinuation.stop());
    }
    watermarkEstimator.setWatermark(new Instant(timestamp.toSqlTimestamp().getTime()));

    LOG.debug("Heartbeat record action completed successfully");
    return Optional.empty();
  }
}
