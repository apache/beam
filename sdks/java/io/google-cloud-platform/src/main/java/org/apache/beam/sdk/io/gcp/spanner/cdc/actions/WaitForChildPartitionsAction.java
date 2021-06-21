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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.FINISHED;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.SCHEDULED;

import java.util.Arrays;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.cdc.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class WaitForChildPartitionsAction {

  private static final Logger LOG = LoggerFactory.getLogger(WaitForChildPartitionsAction.class);
  private final PartitionMetadataDao partitionMetadataDao;
  private final Duration resumeDuration;

  public WaitForChildPartitionsAction(
      PartitionMetadataDao partitionMetadataDao, Duration resumeDuration) {
    this.partitionMetadataDao = partitionMetadataDao;
    this.resumeDuration = resumeDuration;
  }

  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker,
      long childPartitionsToWaitFor) {
    LOG.debug("Waiting for child partitions for " + partition.getPartitionToken());

    if (!tracker.tryClaim(PartitionPosition.waitForChildPartitions(childPartitionsToWaitFor))) {
      LOG.debug("Could not claim, stopping");
      return Optional.of(ProcessContinuation.stop());
    }
    long numberOfFinishedChildren =
        partitionMetadataDao.countChildPartitionsInStates(
            partition.getPartitionToken(), Arrays.asList(SCHEDULED, FINISHED));
    LOG.debug(
        "Number of finished children is "
            + numberOfFinishedChildren
            + " and expected children to wait for is "
            + childPartitionsToWaitFor);
    if (numberOfFinishedChildren < childPartitionsToWaitFor) {
      LOG.debug(
          "Resuming, not all children are scheduled / finished (only "
              + numberOfFinishedChildren
              + " of "
              + childPartitionsToWaitFor
              + ")");
      return Optional.of(ProcessContinuation.resume().withResumeDelay(resumeDuration));
    }

    LOG.debug("Wait for child partitions action completed successfully");
    return Optional.empty();
  }
}
