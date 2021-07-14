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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata.State.CREATED;

import java.util.Collections;
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
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    final String token = partition.getPartitionToken();
    LOG.debug("[" + token + "] Waiting for child partitions");

    if (!tracker.tryClaim(PartitionPosition.waitForChildPartitions())) {
      LOG.debug("[" + token + "] Could not claim waitForChildPartitions(), stopping");
      return Optional.of(ProcessContinuation.stop());
    }
    long numberOfUnscheduledChildren =
        partitionMetadataDao.countChildPartitionsInStates(
            token, Collections.singletonList(CREATED));
    LOG.debug("[" + token + "] Number of unscheduled children is " + numberOfUnscheduledChildren);
    if (numberOfUnscheduledChildren > 0) {
      LOG.debug(
          "["
              + token
              + " ] Resuming, there are "
              + numberOfUnscheduledChildren
              + " unscheduled / not finished children");

      return Optional.of(ProcessContinuation.resume().withResumeDelay(resumeDuration));
    }

    LOG.debug("[" + token + "] Wait for child partitions action completed successfully");
    return Optional.empty();
  }
}
