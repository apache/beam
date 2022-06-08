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

import java.util.HashSet;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataDao;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is part of the process for {@link
 * org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.ReadChangeStreamPartitionDoFn} SDF. It is
 * responsible for processing {@link ChildPartitionsRecord}s. The new child partitions will be
 * stored in the Connector's metadata tables in order to be scheduled for future querying by the
 * {@link org.apache.beam.sdk.io.gcp.spanner.changestreams.dofn.DetectNewPartitionsDoFn} SDF.
 */
public class WaitForChildPartitionsAction {

  private static final Logger LOG = LoggerFactory.getLogger(WaitForChildPartitionsAction.class);
  private final PartitionMetadataDao partitionMetadataDao;
  private final Duration resumeDuration;

  /**
   * Constructs an action class for handling {@link ChildPartitionsRecord}s.
   *
   * @param partitionMetadataDao DAO class to access the Connector's metadata tables
   * @param metrics metrics gathering class
   */
  WaitForChildPartitionsAction(PartitionMetadataDao partitionMetadataDao) {
    this.partitionMetadataDao = partitionMetadataDao;
    resumeDuration = Duration.millis(3000);
  }

  /** Add java doc */
  @VisibleForTesting
  public Optional<ProcessContinuation> run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    final String token = partition.getPartitionToken();

    if (!tracker.tryClaim(PartitionPosition.waitForChildPartitions())) {
      LOG.info("[" + token + "] Could not claim waitForChildPartitions(), stopping");
      return Optional.of(ProcessContinuation.stop());
    }
    HashSet<String> childTokens = partitionMetadataDao.getChildTokens(token);
    if (childTokens != null) {
      long numberOfNotRunningChildren =
          partitionMetadataDao.countNeverRunChildPartitions(childTokens);
      LOG.info("[" + token + "] Number never run children is " + numberOfNotRunningChildren);
      if (numberOfNotRunningChildren > 0) {
        LOG.info(
            "["
                + token
                + " ] Resuming, there are "
                + numberOfNotRunningChildren
                + " never run children");

        return Optional.of(ProcessContinuation.resume().withResumeDelay(resumeDuration));
      }
    }

    LOG.info("[" + token + "] Wait for child partitions action completed successfully");
    return Optional.empty();
  }
}
