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

import org.apache.beam.sdk.io.gcp.spanner.cdc.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionPosition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionRestriction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContinuation;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
public class DonePartitionAction {
  private static final Logger LOG = LoggerFactory.getLogger(DonePartitionAction.class);

  public ProcessContinuation run(
      PartitionMetadata partition,
      RestrictionTracker<PartitionRestriction, PartitionPosition> tracker) {
    final String token = partition.getPartitionToken();
    LOG.info("[" + token + "] Marking partition as done");

    if (!tracker.tryClaim(PartitionPosition.done())) {
      LOG.info("[" + token + "] Could not claim done(), stopping");
      return ProcessContinuation.stop();
    }

    LOG.info("[" + token + "] Done partition action completed successfully");
    return ProcessContinuation.stop();
  }
}
