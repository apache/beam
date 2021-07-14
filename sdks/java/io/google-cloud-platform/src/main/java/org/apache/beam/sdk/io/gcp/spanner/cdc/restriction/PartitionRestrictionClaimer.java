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
package org.apache.beam.sdk.io.gcp.spanner.cdc.restriction;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DELETE_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.FINISH_PARTITION;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.WAIT_FOR_PARENT_PARTITIONS;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.Timestamp;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionRestrictionClaimer {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionRestrictionClaimer.class);

  private final Map<PartitionMode, Set<PartitionMode>> allowedTransitions;

  public PartitionRestrictionClaimer() {
    this.allowedTransitions = new HashMap<>();
    allowedTransitions.put(
        QUERY_CHANGE_STREAM,
        Sets.newHashSet(QUERY_CHANGE_STREAM, WAIT_FOR_CHILD_PARTITIONS, FINISH_PARTITION));
    allowedTransitions.put(
        WAIT_FOR_CHILD_PARTITIONS, Sets.newHashSet(WAIT_FOR_CHILD_PARTITIONS, FINISH_PARTITION));
    allowedTransitions.put(
        FINISH_PARTITION, Sets.newHashSet(FINISH_PARTITION, WAIT_FOR_PARENT_PARTITIONS));
    allowedTransitions.put(
        WAIT_FOR_PARENT_PARTITIONS, Sets.newHashSet(WAIT_FOR_PARENT_PARTITIONS, DELETE_PARTITION));
    allowedTransitions.put(DELETE_PARTITION, Sets.newHashSet(DELETE_PARTITION, DONE));
    allowedTransitions.put(DONE, Sets.newHashSet(DONE));
  }

  public boolean tryClaim(
      PartitionRestriction restriction,
      PartitionPosition lastClaimedPosition,
      PartitionPosition position) {
    final PartitionMode fromMode =
        Optional.ofNullable(lastClaimedPosition)
            .map(PartitionPosition::getMode)
            .orElse(restriction.getMode());
    final PartitionMode toMode = position.getMode();

    if (fromMode == STOP) {
      LOG.debug(
          "Try claim from ("
              + restriction
              + ","
              + lastClaimedPosition
              + ", "
              + position
              + ") is false");
      return false;
    }

    checkArgument(
        allowedTransitions.getOrDefault(fromMode, Collections.emptySet()).contains(toMode),
        "Invalid partition mode transition from %s to %s",
        fromMode,
        toMode);
    checkArgument(
        toMode != QUERY_CHANGE_STREAM || position.getTimestamp().isPresent(),
        "%s mode must specify a timestamp (no value sent)",
        toMode);

    boolean tryClaimResult;
    switch (toMode) {
      case QUERY_CHANGE_STREAM:
        final Timestamp attemptedTimestamp = position.getTimestamp().get();
        final Timestamp endTimestamp =
            Optional.ofNullable(restriction.getEndTimestamp()).orElse(Timestamp.MAX_VALUE);

        // FIXME: Should this be inclusive?
        tryClaimResult = attemptedTimestamp.compareTo(endTimestamp) <= 0;
        break;
      case WAIT_FOR_CHILD_PARTITIONS:
      case FINISH_PARTITION:
      case WAIT_FOR_PARENT_PARTITIONS:
      case DELETE_PARTITION:
      case DONE:
        tryClaimResult = true;
        break;
      case STOP:
        throw new IllegalArgumentException("Trying to claim STOP state is invalid");
      default:
        // TODO: See if we need to throw or do something else
        throw new IllegalArgumentException("Unknown mode " + fromMode);
    }

    LOG.debug("Try claim from (" + restriction + ", " + position + ") is " + tryClaimResult);
    return tryClaimResult;
  }
}
