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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.Timestamp;
import com.google.protobuf.Duration;
import com.google.protobuf.util.Timestamps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PartitionRestrictionSplitChecker {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionRestrictionSplitChecker.class);
  private static final long SECONDS_REQUIRED_BEFORE_SPLIT = 1L;

  public boolean isSplitAllowed(
      PartitionPosition lastClaimedPosition, PartitionPosition claimedPosition) {
    final PartitionMode positionMode = claimedPosition.getMode();
    checkArgument(
        positionMode != QUERY_CHANGE_STREAM || claimedPosition.getTimestamp().isPresent(),
        "%s mode must specify a timestamp (no value sent)",
        positionMode);

    boolean isSplitAllowed;
    switch (positionMode) {
      case QUERY_CHANGE_STREAM:
        isSplitAllowed = isQueryChangeStreamSplitAllowed(lastClaimedPosition, claimedPosition);
        break;
      case WAIT_FOR_CHILD_PARTITIONS:
      case FINISH_PARTITION:
      case WAIT_FOR_PARENT_PARTITIONS:
      case DELETE_PARTITION:
        isSplitAllowed = true;
        break;
      case DONE:
      case STOP:
        isSplitAllowed = false;
        break;
      default:
        throw new IllegalArgumentException("Unknown mode " + positionMode);
    }

    LOG.debug(
        "Is split allowed for ("
            + lastClaimedPosition
            + ","
            + claimedPosition
            + ") is "
            + isSplitAllowed);
    return isSplitAllowed;
  }

  private boolean isQueryChangeStreamSplitAllowed(
      PartitionPosition lastClaimedPosition, PartitionPosition currentPosition) {
    if (lastClaimedPosition == null) {
      return false;
    }
    if (!lastClaimedPosition.getTimestamp().isPresent()) {
      return false;
    }

    final Timestamp lastClaimedTimestamp = lastClaimedPosition.getTimestamp().get();
    final Timestamp claimedTimestamp = currentPosition.getTimestamp().get();
    final Duration duration =
        Timestamps.between(lastClaimedTimestamp.toProto(), claimedTimestamp.toProto());
    return duration.getSeconds() >= SECONDS_REQUIRED_BEFORE_SPLIT;
  }
}
