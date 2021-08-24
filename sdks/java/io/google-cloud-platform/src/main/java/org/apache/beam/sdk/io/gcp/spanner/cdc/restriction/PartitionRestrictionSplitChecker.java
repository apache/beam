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
  private static final long MICRO_SECONDS_REQUIRED_BEFORE_SPLIT = 1L;

  public boolean isSplitAllowed(
      PartitionRestriction restriction, PartitionPosition claimedPosition) {
    final String partitionToken = restriction.getMetadata().getPartitionToken();
    final PartitionMode positionMode = claimedPosition.getMode();
    checkArgument(
        positionMode != QUERY_CHANGE_STREAM || claimedPosition.getTimestamp().isPresent(),
        "%s mode must specify a timestamp (no value sent)",
        positionMode);

    boolean isSplitAllowed;
    switch (positionMode) {
      case QUERY_CHANGE_STREAM:
        final Timestamp startTimestamp = restriction.getStartTimestamp();
        isSplitAllowed = isQueryChangeStreamSplitAllowed(startTimestamp, claimedPosition);
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
        "["
            + partitionToken
            + "] Is split allowed for ("
            + restriction
            + ","
            + claimedPosition
            + ") is "
            + isSplitAllowed);
    return isSplitAllowed;
  }

  private boolean isQueryChangeStreamSplitAllowed(
      Timestamp startTimestamp, PartitionPosition currentPosition) {
    final Timestamp claimedTimestamp = currentPosition.getTimestamp().get();
    final Duration duration =
        Timestamps.between(startTimestamp.toProto(), claimedTimestamp.toProto());
    if (duration.getSeconds() >= 1) {
      return true;
    }
    return (duration.getNanos() / 1000.0) >= MICRO_SECONDS_REQUIRED_BEFORE_SPLIT;
  }
}
