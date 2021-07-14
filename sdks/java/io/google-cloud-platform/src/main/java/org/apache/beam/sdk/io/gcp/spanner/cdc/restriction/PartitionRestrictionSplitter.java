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

import static org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampConverter.timestampFromMicros;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.TimestampConverter.timestampToMicros;
import static org.apache.beam.sdk.io.gcp.spanner.cdc.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add javadocs
public class PartitionRestrictionSplitter {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionRestrictionSplitter.class);

  public SplitResult<PartitionRestriction> trySplit(
      double fractionOfRemainder,
      boolean isSplitAllowed,
      PartitionPosition lastClaimedPosition,
      PartitionRestriction restriction) {
    // Move this check to the caller class
    if (!isSplitAllowed) {
      return null;
    }
    if (lastClaimedPosition == null) {
      return null;
    }

    final PartitionMode positionMode = lastClaimedPosition.getMode();
    checkArgument(
        positionMode != QUERY_CHANGE_STREAM || lastClaimedPosition.getTimestamp().isPresent(),
        "%s mode must specify a timestamp (no value sent)",
        positionMode);

    final Timestamp startTimestamp = restriction.getStartTimestamp();
    final Timestamp endTimestamp = restriction.getEndTimestamp();

    SplitResult<PartitionRestriction> splitResult = null;
    switch (positionMode) {
      case QUERY_CHANGE_STREAM:
        splitResult = splitQueryChangeStream(fractionOfRemainder, restriction, lastClaimedPosition);
        break;
      case WAIT_FOR_CHILD_PARTITIONS:
        // If we need to split the wait for child partitions, we remain at the same mode. That is
        // because the primary restriction might resume and it might so happen that the residual
        // restriction gets scheduled before the primary.
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp));
        break;
      case FINISH_PARTITION:
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.waitForParentPartitions(startTimestamp, endTimestamp));
        break;
      case WAIT_FOR_PARENT_PARTITIONS:
        // If we need to split the wait for parent partitions, we remain at the same mode. That is
        // because the primary restriction might resume and it might so happen that the residual
        // restriction gets scheduled before the primary.
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.waitForParentPartitions(startTimestamp, endTimestamp));
        break;
      case DELETE_PARTITION:
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.done(startTimestamp, endTimestamp));
        break;
      case DONE:
        return null;
      case STOP:
        splitResult = null;
        break;
      default:
        // TODO: See if we need to throw or do something else
        throw new IllegalArgumentException("Unknown mode " + positionMode);
    }

    LOG.debug(
        "Split result for ("
            + fractionOfRemainder
            + ", "
            + isSplitAllowed
            + ", "
            + lastClaimedPosition
            + ", "
            + restriction
            + ") is "
            + splitResult);
    return splitResult;
  }

  private SplitResult<PartitionRestriction> splitQueryChangeStream(
      double fractionOfRemainder,
      PartitionRestriction restriction,
      PartitionPosition lastClaimedPosition) {
    final Timestamp startTimestamp = restriction.getStartTimestamp();
    final Timestamp endTimestamp = restriction.getEndTimestamp();

    // FIXME: The backend only supports micros precision for now. Change this to nanos whenever
    // possible
    final BigDecimal currentMicros = timestampToMicros(lastClaimedPosition.getTimestamp().get());
    final BigDecimal endMicros = timestampToMicros(endTimestamp);
    final BigDecimal splitPositionMicros =
        currentMicros.add(
            endMicros
                .subtract(currentMicros)
                .multiply(BigDecimal.valueOf(fractionOfRemainder))
                .max(BigDecimal.ONE));

    final Timestamp primaryEndTimestamp = timestampFromMicros(splitPositionMicros);
    final Timestamp residualStartTimestamp =
        timestampFromMicros(splitPositionMicros.add(BigDecimal.ONE));

    if (residualStartTimestamp.compareTo(endTimestamp) > 0) {
      return null;
    } else {
      return SplitResult.of(
          PartitionRestriction.queryChangeStream(startTimestamp, primaryEndTimestamp),
          PartitionRestriction.queryChangeStream(residualStartTimestamp, endTimestamp));
    }
  }
}
