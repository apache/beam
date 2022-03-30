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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction;

import static java.math.MathContext.DECIMAL128;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.toNanos;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.toTimestamp;

import com.google.cloud.Timestamp;
import java.math.BigDecimal;
import java.util.Optional;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.InitialPartition;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The PartitionRestrictionSplitter class. */
public class PartitionRestrictionSplitter {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionRestrictionSplitter.class);

  public @Nullable SplitResult<PartitionRestriction> trySplit(
      double fractionOfRemainder,
      @Nullable PartitionPosition lastClaimedPosition,
      PartitionRestriction restriction) {
    if (lastClaimedPosition == null) {
      return null;
    }

    final String token =
        Optional.ofNullable(restriction.getMetadata())
            .map(PartitionRestrictionMetadata::getPartitionToken)
            .orElse("");
    final PartitionMode positionMode = lastClaimedPosition.getMode();
    final Timestamp startTimestamp = restriction.getStartTimestamp();
    final Timestamp endTimestamp = restriction.getEndTimestamp();

    SplitResult<PartitionRestriction> splitResult;
    switch (positionMode) {
      case UPDATE_STATE:
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.queryChangeStream(startTimestamp, endTimestamp)
                    .withMetadata(restriction.getMetadata()));
        break;
      case QUERY_CHANGE_STREAM:
        if (token.equals(InitialPartition.PARTITION_TOKEN)) {
          splitResult = null;
        } else {
          final BigDecimal toInNanos = toNanos(endTimestamp);
          final BigDecimal currentInNanos = toNanos(lastClaimedPosition.getTimestamp().get());
          final BigDecimal nanosOffset =
              toInNanos
                  .subtract(currentInNanos, DECIMAL128)
                  .multiply(BigDecimal.valueOf(fractionOfRemainder), DECIMAL128)
                  .max(BigDecimal.ONE);

          // splitPosition = current + max(1, (range.getTo() - current) * fractionOfRemainder)
          final BigDecimal splitPositionInNanos = currentInNanos.add(nanosOffset, DECIMAL128);
          final Timestamp splitPosition = toTimestamp(splitPositionInNanos);

          // if the split position is greater than or equal to the restriction's end timestamp,
          // which is the partition's end timestamp + 1, it means that we are done executing the
          // change stream query. Thus, we should return STOP as primary, and WAIT_FOR_CHILD_
          // PARTITIONS as residual.
          if (splitPosition.compareTo(endTimestamp) >= 0) {
            splitResult =
                SplitResult.of(
                    PartitionRestriction.stop(restriction),
                    PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp)
                        .withMetadata(restriction.getMetadata()));
          } else {
            splitResult =
                SplitResult.of(
                    PartitionRestriction.queryChangeStream(startTimestamp, splitPosition)
                        .withMetadata(restriction.getMetadata()),
                    PartitionRestriction.queryChangeStream(splitPosition, endTimestamp)
                        .withMetadata(restriction.getMetadata()));
          }
        }
        break;
      case WAIT_FOR_CHILD_PARTITIONS:
        splitResult =
            SplitResult.of(
                PartitionRestriction.stop(restriction),
                PartitionRestriction.waitForChildPartitions(startTimestamp, endTimestamp)
                    .withMetadata(restriction.getMetadata()));
        break;
      case DONE:
      case STOP:
        splitResult = null;
        break;
      default:
        // TODO: See if we need to throw or do something else
        throw new IllegalArgumentException("Unknown mode " + positionMode);
    }

    LOG.debug(
        "["
            + token
            + "] Split result for ("
            + fractionOfRemainder
            + ", "
            + lastClaimedPosition
            + ", "
            + restriction
            + ") is "
            + splitResult);
    return splitResult;
  }
}
