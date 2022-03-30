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
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.UPDATE_STATE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.WAIT_FOR_CHILD_PARTITIONS;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.toNanos;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.Progress;
import org.checkerframework.checker.nullness.qual.Nullable;

public class PartitionRestrictionProgressChecker {

  private static final BigDecimal TOTAL_MODE_TRANSITIONS = BigDecimal.valueOf(3L);

  /**
   * Indicates how many mode transitions have been completed for the current mode. The transitions
   * are as follows: * (1) QUERY_CHANGE_STREAM, (2) WAIT_FOR_CHILD_PARTITIONS, (3) FINISH_PARTITION,
   * (4) WAIT_FOR_PARENT_PARTITIONS, (5) DELETE_PARTITION, (6) DONE.
   *
   * <p>This is used to calculate the units of work left, meaning that 1 transition = 1 unit of
   * work.
   */
  private final Map<PartitionMode, BigDecimal> modeToTransitionsCompleted;

  public PartitionRestrictionProgressChecker() {
    modeToTransitionsCompleted = new HashMap<>();
    modeToTransitionsCompleted.put(UPDATE_STATE, BigDecimal.valueOf(0L));
    modeToTransitionsCompleted.put(QUERY_CHANGE_STREAM, BigDecimal.valueOf(1L));
    modeToTransitionsCompleted.put(WAIT_FOR_CHILD_PARTITIONS, BigDecimal.valueOf(2L));
    modeToTransitionsCompleted.put(DONE, BigDecimal.valueOf(3L));
  }

  public Progress getProgress(
      PartitionRestriction restriction, @Nullable PartitionPosition lastClaimedPosition) {
    final PartitionMode currentMode =
        Optional.ofNullable(lastClaimedPosition)
            .map(PartitionPosition::getMode)
            .orElse(restriction.getMode());
    if (currentMode == STOP) {
      // Return progress indicating that there is no more work to be done for this SDF.
      final double workCompleted = 1D;
      final double workRemaining = 0D;

      return Progress.from(workCompleted, workRemaining);
    }
    final BigDecimal transitionsCompleted =
        modeToTransitionsCompleted.getOrDefault(currentMode, BigDecimal.ZERO);

    final BigDecimal startTimestampAsNanos = toNanos(restriction.getStartTimestamp());
    final BigDecimal endTimestampAsNanos = toNanos(restriction.getEndTimestamp());
    final BigDecimal currentTimestampAsNanos =
        Optional.ofNullable(lastClaimedPosition)
            .flatMap(PartitionPosition::getTimestamp)
            .map(TimestampUtils::toNanos)
            .orElse(
                (currentMode == UPDATE_STATE || currentMode == QUERY_CHANGE_STREAM)
                    ? startTimestampAsNanos
                    : endTimestampAsNanos);

    final BigDecimal totalWork =
        endTimestampAsNanos
            .subtract(startTimestampAsNanos, DECIMAL128)
            .add(TOTAL_MODE_TRANSITIONS, DECIMAL128);

    final BigDecimal workCompleted =
        currentTimestampAsNanos.subtract(startTimestampAsNanos).add(transitionsCompleted);
    final BigDecimal workLeft =
        endTimestampAsNanos
            .subtract(startTimestampAsNanos)
            .add(TOTAL_MODE_TRANSITIONS)
            .subtract(workCompleted);

    return Progress.from(
        workCompleted.divide(totalWork, DECIMAL128).doubleValue(),
        workLeft.divide(totalWork, DECIMAL128).doubleValue());
  }
}
