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

import com.google.cloud.Timestamp;
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
   * are as follows: * (1) UPDATE_STATE, (2) QUERY_CHANGE_STREAM, (3) WAIT_FOR_CHILD_PARTITIONS, (4)
   * DONE.
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
      PartitionRestriction restriction,
      @Nullable PartitionPosition lastClaimedPosition,
      BigDecimal totalSeconds) {
    final PartitionMode currentMode =
        Optional.ofNullable(lastClaimedPosition)
            .map(PartitionPosition::getMode)
            .orElse(restriction.getMode());

    // Total work is the total number of seconds in the timestamp range plus the total number
    // of mode transitions.
    final BigDecimal totalWork = totalSeconds.add(TOTAL_MODE_TRANSITIONS, DECIMAL128);
    if (currentMode == STOP) {
      // Return progress indicating that there is no more work to be done for this SDF.
      final double workCompleted = totalWork.doubleValue();
      final double workRemaining = 1D;

      return Progress.from(workCompleted, workRemaining);
    }

    // We get the current number of seconds.
    final long currentSecondsNum =
        Optional.ofNullable(lastClaimedPosition)
            .flatMap(PartitionPosition::getTimestamp)
            .map(Timestamp::getSeconds)
            .orElse(
                (currentMode == UPDATE_STATE || currentMode == QUERY_CHANGE_STREAM)
                    ? restriction.getStartTimestamp().getSeconds()
                    : restriction.getEndTimestamp().getSeconds());
    final BigDecimal currentSeconds = BigDecimal.valueOf(currentSecondsNum);
    final BigDecimal transitionsCompleted =
        modeToTransitionsCompleted.getOrDefault(currentMode, BigDecimal.ZERO);

    // The work completed is the current number of seconds plus the number of transitions completed.
    final BigDecimal workCompleted = currentSeconds.add(transitionsCompleted);

    // The work remaining is the total work minus the work completed.
    // The remaining work must be greater than 0. Otherwise, it will cause an issue
    // that the watermark does not advance.
    final BigDecimal workRemaining = totalWork.subtract(workCompleted).max(BigDecimal.ONE);

    return Progress.from(workCompleted.doubleValue(), workRemaining.doubleValue());
  }
}
