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
package org.apache.beam.runners.flink.translation.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.beam.runners.core.metrics.DistributionCell;

/** Helpers for reporting checkpoint durations. */
public class CheckpointStats {

  /** Checkpoint id => Checkpoint start (System.currentTimeMillis()). */
  private final Map<Long, Long> checkpointDurations = new HashMap<>();
  /** Distribution cell for reporting checkpoint durations. */
  private final Supplier<DistributionCell> distributionCellSupplier;

  public CheckpointStats(Supplier<DistributionCell> distributionCellSupplier) {
    this.distributionCellSupplier = distributionCellSupplier;
  }

  public void snapshotStart(long checkpointId) {
    checkpointDurations.put(checkpointId, System.currentTimeMillis());
  }

  public void reportCheckpointDuration(long checkpointId) {
    Long checkpointStart = checkpointDurations.remove(checkpointId);
    if (checkpointStart != null) {
      long checkpointDuration = System.currentTimeMillis() - checkpointStart;
      distributionCellSupplier.get().update(checkpointDuration);
    }
  }
}
