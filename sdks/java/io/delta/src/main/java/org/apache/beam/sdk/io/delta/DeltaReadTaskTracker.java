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
package org.apache.beam.sdk.io.delta;

import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;

/**
 * A {@link org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker} for tracking progress
 * across Parquet row groups represented by a {@link DeltaReadTask}.
 */
public class DeltaReadTaskTracker extends OffsetRangeTracker {
  private final List<Long> rowGroupSizes;

  public DeltaReadTaskTracker(OffsetRange restriction, List<Long> rowGroupSizes) {
    super(restriction);
    this.rowGroupSizes = rowGroupSizes;
  }

  @Override
  public Progress getProgress() {
    long workCompleted = 0L;
    long workRemaining = 0L;
    long from = range.getFrom();
    long to = range.getTo();
    long attempted = lastAttemptedOffset == null ? (from - 1) : lastAttemptedOffset;

    for (int i = (int) from; i < (int) to; i++) {
      // Upper bound of the range is the number of row groups.
      if (i < rowGroupSizes.size()) {
        if (i <= attempted) {
          workCompleted += rowGroupSizes.get(i);
        } else {
          workRemaining += rowGroupSizes.get(i);
        }
      }
    }
    return Progress.from((double) workCompleted, (double) workRemaining);
  }
}
