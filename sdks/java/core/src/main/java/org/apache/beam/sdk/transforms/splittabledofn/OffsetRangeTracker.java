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
package org.apache.beam.sdk.transforms.splittabledofn;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link RestrictionTracker} for claiming offsets in an {@link OffsetRange} in a monotonically
 * increasing fashion.
 */
public class OffsetRangeTracker implements RestrictionTracker<OffsetRange> {
  private OffsetRange range;
  private Long lastClaimedOffset = null;

  public OffsetRangeTracker(OffsetRange range) {
    this.range = checkNotNull(range);
  }

  @Override
  public synchronized OffsetRange currentRestriction() {
    return range;
  }

  @Override
  public synchronized OffsetRange checkpoint() {
    if (lastClaimedOffset == null) {
      OffsetRange res = range;
      range = new OffsetRange(range.getFrom(), range.getFrom());
      return res;
    }
    OffsetRange res = new OffsetRange(lastClaimedOffset + 1, range.getTo());
    this.range = new OffsetRange(range.getFrom(), lastClaimedOffset + 1);
    return res;
  }

  /**
   * Attempts to claim the given offset.
   *
   * <p>Must be larger than the last successfully claimed offset.
   *
   * @return {@code true} if the offset was successfully claimed, {@code false} if it is outside the
   *     current {@link OffsetRange} of this tracker (in that case this operation is a no-op).
   */
  public synchronized boolean tryClaim(long i) {
    checkArgument(
        lastClaimedOffset == null || i > lastClaimedOffset,
        "Trying to claim offset %s while last claimed was %s",
        i,
        lastClaimedOffset);
    checkArgument(
        i >= range.getFrom(), "Trying to claim offset %s before start of the range %s", i, range);
    // No respective checkArgument for i < range.to() - it's ok to try claiming offsets beyond it.
    if (i >= range.getTo()) {
      return false;
    }
    lastClaimedOffset = i;
    return true;
  }
}
