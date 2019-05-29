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
package org.apache.beam.sdk.io.hcatalog;

import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v20_0.com.google.common.base.Preconditions.checkState;

import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.splittabledofn.Backlog;
import org.apache.beam.sdk.transforms.splittabledofn.Backlogs;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;

/** Tracks split for partitions. */
public class SplitTracker extends RestrictionTracker<Split, Integer>
    implements Backlogs.HasBacklog {

  @Nullable private Integer lastAttemptedSplit = null;

  @Nullable
  public Integer getLastClaimedSplit() {
    return lastClaimedSplit;
  }

  @Nullable private Integer lastClaimedSplit = null;

  public Split getSplit() {
    return split;
  }

  private Split split;

  public SplitTracker(Split split) {
    this.split = split;
  }

  @Override
  public synchronized Backlog getBacklog() {
    // If we have never attempted an offset, we return the length of the entire range.
    if (lastAttemptedSplit == null) {
      return Backlog.of(BigDecimal.valueOf(split.getTo() - split.getFrom()));
    }

    // Otherwise we return the length from where we are to where we are attempting to get to
    // with a minimum of zero in case we have claimed beyond the end of the range.
    return Backlog.of(BigDecimal.valueOf(Math.max(split.getTo() - lastAttemptedSplit, 0)));
  }

  @Override
  public boolean tryClaim(Integer position) {
    checkArgument(
        lastAttemptedSplit == null || position > lastAttemptedSplit,
        "Trying to claim a split %s while last attempted was %s",
        position,
        lastAttemptedSplit);
    checkArgument(
        position >= split.getFrom(),
        "Trying to claim split %s before start of the range %s",
        position,
        split);
    lastAttemptedSplit = position;
    if (position >= split.getTo()) {
      return false;
    }
    lastClaimedSplit = position;
    return true;
  }

  @Override
  public Split currentRestriction() {
    return split;
  }

  @Override
  public Split checkpoint() {
    checkState(
        lastClaimedSplit != null, "Can't checkpoint before any split was successfully claimed");
    Split res = new Split(lastClaimedSplit + 1, split.getTo());
    this.split = new Split(split.getFrom(), lastClaimedSplit + 1);
    return res;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    checkState(
        lastAttemptedSplit >= split.getFrom(),
        "Last attempted split was %s in range %s, claiming work in [%s, %s) was not attempted",
        lastAttemptedSplit,
        split,
        lastAttemptedSplit + 1,
        split.getTo());
  }
}
