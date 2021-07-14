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

import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;

// TODO: Add java docs
// TODO: Implement duration waiting for returning false on try claim
public class PartitionRestrictionTracker
    extends RestrictionTracker<PartitionRestriction, PartitionPosition> implements HasProgress {

  private final PartitionRestrictionSplitter splitter;
  private final PartitionRestrictionClaimer claimer;
  private final PartitionRestrictionSplitChecker splitChecker;
  private final PartitionRestrictionProgressChecker progressChecker;
  private PartitionRestriction restriction;
  private PartitionPosition lastClaimedPosition;
  private boolean isSplitAllowed;

  public PartitionRestrictionTracker(PartitionRestriction restriction) {
    this(
        restriction,
        new PartitionRestrictionSplitter(),
        new PartitionRestrictionClaimer(),
        new PartitionRestrictionSplitChecker(),
        new PartitionRestrictionProgressChecker());
  }

  PartitionRestrictionTracker(
      PartitionRestriction restriction,
      PartitionRestrictionSplitter splitter,
      PartitionRestrictionClaimer claimer,
      PartitionRestrictionSplitChecker splitChecker,
      PartitionRestrictionProgressChecker progressChecker) {
    this.splitter = splitter;
    this.claimer = claimer;
    this.splitChecker = splitChecker;
    this.restriction = restriction;
    this.isSplitAllowed = restriction.getMode() != QUERY_CHANGE_STREAM;
    this.progressChecker = progressChecker;
  }

  @Override
  public @Nullable SplitResult<PartitionRestriction> trySplit(double fractionOfRemainder) {
    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(fractionOfRemainder, isSplitAllowed, lastClaimedPosition, restriction);
    if (splitResult != null) {
      this.restriction = splitResult.getPrimary();
    }
    return splitResult;
  }

  @Override
  public boolean tryClaim(PartitionPosition position) {
    final boolean canClaim = claimer.tryClaim(restriction, lastClaimedPosition, position);

    if (canClaim) {
      this.isSplitAllowed = splitChecker.isSplitAllowed(lastClaimedPosition, position);
      this.lastClaimedPosition = position;
    }

    return canClaim;
  }

  @Override
  public Progress getProgress() {
    return progressChecker.getProgress(restriction, lastClaimedPosition);
  }

  @Override
  public PartitionRestriction currentRestriction() {
    return restriction;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    // FIXME: Implement
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @VisibleForTesting
  boolean isSplitAllowed() {
    return isSplitAllowed;
  }

  @VisibleForTesting
  PartitionRestriction getRestriction() {
    return restriction;
  }

  @VisibleForTesting
  PartitionPosition getLastClaimedPosition() {
    return lastClaimedPosition;
  }
}
