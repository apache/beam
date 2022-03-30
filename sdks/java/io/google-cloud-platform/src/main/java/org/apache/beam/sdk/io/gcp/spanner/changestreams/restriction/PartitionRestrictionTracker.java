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

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.DONE;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.STOP;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import java.util.Optional;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// TODO: Add java docs
// TODO: Implement duration waiting for returning false on try claim
public class PartitionRestrictionTracker
    extends RestrictionTracker<PartitionRestriction, PartitionPosition> implements HasProgress {

  private static final Logger LOG = LoggerFactory.getLogger(PartitionRestrictionTracker.class);
  private final PartitionRestrictionSplitter splitter;
  private final PartitionRestrictionClaimer claimer;
  private final PartitionRestrictionProgressChecker progressChecker;
  protected PartitionRestriction restriction;
  private @Nullable PartitionPosition lastClaimedPosition;

  public PartitionRestrictionTracker(PartitionRestriction restriction) {
    this(
        restriction,
        new PartitionRestrictionSplitter(),
        new PartitionRestrictionClaimer(),
        new PartitionRestrictionProgressChecker());
  }

  PartitionRestrictionTracker(
      PartitionRestriction restriction,
      PartitionRestrictionSplitter splitter,
      PartitionRestrictionClaimer claimer,
      PartitionRestrictionProgressChecker progressChecker) {
    this.splitter = splitter;
    this.claimer = claimer;
    this.restriction = restriction;
    this.progressChecker = progressChecker;
  }

  @Override
  public @Nullable SplitResult<PartitionRestriction> trySplit(double fractionOfRemainder) {
    final String token =
        Optional.ofNullable(restriction.getMetadata())
            .map(PartitionRestrictionMetadata::getPartitionToken)
            .orElse("");
    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(fractionOfRemainder, lastClaimedPosition, restriction);
    LOG.debug(
        "["
            + token
            + "] Try split "
            + lastClaimedPosition
            + ", "
            + restriction
            + ": "
            + splitResult);
    if (splitResult != null) {
      PartitionRestriction restrictionFromSplit =
          Optional.ofNullable(splitResult.getPrimary()).orElse(this.restriction);
      this.restriction = restrictionFromSplit;
    }
    return splitResult;
  }

  @Override
  public boolean tryClaim(PartitionPosition position) {
    final boolean canClaim = claimer.tryClaim(restriction, lastClaimedPosition, position);

    if (canClaim) {
      this.lastClaimedPosition = position;
    }

    return canClaim;
  }

  @Override
  public Progress getProgress() {
    final String token =
        Optional.ofNullable(restriction.getMetadata())
            .map(PartitionRestrictionMetadata::getPartitionToken)
            .orElse(null);
    final Progress progress = progressChecker.getProgress(restriction, lastClaimedPosition);
    LOG.debug("[" + token + "] Progress is " + progress);
    return progress;
  }

  @Override
  public PartitionRestriction currentRestriction() {
    return restriction;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    if (restriction.getMode() == STOP) {
      return;
    }
    // If nothing was attempted, throws an exception
    checkState(
        lastClaimedPosition != null,
        "restriction is non-empty %s and no keys have been attempted.",
        restriction.toString());

    if (lastClaimedPosition != null) {
      final PartitionMode currentMode = lastClaimedPosition.getMode();
      checkState(
          currentMode == DONE, "Restriction %s does not have mode DONE", restriction.toString());
    } else {
      throw new IllegalStateException(
          "Last claimed position is null in restriction: " + restriction.toString());
    }
  }

  @Override
  public IsBounded isBounded() {
    return IsBounded.UNBOUNDED;
  }

  @VisibleForTesting
  PartitionRestriction getRestriction() {
    return restriction;
  }

  @VisibleForTesting
  @Nullable
  PartitionPosition getLastClaimedPosition() {
    return lastClaimedPosition;
  }
}
