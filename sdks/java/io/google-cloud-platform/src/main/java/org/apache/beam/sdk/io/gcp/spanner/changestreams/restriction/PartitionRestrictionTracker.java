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
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.QUERY_CHANGE_STREAM;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.PartitionMode.STOP;
import static org.apache.beam.sdk.io.gcp.spanner.changestreams.restriction.TimestampUtils.next;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.cloud.Timestamp;
import java.util.Optional;
import java.util.function.Supplier;
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
  protected Supplier<Timestamp> timeSupplier;

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
    this.timeSupplier = () -> Timestamp.now();
  }

  @VisibleForTesting
  public void setTimeSupplier(Supplier<Timestamp> timeSupplier) {
    this.timeSupplier = timeSupplier;
  }

  @Override
  public @Nullable SplitResult<PartitionRestriction> trySplit(double fractionOfRemainder) {
    final SplitResult<PartitionRestriction> splitResult =
        splitter.trySplit(fractionOfRemainder, lastClaimedPosition, restriction);
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
    if (lastClaimedPosition != null) {
      final PartitionMode currentMode = lastClaimedPosition.getMode();
      if (currentMode == QUERY_CHANGE_STREAM) {
        // If the mode is QUERY_CHANGE_STREAM, and the end of the timestamp range wasn't attempted,
        // throw an exception
        final Timestamp nextPosition = next(lastClaimedPosition.getTimestamp().get());
        if (nextPosition.compareTo(restriction.getEndTimestamp()) < 0) {
          throw new IllegalStateException(
              String.format(
                  "Last attempted key was %s in range %s, claiming work in [%s, %s) was not"
                      + " attempted",
                  nextPosition, restriction, nextPosition, restriction.getEndTimestamp()));
        }
      } else {
        checkState(
            currentMode == DONE, "Restriction %s does not have mode DONE", restriction.toString());
      }
    } else {
      // If nothing was attempted, throws an exception
      throw new IllegalStateException(
          String.format(
              "restriction is non-empty %s and no keys have been attempted.", restriction));
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
