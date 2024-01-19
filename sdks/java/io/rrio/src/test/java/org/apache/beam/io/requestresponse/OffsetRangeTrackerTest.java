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
package org.apache.beam.io.requestresponse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;

import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.junit.Test;

/** Tests for {@link ThrottleWithoutExternalResource.OffsetRangeTracker}. */
public class OffsetRangeTrackerTest {

  /////////////////////////// Empty tests //////////////////////////

  @Test
  public void givenEmpty_thenTryClaim_isFalse() {
    assertThat(createEmptyRestriction().newTracker().tryClaim(0), is(false));
  }

  @Test
  public void givenEmpty_thenCurrentRestrictionEmpty() {
    ThrottleWithoutExternalResource.OffsetRange restriction =
        createEmptyRestriction().newTracker().currentRestriction();
    assertThat(restriction.getFromInclusive(), is(-1));
    assertThat(restriction.getCurrent(), is(-1));
    assertThat(restriction.getToExclusive(), is(0));
  }

  @Test
  public void givenEmpty_thenTrySplitNull() {
    assertThat(createEmptyRestriction().newTracker().trySplit(0.4), nullValue());
  }

  @Test
  public void givenEmpty_thenCheckDoneNoop() {
    createEmptyRestriction().newTracker().checkDone();
  }

  @Test
  public void givenEmpty_thenIsBounded_Bounded() {
    assertThat(
        createEmptyRestriction().newTracker().isBounded(),
        is(RestrictionTracker.IsBounded.BOUNDED));
  }

  @Test
  public void givenEmpty_thenGetProgressEmpty() {
    RestrictionTracker.Progress progress = createEmptyRestriction().newTracker().getProgress();
    assertThat(progress.getWorkCompleted(), is(0.0));
    assertThat(progress.getWorkRemaining(), is(0.0));
  }

  private static ThrottleWithoutExternalResource.OffsetRange createEmptyRestriction() {
    return ThrottleWithoutExternalResource.OffsetRange.empty();
  }

  /////////////////////////// End Empty tests ////////////////////////////

  /////////////////////////// Start Non Empty tests //////////////////////

  @Test
  public void givenNonEmpty_positionOutsideRange_thenTryClaimThrows() {
    ThrottleWithoutExternalResource.OffsetRangeTracker tracker =
        createRestriction(0, 10).newTracker();
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(-1));
    assertThat(error.getMessage(), is("Illegal value for offset position: -1, must be [0, 10)"));

    error = assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(10));
    assertThat(error.getMessage(), is("Illegal value for offset position: 10, must be [0, 10)"));
  }

  @Test
  public void givenNonEmpty_positionAtBegin_thenTryClaimTrue_setsCurrentRestriction() {
    ThrottleWithoutExternalResource.OffsetRangeTracker tracker = createNonEmpty().newTracker();
    assertThat(tracker.tryClaim(0), is(true));
    ThrottleWithoutExternalResource.OffsetRange restriction = tracker.currentRestriction();
    assertThat(restriction.getCurrent(), is(0));
    assertThat(restriction.getFromInclusive(), is(-1));
    assertThat(restriction.getToExclusive(), is(100));
  }

  @Test
  public void givenNonEmpty_positionAtEnd_thenTryClaim_isFalse() {
    ThrottleWithoutExternalResource.OffsetRangeTracker tracker =
        createNonEmpty().toBuilder().setCurrent(99).build().newTracker();
    assertThat(tracker.tryClaim(100), is(false));
    ThrottleWithoutExternalResource.OffsetRange restriction = tracker.currentRestriction();
    assertThat(restriction.getCurrent(), is(99));
    assertThat(restriction.getFromInclusive(), is(-1));
    assertThat(restriction.getToExclusive(), is(100));
  }

  @Test
  public void givenNonEmpty_thenTrySplitIsNull() {
    ThrottleWithoutExternalResource.OffsetRangeTracker tracker = createNonEmpty().newTracker();
    assertThat(tracker.trySplit(0.4), nullValue());
  }

  @Test
  public void givenNonEmpty_thenCheckDoneNoop() {
    ThrottleWithoutExternalResource.OffsetRangeTracker tracker = createNonEmpty().newTracker();
    tracker.checkDone();
  }

  @Test
  public void givenNonEmpty_thenIsBounded_Bounded() {
    assertThat(createNonEmpty().newTracker().isBounded(), is(RestrictionTracker.IsBounded.BOUNDED));
  }

  @Test
  public void givenNonEmpty_positionAtStart_thenGetProgressStart() {
    RestrictionTracker.Progress progress = createNonEmpty().newTracker().getProgress();
    assertThat(progress.getWorkCompleted(), is(0.0));
    assertThat(progress.getWorkRemaining(), is(1.0));
  }

  @Test
  public void givenNonEmpty_positionAtMiddle_thenGetProgressMiddle() {
    RestrictionTracker.Progress progress =
        createNonEmpty().toBuilder().setCurrent(49).build().newTracker().getProgress();

    assertThat(progress.getWorkCompleted(), is(0.5));
    assertThat(progress.getWorkRemaining(), is(0.5));
  }

  @Test
  public void givenNonEmpty_positionAtEnd_thenGetProgressEnd() {
    RestrictionTracker.Progress progress =
        createNonEmpty().toBuilder().setCurrent(99).build().newTracker().getProgress();
    assertThat(progress.getWorkCompleted(), is(1.0));
    assertThat(progress.getWorkRemaining(), is(0.0));
  }

  private static ThrottleWithoutExternalResource.OffsetRange createNonEmpty() {
    return ThrottleWithoutExternalResource.OffsetRange.ofSize(100);
  }

  private static ThrottleWithoutExternalResource.OffsetRange createRestriction(int from, int to) {
    return ThrottleWithoutExternalResource.OffsetRange.of(from, to);
  }

  /////////////////////////// End Non Empty tests ////////////////////////
}
