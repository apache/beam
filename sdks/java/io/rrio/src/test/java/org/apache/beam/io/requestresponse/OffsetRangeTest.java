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
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ThrottleWithoutExternalResource.OffsetRange}. */
@RunWith(JUnit4.class)
public class OffsetRangeTest {

  @Test
  public void givenInvalidRange_thenThrows() {
    // [x, y): x == y
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> createRestriction(0, 0));
    assertThat(error.getMessage(), is("Malformed range [0, 0)"));

    // [x, y): x > y
    error = assertThrows(IllegalArgumentException.class, () -> createRestriction(1, 0));
    assertThat(error.getMessage(), is("Malformed range [1, 0)"));
  }

  /////////////////////////// Empty tests //////////////////////////
  @Test
  public void empty() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createEmptyRestriction();
    assertThat(restriction.getCurrent(), is(-1));
    assertThat(restriction.getFromInclusive(), is(-1));
    assertThat(restriction.getToExclusive(), is(0));
  }

  @Test
  public void givenEmpty_thenGetFractionOfIsZero() {
    for (double f = 0.0; f < 1.0; f += 0.1) {
      assertThat(createEmptyRestriction().getFractionOf(f), is(0));
    }
  }

  @Test
  public void givenEmpty_thenNotHasMoreOffset() {
    assertThat(createEmptyRestriction().hasMoreOffset(), is(false));
  }

  @Test
  public void givenEmpty_thenGetSizeZero() {
    assertThat(createEmptyRestriction().getSize(), is(0));
  }

  @Test
  public void givenEmpty_thenGetProgressZero() {
    assertThat(createEmptyRestriction().getProgress(), is(0));
  }

  @Test
  public void givenEmpty_thenGetRemainingZero() {
    assertThat(createEmptyRestriction().getRemaining(), is(0));
  }

  @Test
  public void givenEmpty_thenGetFractionProgressZero() {
    assertThat(createEmptyRestriction().getFractionProgress(), is(0.0));
  }

  @Test
  public void givenEmpty_thenGetFractionRemainingZero() {
    assertThat(createEmptyRestriction().getFractionRemaining(), is(0.0));
  }

  @Test
  public void givenEmpty_thenOffsetByOneThrowsError() {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> createEmptyRestriction().offset(1));
    assertThat(error.getMessage(), is("Illegal value for offset position: 1, must be [-1, 0)"));
  }

  private static ThrottleWithoutExternalResource.OffsetRange createEmptyRestriction() {
    return ThrottleWithoutExternalResource.OffsetRange.empty();
  }

  /////////////////////////// End Empty tests ////////////////////////////

  /////////////////////////// Start Non Empty tests //////////////////////

  @Test
  public void testNonEmpty() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    assertThat(restriction.getFromInclusive(), is(-1));
    assertThat(restriction.getCurrent(), is(-1));
    assertThat(restriction.getToExclusive(), is(100));
  }

  @Test
  public void givenNonEmpty_testGetFractionOf() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    for (double f = 0.00; f <= 1.00; f += 0.01) {
      int want = Double.valueOf(f * 100).intValue();
      int got = restriction.getFractionOf(f);
      assertThat(got, is(want));
    }
  }

  @Test
  public void givenNonEmpty_currentAtStart_thenTestHasMoreOffset() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    assertThat(restriction.hasMoreOffset(), is(true));
  }

  @Test
  public void givenNonEmpty_currentAtEnd_thenTestNotHasMoreOffset() {
    ThrottleWithoutExternalResource.OffsetRange restriction =
        createNonEmpty().toBuilder().setCurrent(99).build();
    assertThat(restriction.hasMoreOffset(), is(false));
  }

  @Test
  public void givenNonEmpty_testGetSize() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    assertThat(restriction.getSize(), is(100));
  }

  @Test
  public void givenNonEmpty_currentAtStart_thenGetProgressIsZero() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    assertThat(restriction.getProgress(), is(0));
  }

  @Test
  public void givenNonEmpty_currentAtEnd_thenGetProgressIsSize() {
    ThrottleWithoutExternalResource.OffsetRange restriction =
        createNonEmpty().toBuilder().setCurrent(99).build();
    assertThat(restriction.getProgress(), is(100));
  }

  @Test
  public void givenNonEmpty_currentAtStart_thenGetRemainingIsSize() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    assertThat(restriction.getRemaining(), is(100));
  }

  @Test
  public void givenNonEmpty_currentAtEnd_thenGetRemainingIsZero() {
    ThrottleWithoutExternalResource.OffsetRange restriction =
        createNonEmpty().toBuilder().setCurrent(99).build();
    assertThat(restriction.getRemaining(), is(0));
  }

  @Test
  public void givenNonEmpty_currentAtStart_thenGetFractionProgressIsZero() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    assertThat(restriction.getFractionProgress(), is(0.0));
  }

  @Test
  public void givenNonEmpty_currentAtEnd_thenGetFractionProgressIsOne() {
    ThrottleWithoutExternalResource.OffsetRange restriction =
        createNonEmpty().toBuilder().setCurrent(99).build();
    assertThat(restriction.getFractionProgress(), is(1.0));
  }

  @Test
  public void givenNonEmpty_currentAtStart_thenGetFractionRemainingIsOne() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    assertThat(restriction.getFractionRemaining(), is(1.0));
  }

  @Test
  public void givenNonEmpty_currentAtEnd_thenGetFractionRemainingIsZero() {
    ThrottleWithoutExternalResource.OffsetRange restriction =
        createNonEmpty().toBuilder().setCurrent(99).build();
    assertThat(restriction.getFractionRemaining(), is(0.0));
  }

  @Test
  public void givenNonEmpty_positionExceedsRange_thenOffsetThrows() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createRestriction(1, 10);
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> restriction.offset(0));
    assertThat(error.getMessage(), is("Illegal value for offset position: 0, must be [1, 10)"));
    error = assertThrows(IllegalArgumentException.class, () -> restriction.offset(10));
    assertThat(error.getMessage(), is("Illegal value for offset position: 10, must be [1, 10)"));
  }

  @Test
  public void givenNonEmpty_positionWithinRange_thenGetCurrentIncr() {
    ThrottleWithoutExternalResource.OffsetRange restriction = createNonEmpty();
    int wantSize = 100;
    int wantPosition = -1;
    while (wantSize > 0) {
      wantSize--;
      wantPosition++;
      restriction = restriction.offset(restriction.getCurrent() + 1);
      assertThat(restriction.getCurrent(), is(wantPosition));
    }
    assertThat(restriction.hasMoreOffset(), is(false));
  }

  private static ThrottleWithoutExternalResource.OffsetRange createNonEmpty() {
    return ThrottleWithoutExternalResource.OffsetRange.ofSize(100);
  }

  private static ThrottleWithoutExternalResource.OffsetRange createRestriction(int from, int to) {
    return ThrottleWithoutExternalResource.OffsetRange.of(from, to);
  }

  /////////////////////////// End Non Empty tests ////////////////////////
}
