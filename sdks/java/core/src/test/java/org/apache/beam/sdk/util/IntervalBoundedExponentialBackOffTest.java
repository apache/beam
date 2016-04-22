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
package org.apache.beam.sdk.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link IntervalBoundedExponentialBackOff}. */
@RunWith(JUnit4.class)
public class IntervalBoundedExponentialBackOffTest {
  @Rule public ExpectedException exception = ExpectedException.none();


  @Test
  public void testUsingInvalidInitialInterval() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Initial interval must be greater than zero.");
    new IntervalBoundedExponentialBackOff(1000L, 0L);
  }

  @Test
  public void testUsingInvalidMaximumInterval() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Maximum interval must be greater than zero.");
    new IntervalBoundedExponentialBackOff(-1L, 10L);
  }

  @Test
  public void testThatcertainNumberOfAttemptsReachesMaxInterval() throws Exception {
    IntervalBoundedExponentialBackOff backOff = new IntervalBoundedExponentialBackOff(1000L, 500);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(374L), lessThan(1126L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
  }

  @Test
  public void testThatResettingAllowsReuse() throws Exception {
    IntervalBoundedExponentialBackOff backOff = new IntervalBoundedExponentialBackOff(1000L, 500);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(374L), lessThan(1126L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(374L), lessThan(1126L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
  }

  @Test
  public void testAtMaxInterval() throws Exception {
    IntervalBoundedExponentialBackOff backOff = new IntervalBoundedExponentialBackOff(1000L, 500);
    assertFalse(backOff.atMaxInterval());
    backOff.nextBackOffMillis();
    assertFalse(backOff.atMaxInterval());
    backOff.nextBackOffMillis();
    assertTrue(backOff.atMaxInterval());
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L),
        lessThanOrEqualTo(1500L)));
  }
}
