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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link FluentBackoff}. */
@RunWith(JUnit4.class)
public class FluentBackoffTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  private final FluentBackoff defaultBackoff = FluentBackoff.DEFAULT;

  @Test
  public void testInvalidExponent() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("exponent -2.0 must be greater than 0");
    defaultBackoff.withExponent(-2.0);
  }

  @Test
  public void testInvalidInitialBackoff() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("initialBackoff PT0S must be at least 1 millisecond");
    defaultBackoff.withInitialBackoff(Duration.ZERO);
  }

  @Test
  public void testInvalidMaxBackoff() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("maxBackoff PT0S must be at least 1 millisecond");
    defaultBackoff.withMaxBackoff(Duration.ZERO);
  }

  @Test
  public void testInvalidMaxRetries() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("maxRetries -1 cannot be negative");
    defaultBackoff.withMaxRetries(-1);
  }

  @Test
  public void testInvalidCumulativeBackoff() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("maxCumulativeBackoff PT-0.002S must be at least 1 millisecond");
    defaultBackoff.withMaxCumulativeBackoff(Duration.millis(-2));
  }

  /** Tests with bounded interval, custom exponent, and unlimited retries. */
  @Test
  public void testBoundedIntervalWithReset() throws Exception {
    BackOff backOff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.millis(500))
            .withMaxBackoff(Duration.standardSeconds(1))
            .backoff();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(374L), lessThan(1126L)));
    assertThat(
        backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L), lessThanOrEqualTo(1500L)));
    assertThat(
        backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L), lessThanOrEqualTo(1500L)));
    assertThat(
        backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L), lessThanOrEqualTo(1500L)));
    assertThat(
        backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L), lessThanOrEqualTo(1500L)));
    assertThat(
        backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L), lessThanOrEqualTo(1500L)));

    // Reset, should go back to short times.
    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(374L), lessThan(1126L)));
    assertThat(
        backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L), lessThanOrEqualTo(1500L)));
    assertThat(
        backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(500L), lessThanOrEqualTo(1500L)));
  }

  /** Tests with bounded interval, custom exponent, limited retries, and a reset. */
  @Test
  public void testMaxRetriesWithReset() throws Exception {
    BackOff backOff =
        FluentBackoff.DEFAULT.withInitialBackoff(Duration.millis(500)).withMaxRetries(1).backoff();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), equalTo(BackOff.STOP));
    assertThat(backOff.nextBackOffMillis(), equalTo(BackOff.STOP));
    assertThat(backOff.nextBackOffMillis(), equalTo(BackOff.STOP));
    assertThat(backOff.nextBackOffMillis(), equalTo(BackOff.STOP));

    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThanOrEqualTo(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), equalTo(BackOff.STOP));
  }

  private static long countMaximumBackoff(BackOff backOff) throws IOException {
    long cumulativeBackoffMillis = 0;
    long currentBackoffMillis = backOff.nextBackOffMillis();
    while (currentBackoffMillis != BackOff.STOP) {
      cumulativeBackoffMillis += currentBackoffMillis;
      currentBackoffMillis = backOff.nextBackOffMillis();
    }
    return cumulativeBackoffMillis;
  }

  /** Tests with bounded interval, custom exponent, limited cumulative time, and a reset. */
  @Test
  public void testBoundedIntervalAndCumTimeWithReset() throws Exception {
    BackOff backOff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.millis(500))
            .withMaxBackoff(Duration.standardSeconds(1))
            .withMaxCumulativeBackoff(Duration.standardMinutes(1))
            .backoff();

    assertThat(countMaximumBackoff(backOff), equalTo(Duration.standardMinutes(1).getMillis()));

    backOff.reset();
    assertThat(countMaximumBackoff(backOff), equalTo(Duration.standardMinutes(1).getMillis()));
    // sanity check: should get 0 if we don't reset
    assertThat(countMaximumBackoff(backOff), equalTo(0L));

    backOff.reset();
    assertThat(countMaximumBackoff(backOff), equalTo(Duration.standardMinutes(1).getMillis()));
  }

  /** Tests with bounded interval, custom exponent, limited cumulative time and retries. */
  @Test
  public void testBoundedIntervalAndCumTimeAndRetriesWithReset() throws Exception {
    BackOff backOff =
        FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.millis(500))
            .withMaxBackoff(Duration.standardSeconds(1))
            .withMaxCumulativeBackoff(Duration.standardMinutes(1))
            .backoff();

    long cumulativeBackoffMillis = 0;
    long currentBackoffMillis = backOff.nextBackOffMillis();
    while (currentBackoffMillis != BackOff.STOP) {
      cumulativeBackoffMillis += currentBackoffMillis;
      currentBackoffMillis = backOff.nextBackOffMillis();
    }
    assertThat(cumulativeBackoffMillis, equalTo(Duration.standardMinutes(1).getMillis()));
  }

  @Test
  public void testFluentBackoffToString() throws IOException {
    FluentBackoff config =
        FluentBackoff.DEFAULT
            .withExponent(3.4)
            .withMaxRetries(4)
            .withInitialBackoff(Duration.standardSeconds(3))
            .withMaxBackoff(Duration.standardHours(1))
            .withMaxCumulativeBackoff(Duration.standardDays(1));

    assertEquals(
        "FluentBackoff{exponent=3.4, initialBackoff=PT3S, maxBackoff=PT3600S,"
            + " maxRetries=4, maxCumulativeBackoff=PT86400S}",
        config.toString());
  }

  @Test
  public void testBackoffImplToString() throws IOException {
    FluentBackoff config =
        FluentBackoff.DEFAULT
            .withExponent(3.4)
            .withMaxRetries(4)
            .withInitialBackoff(Duration.standardSeconds(3))
            .withMaxBackoff(Duration.standardHours(1))
            .withMaxCumulativeBackoff(Duration.standardDays(1));
    BackOff backOff = config.backoff();

    assertEquals(
        "BackoffImpl{backoffConfig="
            + config.toString()
            + ","
            + " currentRetry=0, currentCumulativeBackoff=PT0S}",
        backOff.toString());

    // backoff once, ignoring result
    backOff.nextBackOffMillis();

    // currentRetry is exact, we can test it.
    assertThat(backOff.toString(), containsString("currentRetry=1"));
    // currentCumulativeBackoff is not exact; we cannot even check that it's non-zero (randomness).
  }
}
