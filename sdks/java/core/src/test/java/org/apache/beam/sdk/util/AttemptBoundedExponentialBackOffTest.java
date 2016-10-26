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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.util.BackOff;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link AttemptBoundedExponentialBackOff}. */
@RunWith(JUnit4.class)
@SuppressWarnings("deprecation") // test of deprecated class
public class AttemptBoundedExponentialBackOffTest {
  @Rule public ExpectedException exception = ExpectedException.none();

  @Test
  public void testUsingInvalidInitialInterval() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Initial interval must be greater than zero.");
    new AttemptBoundedExponentialBackOff(10, 0L);
  }

  @Test
  public void testUsingInvalidMaximumNumberOfRetries() throws Exception {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Maximum number of attempts must be greater than zero.");
    new AttemptBoundedExponentialBackOff(-1, 10L);
  }

  @Test
  public void testThatFixedNumberOfAttemptsExits() throws Exception {
    BackOff backOff = new AttemptBoundedExponentialBackOff(3, 500);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }

  @Test
  public void testThatResettingAllowsReuse() throws Exception {
    BackOff backOff = new AttemptBoundedExponentialBackOff(3, 500);
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
    backOff.reset();
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(249L), lessThan(751L)));
    assertThat(backOff.nextBackOffMillis(), allOf(greaterThan(374L), lessThan(1126L)));
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }

  @Test
  public void testAtMaxAttempts() throws Exception {
    AttemptBoundedExponentialBackOff backOff = new AttemptBoundedExponentialBackOff(3, 500);
    assertFalse(backOff.atMaxAttempts());
    backOff.nextBackOffMillis();
    assertFalse(backOff.atMaxAttempts());
    backOff.nextBackOffMillis();
    assertTrue(backOff.atMaxAttempts());
    assertEquals(BackOff.STOP, backOff.nextBackOffMillis());
  }
}
