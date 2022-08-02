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
package org.apache.beam.runners.dataflow.worker.counters;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Unit tests for {@link CounterSet}. */
@RunWith(JUnit4.class)
public class CounterSetTest {

  private final CounterName name1 = CounterName.named("c1");
  private final CounterName name2 = CounterName.named("c2");

  private CounterSet counterSet = new CounterSet();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testAddWithDifferentNamesAddsAll() {
    counterSet.longSum(name1);
    counterSet.intMax(name2);

    assertThat(counterSet.size(), equalTo(2L));
  }

  @Test
  public void testNonMatchingCounterName() {
    CounterName cn1 = CounterName.named("myCounter1, origin=");
    CounterName cn2 = CounterName.named("myCounter1").withOrigin(", origin=");

    assertThat(cn1.getFlatName(), equalTo(cn1.getFlatName()));
    Counter<?, ?> c1 = counterSet.longSum(cn1);
    assertThat(c1, not(equalTo(counterSet.getExistingCounter(cn2))));
  }

  @Test
  public void testMatchingByCounterName() {
    CounterName cn1 = CounterName.named("myCounter1");
    CounterName cn2 = CounterName.named("myCounter1");

    Counter<?, ?> c1 = counterSet.longSum(cn1);
    assertThat(c1, equalTo(counterSet.getExistingCounter(cn2)));
  }

  @Test
  public void testAddWithAlreadyPresentNameReturnsFalse() {
    Counter<?, ?> c1 = counterSet.longSum(name1);
    Counter<?, ?> c1Dup = counterSet.longSum(name1);

    assertSame(c1, c1Dup);
    assertThat(counterSet.size(), equalTo(1L));
  }

  @Test
  public void testAddOrReuseWithIncompatibleTypesThrowsException() {
    Counter<?, ?> c1 = counterSet.longSum(name1);

    thrown.expect(IllegalArgumentException.class);
    // Should print the existing counter
    thrown.expectMessage(c1.toString());
    // Should print the counter we're trying to create
    thrown.expectMessage(new CounterFactory().doubleSum(name1).toString());
    // Should print the contents of the counter set
    thrown.expectMessage(counterSet.toString());
    counterSet.doubleSum(name1);
  }

  @Test
  public void testExtractUpdates() {
    CounterSet deltaSet = new CounterSet();
    CounterSet cumulativeSet = new CounterSet();

    CounterUpdateExtractor<?> updateExtractor = Mockito.mock(CounterUpdateExtractor.class);

    Counter<Long, Long> delta1 = deltaSet.longSum(name1);
    Counter<Long, Long> cumulative1 = cumulativeSet.longSum(name1);

    // delta counters

    delta1.addValue(10L);

    deltaSet.extractUpdates(true, updateExtractor);
    verify(updateExtractor).longSum(name1, true, 10L);

    delta1.addValue(5L);

    deltaSet.extractUpdates(true, updateExtractor);
    verify(updateExtractor).longSum(name1, true, 5L);

    // no updates to delta counters

    deltaSet.extractUpdates(true, updateExtractor);
    verify(updateExtractor).longSum(name1, true, 0L);

    // cumulative counters

    cumulative1.addValue(10L);

    cumulativeSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor).longSum(name1, false, 10L);

    cumulative1.addValue(5L);

    cumulativeSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor).longSum(name1, false, 15L);

    // no updates to cumulative counters

    cumulativeSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor, times(2)).longSum(name1, false, 15L);

    // test extracting only modified deltas

    Counter<Integer, Integer> delta2 = deltaSet.intSum(name2);

    delta1.addValue(100L);
    delta2.addValue(200);

    deltaSet.extractModifiedDeltaUpdates(updateExtractor);
    verify(updateExtractor).longSum(name1, true, 100L);
    verify(updateExtractor).intSum(name2, true, 200);

    delta1.addValue(1L);
    deltaSet.extractModifiedDeltaUpdates(updateExtractor);
    verify(updateExtractor).longSum(name1, true, 1L);

    delta2.addValue(5);
    deltaSet.extractModifiedDeltaUpdates(updateExtractor);
    verify(updateExtractor).intSum(name2, true, 5);

    verifyNoMoreInteractions(updateExtractor);
  }
}
