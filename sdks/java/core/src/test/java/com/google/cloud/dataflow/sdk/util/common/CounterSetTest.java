/*******************************************************************************
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MAX;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Unit tests for {@link CounterSet}.
 */
@RunWith(JUnit4.class)
public class CounterSetTest {
  private CounterSet set;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    set = new CounterSet();
  }

  @Test
  public void testAddWithDifferentNamesAddsAll() {
    Counter<?> c1 = Counter.longs("c1", SUM);
    Counter<?> c2 = Counter.ints("c2", MAX);

    boolean c1Add = set.add(c1);
    boolean c2Add = set.add(c2);

    assertTrue(c1Add);
    assertTrue(c2Add);
    assertThat(set, containsInAnyOrder(c1, c2));
  }

  @Test
  public void testAddWithAlreadyPresentNameReturnsFalse() {
    Counter<?> c1 = Counter.longs("c1", SUM);
    Counter<?> c1Dup = Counter.longs("c1", SUM);

    boolean c1Add = set.add(c1);
    boolean c1DupAdd = set.add(c1Dup);

    assertTrue(c1Add);
    assertFalse(c1DupAdd);
    assertThat(set, containsInAnyOrder((Counter) c1));
  }

  @Test
  public void testAddOrReuseWithAlreadyPresentReturnsPresent() {
    Counter<?> c1 = Counter.longs("c1", SUM);
    Counter<?> c1Dup = Counter.longs("c1", SUM);

    Counter<?> c1AddResult = set.addOrReuseCounter(c1);
    Counter<?> c1DupAddResult = set.addOrReuseCounter(c1Dup);

    assertSame(c1, c1AddResult);
    assertSame(c1AddResult, c1DupAddResult);
    assertThat(set, containsInAnyOrder((Counter) c1));
  }

  @Test
  public void testAddOrReuseWithNoCounterReturnsProvided() {
    Counter<?> c1 = Counter.longs("c1", SUM);

    Counter<?> c1AddResult = set.addOrReuseCounter(c1);

    assertSame(c1, c1AddResult);
    assertThat(set, containsInAnyOrder((Counter) c1));
  }

  @Test
  public void testAddOrReuseWithIncompatibleTypesThrowsException() {
    Counter<?> c1 = Counter.longs("c1", SUM);
    Counter<?> c1Incompatible = Counter.ints("c1", MAX);

    set.addOrReuseCounter(c1);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Counter " + c1Incompatible
        + " duplicates incompatible counter " + c1 + " in " + set);

    set.addOrReuseCounter(c1Incompatible);
  }

  @Test
  public void testMergeWithDifferentNamesAddsAll() {
    Counter<?> c1 = Counter.longs("c1", SUM);
    Counter<?> c2 = Counter.ints("c2", MAX);

    set.add(c1);
    set.add(c2);

    CounterSet newSet = new CounterSet();
    newSet.merge(set);

    assertThat(newSet, containsInAnyOrder(c1, c2));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMergeWithSameNamesMerges() {
    Counter<Long> c1 = Counter.longs("c1", SUM);
    Counter<Integer> c2 = Counter.ints("c2", MAX);

    set.add(c1);
    set.add(c2);

    c1.addValue(3L);
    c2.addValue(22);

    CounterSet newSet = new CounterSet();
    Counter<Long> c1Prime = Counter.longs("c1", SUM);
    Counter<Integer> c2Prime = Counter.ints("c2", MAX);

    c1Prime.addValue(7L);
    c2Prime.addValue(14);

    newSet.add(c1Prime);
    newSet.add(c2Prime);

    newSet.merge(set);

    assertThat((Counter<Long>) newSet.getExistingCounter("c1"), equalTo(c1Prime));
    assertThat((Long) newSet.getExistingCounter("c1").getAggregate(), equalTo(10L));

    assertThat((Counter<Integer>) newSet.getExistingCounter("c2"), equalTo(c2Prime));
    assertThat((Integer) newSet.getExistingCounter("c2").getAggregate(), equalTo(22));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testMergeWithIncompatibleTypesThrowsException() {
    Counter<Long> c1 = Counter.longs("c1", SUM);

    set.add(c1);

    CounterSet newSet = new CounterSet();
    Counter<Long> c1Prime = Counter.longs("c1", MAX);

    newSet.add(c1Prime);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("c1");
    thrown.expectMessage("incompatible counters with the same name");

    newSet.merge(set);
  }

  @Test
  public void testAddCounterMutatorAddCounterAddsCounter() {
    Counter<?> c1 = Counter.longs("c1", SUM);

    Counter<?> addC1Result = set.getAddCounterMutator().addCounter(c1);

    assertSame(c1, addC1Result);
    assertThat(set, containsInAnyOrder((Counter) c1));
  }

  @Test
  public void testAddCounterMutatorAddEqualCounterReusesCounter() {
    Counter<?> c1 = Counter.longs("c1", SUM);
    Counter<?> c1dup = Counter.longs("c1", SUM);

    Counter<?> addC1Result = set.getAddCounterMutator().addCounter(c1);
    Counter<?> addC1DupResult = set.getAddCounterMutator().addCounter(c1dup);

    assertThat(set, containsInAnyOrder((Counter) c1));
    assertSame(c1, addC1Result);
    assertSame(c1, addC1DupResult);
  }

  @Test
  public void testAddCounterMutatorIncompatibleTypesThrowsException() {
    Counter<?> c1 = Counter.longs("c1", SUM);
    Counter<?> c1Incompatible = Counter.longs("c1", MAX);

    set.getAddCounterMutator().addCounter(c1);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Counter " + c1Incompatible
        + " duplicates incompatible counter " + c1 + " in " + set);

    set.getAddCounterMutator().addCounter(c1Incompatible);
  }

  @Test
  public void testAddCounterMutatorAddMultipleCounters() {
    Counter<?> c1 = Counter.longs("c1", SUM);
    Counter<?> c2 = Counter.longs("c2", MAX);

    set.getAddCounterMutator().addCounter(c1);
    set.getAddCounterMutator().addCounter(c2);

    assertThat(set, containsInAnyOrder(c1, c2));
  }

}
