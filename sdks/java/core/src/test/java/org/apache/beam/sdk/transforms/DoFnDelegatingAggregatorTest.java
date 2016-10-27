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
package org.apache.beam.sdk.transforms;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Tests for {@link DelegatingAggregator}.
 */
@RunWith(JUnit4.class)
public class DoFnDelegatingAggregatorTest {

  @Mock
  private Aggregator<Long, Long> delegate;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testAddValueWithoutDelegateThrowsException() {
    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    String name = "agg";
    CombineFn<Double, ?, Double> combiner = mockCombineFn(Double.class);

    DelegatingAggregator<Double, Double> aggregator =
        (DelegatingAggregator<Double, Double>) doFn.createAggregator(name, combiner);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("cannot be called");
    thrown.expectMessage("DoFn");

    aggregator.addValue(21.2);
  }

  @Test
  public void testSetDelegateThenAddValueCallsDelegate() {
    String name = "agg";
    CombineFn<Long, ?, Long> combiner = mockCombineFn(Long.class);

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    DelegatingAggregator<Long, Long> aggregator =
        (DelegatingAggregator<Long, Long>) doFn.createAggregator(name, combiner);

    aggregator.setDelegate(delegate);

    aggregator.addValue(12L);

    verify(delegate).addValue(12L);
  }

  @Test
  public void testSetDelegateWithExistingDelegateStartsDelegatingToSecond() {
    String name = "agg";
    CombineFn<Double, ?, Double> combiner = mockCombineFn(Double.class);

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    DelegatingAggregator<Double, Double> aggregator =
        (DelegatingAggregator<Double, Double>) doFn.createAggregator(name, combiner);

    @SuppressWarnings("unchecked")
    Aggregator<Double, Double> secondDelegate =
        mock(Aggregator.class, "secondDelegate");

    aggregator.setDelegate(aggregator);
    aggregator.setDelegate(secondDelegate);

    aggregator.addValue(2.25);

    verify(secondDelegate).addValue(2.25);
    verify(delegate, never()).addValue(anyLong());
  }

  @Test
  public void testGetNameReturnsName() {
    String name = "agg";
    CombineFn<Double, ?, Double> combiner = mockCombineFn(Double.class);

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    DelegatingAggregator<Double, Double> aggregator =
        (DelegatingAggregator<Double, Double>) doFn.createAggregator(name, combiner);

    assertEquals(name, aggregator.getName());
  }

  @Test
  public void testGetCombineFnReturnsCombineFn() {
    String name = "agg";
    CombineFn<Double, ?, Double> combiner = mockCombineFn(Double.class);

    OldDoFn<Void, Void> doFn = new NoOpOldDoFn<>();

    DelegatingAggregator<Double, Double> aggregator =
        (DelegatingAggregator<Double, Double>) doFn.createAggregator(name, combiner);

    assertEquals(combiner, aggregator.getCombineFn());
  }

  @SuppressWarnings("unchecked")
  private static <T> CombineFn<T, ?, T> mockCombineFn(
      @SuppressWarnings("unused") Class<T> clazz) {
    return mock(CombineFn.class);
  }

}
