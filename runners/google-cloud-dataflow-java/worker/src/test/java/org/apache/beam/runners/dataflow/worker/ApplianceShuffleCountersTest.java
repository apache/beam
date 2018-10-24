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
package org.apache.beam.runners.dataflow.worker;

import static org.apache.beam.runners.dataflow.worker.counters.CounterName.named;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.apache.beam.runners.dataflow.worker.counters.Counter;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.CounterName;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests for ApplianceShuffleCounters. */
@RunWith(JUnit4.class)
public class ApplianceShuffleCountersTest {
  private static final String DATASET_ID = "dataset";
  private CounterSet counterSet = new CounterSet();

  private ApplianceShuffleCounters counters;

  private CounterUpdateExtractor<?> mockUpdateExtractor =
      Mockito.mock(CounterUpdateExtractor.class);
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    resetCounters();
  }

  private void resetCounters() {
    this.counters =
        new ApplianceShuffleCounters(
            counterSet, NameContextsForTests.nameContextForTest(), DATASET_ID);
  }

  Counter<Long, Long> getCounter(String name) throws Exception {
    @SuppressWarnings("unchecked")
    Counter<Long, Long> counter =
        (Counter<Long, Long>) counterSet.getExistingCounter("stageName-systemName-dataset-" + name);
    return counter;
  }

  @Test
  public void testSingleCounter() throws Exception {
    String[] names = {"sum_counter"};
    String[] kinds = {"sum"};
    long[] deltas = {122};
    counters.importCounters(names, kinds, deltas);

    counterSet.extractUpdates(false, mockUpdateExtractor);
    verify(mockUpdateExtractor)
        .longSum(named("stageName-systemName-dataset-sum_counter"), false, 122L);
    verifyNoMoreInteractions(mockUpdateExtractor);
  }

  @Test
  public void testSingleCounterMultipleDeltas() throws Exception {
    String[] names = {"sum_counter", "sum_counter"};
    String[] kinds = {"sum", "sum"};
    long[] deltas = {122, 78};
    counters.importCounters(names, kinds, deltas);

    counterSet.extractUpdates(false, mockUpdateExtractor);
    verify(mockUpdateExtractor)
        .longSum(named("stageName-systemName-dataset-sum_counter"), false, 200L);
    verifyNoMoreInteractions(mockUpdateExtractor);
  }

  @Test
  public void testMultipleCounters() throws Exception {
    String[] names = {"sum_counter", "max_counter", "min_counter"};
    String[] kinds = {"sum", "max", "min"};
    long[] deltas = {100, 200, 300};
    counters.importCounters(names, kinds, deltas);
    Counter<Long, Long> sumCounter = getCounter("sum_counter");
    assertNotNull(sumCounter);

    counterSet.extractUpdates(false, mockUpdateExtractor);
    verify(mockUpdateExtractor)
        .longSum(named("stageName-systemName-dataset-sum_counter"), false, 100L);
    verify(mockUpdateExtractor)
        .longMax(named("stageName-systemName-dataset-max_counter"), false, 200L);
    verify(mockUpdateExtractor)
        .longMin(named("stageName-systemName-dataset-min_counter"), false, 300L);
    verifyNoMoreInteractions(mockUpdateExtractor);
  }

  @Test
  public void testSinglePreexistingCounter() throws Exception {
    Counter<Long, Long> sumCounter =
        counterSet.longSum(CounterName.named("stageName-systemName-dataset-sum_counter"));
    sumCounter.addValue(1000L);
    String[] names = {"sum_counter"};
    String[] kinds = {"sum"};
    long[] deltas = {122};
    counters.importCounters(names, kinds, deltas);

    counterSet.extractUpdates(false, mockUpdateExtractor);
    verify(mockUpdateExtractor)
        .longSum(named("stageName-systemName-dataset-sum_counter"), false, 1122L);
    verifyNoMoreInteractions(mockUpdateExtractor);
  }

  @Test
  public void testArrayLengthMismatch() throws Exception {
    String[] names = {"sum_counter"};
    String[] kinds = {"sum", "max"};
    long[] deltas = {122};
    try {
      counters.importCounters(names, kinds, deltas);
    } catch (AssertionError e) {
      // expected
    }
  }

  @Test
  public void testUnsupportedKind() throws Exception {
    String[] names = {"sum_counter"};
    String[] kinds = {"sum_int"};
    long[] deltas = {122};
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("sum_int");
    counters.importCounters(names, kinds, deltas);
  }
}
