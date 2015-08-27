/*
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
 */

package com.google.cloud.dataflow.sdk.util.common.worker;

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.MEAN;
import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;
import static com.google.cloud.dataflow.sdk.util.common.worker.TestOutputReceiver.TestOutputCounter.getMeanByteCounterName;
import static com.google.cloud.dataflow.sdk.util.common.worker.TestOutputReceiver.TestOutputCounter.getObjectCounterName;
import static org.junit.Assert.assertEquals;

import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.ElementByteSizeObservableCoder;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.Counter.CounterMean;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.worker.TestOutputReceiver.TestOutputCounter;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Tests for {@link OutputObjectAndByteCounter}.
 */
@RunWith(JUnit4.class)
public class OutputObjectAndByteCounterTest {
  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  @Test
  public void testUpdate() throws Exception {
    TestOutputCounter outputCounter = new TestOutputCounter();
    outputCounter.update("hi");
    outputCounter.finishLazyUpdate("hi");
    outputCounter.update("bob");
    outputCounter.finishLazyUpdate("bob");

    CounterMean<Long> meanByteCount = outputCounter.getMeanByteCount().getMean();
    assertEquals(5, (long) meanByteCount.getAggregate());
    assertEquals(2, meanByteCount.getCount());
  }

  @Test
  public void testIncorrectType() throws Exception {
    TestOutputCounter outputCounter = new TestOutputCounter();
    thrown.expect(ClassCastException.class);
    outputCounter.update(5);
  }

  @Test
  public void testNullArgument() throws Exception {
    TestOutputCounter outputCounter = new TestOutputCounter();
    thrown.expect(CoderException.class);
    outputCounter.update(null);
  }

  @Test
  public void testAddingCountersIntoCounterSet() throws Exception {
    CounterSet counters = new CounterSet();
    new TestOutputCounter(counters);

    assertEquals(
        new CounterSet(
            Counter.longs(getMeanByteCounterName("output_name"), MEAN).resetMeanToValue(0, 0L),
            Counter.longs(getObjectCounterName("output_name"), SUM).resetToValue(0L)),
        counters);
  }

  private OutputObjectAndByteCounter makeCounter(int samplingPeriod, int seed) {
    return new OutputObjectAndByteCounter(
               new ElementByteSizeObservableCoder<>(StringUtf8Coder.of()),
               new CounterSet().getAddCounterMutator())
        .setSamplingPeriod(samplingPeriod)
        .setRandom(new Random(seed))
        .countBytes("byte_count");
  }

  @Test
  public void testSimpleByteCount() throws Exception {
    OutputObjectAndByteCounter counter = makeCounter(1, 0);
    for (int i = 0; i < 10000; i++) {
      counter.update("foo");
    }
    assertEquals(30000L, (long) counter.getByteCount().getAggregate());
  }

  @Test
  public void testSamplingByteCountFewElements() throws Exception {
    OutputObjectAndByteCounter counter = makeCounter(100, 0);
    for (int i = 0; i < 10; i++) {
      counter.update("foo");
    }
    assertEquals(30L, (long) counter.getByteCount().getAggregate());
  }

  @Test
  public void testSamplingByteCount() throws Exception {
    List<Long> expected = Arrays.asList(3007230L, 2984989L, 3010540L);
    for (int n = 0; n < 3; n++) {
      OutputObjectAndByteCounter counter = makeCounter(100, n);
      for (int i = 0; i < 1000000; i++) {
        counter.update("foo");
      }
      assertEquals(expected.get(n), counter.getByteCount().getAggregate());
    }
  }
}
