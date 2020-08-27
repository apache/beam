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
package org.apache.beam.sdk.extensions.euphoria.core.translate.collector;

import java.util.Map;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Counter;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.Histogram;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SingleJvmAccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.testkit.accumulators.SingleJvmAccumulatorProvider.Factory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** {@link SingleValueCollector} unit tests. */
@RunWith(JUnit4.class)
public class SingleValueCollectorTest {

  private static final String TEST_COUNTER_NAME = "test-counter";
  private static final String TEST_HISTOGRAM_NAME = "test-histogram";

  private final Factory accumulatorFactory = SingleJvmAccumulatorProvider.Factory.get();

  @Test
  public void testBasicAccumulatorsAccess() {

    final AccumulatorProvider accumulators = accumulatorFactory.create();

    SingleValueCollector collector = new SingleValueCollector(accumulators, "test-no_op_name");

    Counter counter = collector.getCounter(TEST_COUNTER_NAME);
    Assert.assertNotNull(counter);
    Histogram histogram = collector.getHistogram(TEST_HISTOGRAM_NAME);
    Assert.assertNotNull(histogram);

    // collector.getTimer() <- not yet supported
  }

  @Test
  public void testBasicAccumulatorsFunction() {
    final AccumulatorProvider accumulators = accumulatorFactory.create();

    SingleValueCollector collector = new SingleValueCollector(accumulators, "test-no_op_name");

    Counter counter = collector.getCounter(TEST_COUNTER_NAME);
    Assert.assertNotNull(counter);

    counter.increment();
    counter.increment(2);

    Map<String, Long> counterSnapshots = accumulatorFactory.getCounterSnapshots();
    long counteValue = counterSnapshots.get(TEST_COUNTER_NAME);
    Assert.assertEquals(3L, counteValue);

    Histogram histogram = collector.getHistogram(TEST_HISTOGRAM_NAME);
    Assert.assertNotNull(histogram);

    histogram.add(1);
    histogram.add(2, 2);

    Map<String, Map<Long, Long>> histogramSnapshots = accumulatorFactory.getHistogramSnapshots();
    Map<Long, Long> histogramValue = histogramSnapshots.get(TEST_HISTOGRAM_NAME);

    long numOfValuesOfOne = histogramValue.get(1L);
    Assert.assertEquals(1L, numOfValuesOfOne);
    long numOfValuesOfTwo = histogramValue.get(2L);
    Assert.assertEquals(2L, numOfValuesOfTwo);

    // collector.getTimer() <- not yet supported
  }

  /** need to delete all metrics from accumulator before running another test. */
  @After
  public void cleanUp() {
    accumulatorFactory.clear();
  }
}
