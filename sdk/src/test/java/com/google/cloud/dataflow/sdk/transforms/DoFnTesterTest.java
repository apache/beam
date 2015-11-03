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
package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DoFnTester}.
 */
@RunWith(JUnit4.class)
public class DoFnTesterTest {
  @Test
  public void getAggregatorValuesShouldGetValueOfCounter() {
    CounterDoFn counterDoFn = new CounterDoFn();

    DoFnTester<Long, Void> tester = DoFnTester.of(counterDoFn);
    tester.processBatch(1L, 2L, 4L, 8L);

    Long aggregatorVal = tester.getAggregatorValue(counterDoFn.agg);

    assertThat(aggregatorVal, equalTo(15L));
  }

  @Test
  public void getAggregatorValuesWithEmptyCounterShouldSucceed() {
    CounterDoFn counterDoFn = new CounterDoFn();

    DoFnTester<Long, Void> tester = DoFnTester.of(counterDoFn);
    tester.processBatch();
    Long aggregatorVal = tester.getAggregatorValue(counterDoFn.agg);
    // empty bundle
    assertThat(aggregatorVal, equalTo(0L));
  }

  @Test
  public void getAggregatorValuesInStartFinishBundleShouldGetValues() {
    CounterDoFn fn = new CounterDoFn(1L, 2L);
    DoFnTester<Long, Void> tester = DoFnTester.of(fn);
    tester.processBatch(0L, 0L);

    Long aggValue = tester.getAggregatorValue(fn.agg);
    assertThat(aggValue, equalTo(1L + 2L));
  }

  /**
   * A DoFn that adds values to an aggregator in processElement.
   */
  private static class CounterDoFn extends DoFn<Long, Void> {
    Aggregator<Long, Long> agg = createAggregator("ctr", new Sum.SumLongFn());
    private final long startBundleVal;
    private final long finishBundleVal;

    public CounterDoFn() {
      this(0L, 0L);
    }

    public CounterDoFn(long start, long finish) {
      this.startBundleVal = start;
      this.finishBundleVal = finish;
    }

    @Override
    public void startBundle(DoFn<Long, Void>.Context c) {
      agg.addValue(startBundleVal);
    }

    @Override
    public void processElement(DoFn<Long, Void>.ProcessContext c) throws Exception {
      agg.addValue(c.element());
    }

    @Override
    public void finishBundle(DoFn<Long, Void>.Context c) {
      agg.addValue(finishBundleVal);
    }
  }
}

