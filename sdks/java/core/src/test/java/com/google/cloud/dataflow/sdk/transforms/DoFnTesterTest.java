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
package com.google.cloud.dataflow.sdk.transforms;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.transforms.DoFnTester.OutputElementWithTimestamp;

import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * Tests for {@link DoFnTester}.
 */
@RunWith(JUnit4.class)
public class DoFnTesterTest {

  @Test
  public void processElement() {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);

    tester.processElement(1L);

    List<String> take = tester.takeOutputElements();

    assertThat(take, hasItems("1"));

    // Following takeOutputElements(), neither takeOutputElements()
    // nor peekOutputElements() return anything.
    assertTrue(tester.takeOutputElements().isEmpty());
    assertTrue(tester.peekOutputElements().isEmpty());

    // processElement() caused startBundle() to be called, but finishBundle() was never called.
    CounterDoFn deserializedDoFn = (CounterDoFn) tester.fn;
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertFalse(deserializedDoFn.wasFinishBundleCalled());
  }

  @Test
  public void processElementsWithPeeks() {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);

    // Explicitly call startBundle().
    tester.startBundle();

    // verify startBundle() was called but not finishBundle().
    CounterDoFn deserializedDoFn = (CounterDoFn) tester.fn;
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertFalse(deserializedDoFn.wasFinishBundleCalled());

    // process a couple of elements.
    tester.processElement(1L);
    tester.processElement(2L);

    // peek the first 2 outputs.
    List<String> peek = tester.peekOutputElements();
    assertThat(peek, hasItems("1", "2"));

    // process a couple more.
    tester.processElement(3L);
    tester.processElement(4L);

    // peek all the outputs so far.
    peek = tester.peekOutputElements();
    assertThat(peek, hasItems("1", "2", "3", "4"));
    // take the outputs.
    List<String> take = tester.takeOutputElements();
    assertThat(take, hasItems("1", "2", "3", "4"));

    // Following takeOutputElements(), neither takeOutputElements()
    // nor peekOutputElements() return anything.
    assertTrue(tester.peekOutputElements().isEmpty());
    assertTrue(tester.takeOutputElements().isEmpty());

    // verify finishBundle() hasn't been called yet.
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertFalse(deserializedDoFn.wasFinishBundleCalled());

    // process a couple more.
    tester.processElement(5L);
    tester.processElement(6L);

    // peek and take now have only the 2 last outputs.
    peek = tester.peekOutputElements();
    assertThat(peek, hasItems("5", "6"));
    take = tester.takeOutputElements();
    assertThat(take, hasItems("5", "6"));

    tester.finishBundle();

    // verify finishBundle() was called.
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertTrue(deserializedDoFn.wasFinishBundleCalled());
  }

  @Test
  public void processBatch() {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);

    // processBatch() returns all the output like takeOutputElements().
    List<String> take = tester.processBatch(1L, 2L, 3L, 4L);

    assertThat(take, hasItems("1", "2", "3", "4"));

    // peek now returns nothing.
    assertTrue(tester.peekOutputElements().isEmpty());

    // verify startBundle() and finishBundle() were both called.
    CounterDoFn deserializedDoFn = (CounterDoFn) tester.fn;
    assertTrue(deserializedDoFn.wasStartBundleCalled());
    assertTrue(deserializedDoFn.wasFinishBundleCalled());
  }

  @Test
  public void processElementWithTimestamp() {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);

    tester.processElement(1L);
    tester.processElement(2L);

    List<OutputElementWithTimestamp<String>> peek = tester.peekOutputElementsWithTimestamp();
    OutputElementWithTimestamp<String> one =
        new OutputElementWithTimestamp<>("1", new Instant(1000L));
    OutputElementWithTimestamp<String> two =
        new OutputElementWithTimestamp<>("2", new Instant(2000L));
    assertThat(peek, hasItems(one, two));

    tester.processElement(3L);
    tester.processElement(4L);

    OutputElementWithTimestamp<String> three =
        new OutputElementWithTimestamp<>("3", new Instant(3000L));
    OutputElementWithTimestamp<String> four =
        new OutputElementWithTimestamp<>("4", new Instant(4000L));
    peek = tester.peekOutputElementsWithTimestamp();
    assertThat(peek, hasItems(one, two, three, four));
    List<OutputElementWithTimestamp<String>> take = tester.takeOutputElementsWithTimestamp();
    assertThat(take, hasItems(one, two, three, four));

    // Following takeOutputElementsWithTimestamp(), neither takeOutputElementsWithTimestamp()
    // nor peekOutputElementsWithTimestamp() return anything.
    assertTrue(tester.takeOutputElementsWithTimestamp().isEmpty());
    assertTrue(tester.peekOutputElementsWithTimestamp().isEmpty());

    // peekOutputElements() and takeOutputElements() also return nothing.
    assertTrue(tester.peekOutputElements().isEmpty());
    assertTrue(tester.takeOutputElements().isEmpty());
  }

  @Test
  public void getAggregatorValuesShouldGetValueOfCounter() {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);
    tester.processBatch(1L, 2L, 4L, 8L);

    Long aggregatorVal = tester.getAggregatorValue(counterDoFn.agg);

    assertThat(aggregatorVal, equalTo(15L));
  }

  @Test
  public void getAggregatorValuesWithEmptyCounterShouldSucceed() {
    CounterDoFn counterDoFn = new CounterDoFn();
    DoFnTester<Long, String> tester = DoFnTester.of(counterDoFn);
    tester.processBatch();
    Long aggregatorVal = tester.getAggregatorValue(counterDoFn.agg);
    // empty bundle
    assertThat(aggregatorVal, equalTo(0L));
  }

  @Test
  public void getAggregatorValuesInStartFinishBundleShouldGetValues() {
    CounterDoFn fn = new CounterDoFn(1L, 2L);
    DoFnTester<Long, String> tester = DoFnTester.of(fn);
    tester.processBatch(0L, 0L);

    Long aggValue = tester.getAggregatorValue(fn.agg);
    assertThat(aggValue, equalTo(1L + 2L));
  }

  /**
   * A DoFn that adds values to an aggregator and converts input to String in processElement.
   */
  private static class CounterDoFn extends DoFn<Long, String> {
    Aggregator<Long, Long> agg = createAggregator("ctr", new Sum.SumLongFn());
    private final long startBundleVal;
    private final long finishBundleVal;
    private boolean startBundleCalled;
    private boolean finishBundleCalled;

    public CounterDoFn() {
      this(0L, 0L);
    }

    public CounterDoFn(long start, long finish) {
      this.startBundleVal = start;
      this.finishBundleVal = finish;
    }

    @Override
    public void startBundle(Context c) {
      agg.addValue(startBundleVal);
      startBundleCalled = true;
    }

    @Override
    public void processElement(ProcessContext c) throws Exception {
      agg.addValue(c.element());
      Instant instant = new Instant(1000L * c.element());
      c.outputWithTimestamp(c.element().toString(), instant);
    }

    @Override
    public void finishBundle(Context c) {
      agg.addValue(finishBundleVal);
      finishBundleCalled = true;
    }

    boolean wasStartBundleCalled() {
      return startBundleCalled;
    }

    boolean wasFinishBundleCalled() {
      return finishBundleCalled;
    }
  }
}

