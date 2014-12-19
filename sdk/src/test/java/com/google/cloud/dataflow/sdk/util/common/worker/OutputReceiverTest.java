/*
 * Copyright (C) 2014 Google Inc.
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

import com.google.cloud.dataflow.sdk.coders.CoderException;
import com.google.cloud.dataflow.sdk.coders.StringUtf8Coder;
import com.google.cloud.dataflow.sdk.runners.worker.MapTaskExecutorFactory.ElementByteSizeObservableCoder;
import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;
import com.google.cloud.dataflow.sdk.util.common.CounterTestUtils;
import com.google.cloud.dataflow.sdk.util.common.worker.ExecutorTestUtils.TestReceiver;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for OutputReceiver.
 */
@RunWith(JUnit4.class)
public class OutputReceiverTest {
  // We test OutputReceiver where every element is sampled.
  static class TestOutputReceiver extends OutputReceiver {
    public TestOutputReceiver() {
      this(new CounterSet());
    }

    @SuppressWarnings("rawtypes")
    public TestOutputReceiver(CounterSet counters) {
      super("output_name",
            new ElementByteSizeObservableCoder(StringUtf8Coder.of()),
            "test-",
            counters.getAddCounterMutator());
    }

    @Override
    protected boolean sampleElement() {
      return true;
    }
  }

  @Test
  public void testEmptyOutputReceiver() throws Exception {
    TestOutputReceiver fanOut = new TestOutputReceiver();
    fanOut.process("hi");
    fanOut.process("bob");

    Assert.assertEquals("output_name", fanOut.getName());
    Assert.assertEquals(
        2,
        (long) CounterTestUtils.getTotalAggregate(fanOut.getElementCount()));
    Assert.assertEquals(
        5,
        (long) CounterTestUtils.getTotalAggregate(fanOut.getMeanByteCount()));
    Assert.assertEquals(
        2,
        CounterTestUtils.getTotalCount(fanOut.getMeanByteCount()));
  }

  @Test
  public void testMultipleOutputReceiver() throws Exception {
    TestOutputReceiver fanOut = new TestOutputReceiver();

    CounterSet counters = new CounterSet();
    String counterPrefix = "test-";

    TestReceiver receiver1 = new TestReceiver(counters, counterPrefix);
    fanOut.addOutput(receiver1);

    TestReceiver receiver2 = new TestReceiver(counters, counterPrefix);
    fanOut.addOutput(receiver2);

    fanOut.process("hi");
    fanOut.process("bob");

    Assert.assertEquals("output_name", fanOut.getName());
    Assert.assertEquals(
        2,
        (long) CounterTestUtils.getTotalAggregate(fanOut.getElementCount()));
    Assert.assertEquals(
        5,
        (long) CounterTestUtils.getTotalAggregate(fanOut.getMeanByteCount()));
    Assert.assertEquals(
        2,
        CounterTestUtils.getTotalCount(fanOut.getMeanByteCount()));
    Assert.assertThat(receiver1.outputElems,
                      CoreMatchers.<Object>hasItems("hi", "bob"));
    Assert.assertThat(receiver2.outputElems,
                      CoreMatchers.<Object>hasItems("hi", "bob"));
  }

  @Test(expected = ClassCastException.class)
  public void testIncorrectType() throws Exception {
    TestOutputReceiver fanOut = new TestOutputReceiver();
    fanOut.process(5);
  }

  @Test(expected = CoderException.class)
  public void testNullArgument() throws Exception {
    TestOutputReceiver fanOut = new TestOutputReceiver();
    fanOut.process(null);
  }

  @Test
  public void testAddingCountersIntoCounterSet() throws Exception {
    CounterSet counters = new CounterSet();
    TestOutputReceiver receiver = new TestOutputReceiver(counters);

    Assert.assertEquals(
        new CounterSet(
            Counter.longs("output_name-ElementCount", SUM)
                .resetToValue(0L),
            Counter.longs("output_name-MeanByteCount", MEAN)
                .resetToValue(0, 0L)),
        counters);
  }
}
