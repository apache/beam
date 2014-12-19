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

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for FlattenOperation.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class FlattenOperationTest {
  @Test
  public void testRunFlattenOperation() throws Exception {
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(
        counterPrefix, counterSet.getAddCounterMutator());
    ExecutorTestUtils.TestReceiver receiver =
        new ExecutorTestUtils.TestReceiver(counterSet, counterPrefix);

    FlattenOperation flattenOperation =
        new FlattenOperation(receiver,
                             counterPrefix, counterSet.getAddCounterMutator(),
                             stateSampler);

    flattenOperation.start();

    flattenOperation.process("hi");
    flattenOperation.process("there");
    flattenOperation.process("");
    flattenOperation.process("bob");

    flattenOperation.finish();

    Assert.assertThat(receiver.outputElems,
                      CoreMatchers.<Object>hasItems("hi", "there", "", "bob"));

    Assert.assertEquals(
        new CounterSet(
            Counter.longs("test-FlattenOperation-start-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-FlattenOperation-start-msecs")).getAggregate(false)),
            Counter.longs("test-FlattenOperation-process-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-FlattenOperation-process-msecs")).getAggregate(false)),
            Counter.longs("test-FlattenOperation-finish-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-FlattenOperation-finish-msecs")).getAggregate(false)),
            Counter.longs("test_receiver_out-ElementCount", SUM)
                .resetToValue(4L),
            Counter.longs("test_receiver_out-MeanByteCount", MEAN)
                .resetToValue(4, 10L)),
        counterSet);
  }
}
