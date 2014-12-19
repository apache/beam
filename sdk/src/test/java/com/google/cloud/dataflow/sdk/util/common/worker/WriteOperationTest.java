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

import static com.google.cloud.dataflow.sdk.util.common.Counter.AggregationKind.SUM;

import com.google.cloud.dataflow.sdk.util.common.Counter;
import com.google.cloud.dataflow.sdk.util.common.CounterSet;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for WriteOperation.
 */
@RunWith(JUnit4.class)
public class WriteOperationTest {
  @Test
  @SuppressWarnings("unchecked")
  public void testRunWriteOperation() throws Exception {
    ExecutorTestUtils.TestSink sink = new ExecutorTestUtils.TestSink();
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(
        counterPrefix, counterSet.getAddCounterMutator());

    WriteOperation writeOperation = new WriteOperation(
        sink, counterPrefix, counterSet.getAddCounterMutator(), stateSampler);

    writeOperation.start();

    writeOperation.process("hi");
    writeOperation.process("there");
    writeOperation.process("");
    writeOperation.process("bob");

    writeOperation.finish();

    Assert.assertThat(sink.outputElems,
                      CoreMatchers.hasItems("hi", "there", "", "bob"));

    Assert.assertEquals(
        new CounterSet(
            Counter.longs("WriteOperation-ByteCount", SUM)
                .resetToValue(2L + 5 + 0 + 3),
            Counter.longs("test-WriteOperation-start-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-WriteOperation-start-msecs")).getAggregate(false)),
            Counter.longs("test-WriteOperation-process-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-WriteOperation-process-msecs")).getAggregate(false)),
            Counter.longs("test-WriteOperation-finish-msecs", SUM)
                .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                    "test-WriteOperation-finish-msecs")).getAggregate(false))),
        counterSet);
  }
}
