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
 * Tests for ParDoOperation.
 */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class ParDoOperationTest {
  static class TestParDoFn extends ParDoFn {
    final OutputReceiver outputReceiver;

    public TestParDoFn(OutputReceiver outputReceiver) {
      this.outputReceiver = outputReceiver;
    }

    @Override
    public void startBundle(final Receiver... receivers) throws Exception {
      if (receivers.length != 1) {
        throw new AssertionError(
            "unexpected number of receivers for DoFn");
      }

      outputReceiver.process("x-start");
    }

    @Override
    public void processElement(Object elem) throws Exception {
      outputReceiver.process("y-" + elem);
    }

    @Override
    public void finishBundle() throws Exception {
      outputReceiver.process("z-finish");
    }
  }

  @Test
  public void testRunParDoOperation() throws Exception {
    CounterSet counterSet = new CounterSet();
    String counterPrefix = "test-";
    StateSampler stateSampler = new StateSampler(
        counterPrefix, counterSet.getAddCounterMutator());
    ExecutorTestUtils.TestReceiver receiver =
        new ExecutorTestUtils.TestReceiver(counterSet);

    ParDoOperation parDoOperation =
        new ParDoOperation(
            "ParDoOperation",
            new TestParDoFn(receiver),
            new OutputReceiver[]{ receiver },
            counterPrefix,
            counterSet.getAddCounterMutator(),
            stateSampler);

    parDoOperation.start();

    parDoOperation.process("hi");
    parDoOperation.process("there");
    parDoOperation.process("");
    parDoOperation.process("bob");

    parDoOperation.finish();

    Assert.assertThat(
        receiver.outputElems,
        CoreMatchers.<Object>hasItems(
            "x-start", "y-hi", "y-there", "y-", "y-bob", "z-finish"));

    Assert.assertEquals(
        new CounterSet(
            Counter.longs("test-ParDoOperation-start-msecs", SUM)
              .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                  "test-ParDoOperation-start-msecs")).getAggregate(false)),
            Counter.longs("test-ParDoOperation-process-msecs", SUM)
              .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                  "test-ParDoOperation-process-msecs")).getAggregate(false)),
            Counter.longs("test-ParDoOperation-finish-msecs", SUM)
              .resetToValue(((Counter<Long>) counterSet.getExistingCounter(
                  "test-ParDoOperation-finish-msecs")).getAggregate(false)),
            Counter.longs("test_receiver_out-ElementCount", SUM)
                .resetToValue(6L),
            Counter.longs("test_receiver_out-MeanByteCount", MEAN)
                .resetToValue(6, 33L)),
        counterSet);
  }

  // TODO: Test side inputs.
  // TODO: Test side outputs.
}
