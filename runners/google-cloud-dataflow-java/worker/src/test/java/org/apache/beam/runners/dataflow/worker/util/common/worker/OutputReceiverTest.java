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
package org.apache.beam.runners.dataflow.worker.util.common.worker;

import static org.apache.beam.runners.dataflow.worker.NameContextsForTests.nameContextForTest;
import static org.hamcrest.MatcherAssert.assertThat;

import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.CounterMean;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.util.common.worker.TestOutputReceiver.TestOutputCounter;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for OutputReceiver. */
@RunWith(JUnit4.class)
public class OutputReceiverTest {

  private final CounterSet counterSet = new CounterSet();

  @Test
  public void testEmptyOutputReceiver() throws Exception {
    OutputReceiver fanOut = new OutputReceiver();
    TestOutputCounter outputCounter = new TestOutputCounter(nameContextForTest());
    fanOut.addOutputCounter(outputCounter);
    fanOut.process("hi");
    fanOut.process("bob");

    CounterMean<Long> meanByteCount = outputCounter.getMeanByteCount().getAggregate();
    Assert.assertEquals(7, (long) meanByteCount.getAggregate());
    Assert.assertEquals(2, meanByteCount.getCount());
  }

  @Test
  public void testMultipleOutputReceiver() throws Exception {
    OutputReceiver fanOut = new OutputReceiver();
    TestOutputCounter outputCounter = new TestOutputCounter(nameContextForTest());
    fanOut.addOutputCounter(outputCounter);

    TestOutputReceiver receiver1 = new TestOutputReceiver(counterSet, nameContextForTest());
    fanOut.addOutput(receiver1);

    TestOutputReceiver receiver2 = new TestOutputReceiver(counterSet, nameContextForTest());
    fanOut.addOutput(receiver2);

    fanOut.process("hi");
    fanOut.process("bob");

    CounterMean<Long> meanByteCount = outputCounter.getMeanByteCount().getAggregate();
    Assert.assertEquals(7, meanByteCount.getAggregate().longValue());
    Assert.assertEquals(2, meanByteCount.getCount());
    assertThat(receiver1.outputElems, CoreMatchers.<Object>hasItems("hi", "bob"));
    assertThat(receiver2.outputElems, CoreMatchers.<Object>hasItems("hi", "bob"));
  }
}
