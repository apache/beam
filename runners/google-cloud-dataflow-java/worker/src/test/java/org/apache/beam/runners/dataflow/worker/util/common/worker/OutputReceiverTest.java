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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
  public void testSingleOutputReceiverAndZeroCounters() throws Exception {
    OutputReceiver fanOut = new OutputReceiver();
    TestOutputReceiver receiver = new TestOutputReceiver(counterSet, nameContextForTest());
    fanOut.addOutput(receiver);
    assertEquals(1, fanOut.getReceiverCount());
    assertEquals(receiver, fanOut.getOnlyReceiver());

    // 0 counters, 1 receiver
    fanOut.process("hi");
    assertThat(receiver.outputElems, CoreMatchers.<Object>hasItems("hi"));

    // 1 counter, 1 receiver (hot path)
    TestOutputCounter outputCounter = new TestOutputCounter(nameContextForTest());
    fanOut.addOutputCounter(outputCounter);
    fanOut.process("bob");
    CounterMean<Long> meanByteCount = outputCounter.getMeanByteCount().getAggregate();
    assertEquals(4L, meanByteCount.getAggregate().longValue());
    assertEquals(1L, meanByteCount.getCount());
    assertThat(receiver.outputElems, CoreMatchers.<Object>hasItems("hi", "bob"));
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

  @Test
  public void testNullOutputsSkippedAndReceiverCountPreserved() throws Exception {
    OutputReceiver onlyNull = new OutputReceiver();
    onlyNull.addOutput(null);
    assertEquals(1, onlyNull.getReceiverCount());
    assertNull(onlyNull.getOnlyReceiver());
    onlyNull.process("ignored");

    OutputReceiver mixed = new OutputReceiver();
    List<Object> seen1 = new ArrayList<>();
    List<Object> seen2 = new ArrayList<>();
    List<Object> seen3 = new ArrayList<>();
    mixed.addOutput(null);
    mixed.addOutput(seen1::add);
    mixed.addOutput(null);
    mixed.addOutput(seen2::add);
    mixed.addOutput(seen3::add);
    assertEquals(5, mixed.getReceiverCount());
    mixed.process("x");
    assertEquals(Collections.singletonList("x"), seen1);
    assertEquals(Collections.singletonList("x"), seen2);
    assertEquals(Collections.singletonList("x"), seen3);
  }

  @Test
  public void testMultipleCountersOverwriteAndCallbackOrder() throws Exception {
    List<String> events = new ArrayList<>();
    ElementCounter c1 =
        new ElementCounter() {
          @Override
          public void update(Object elem) {
            events.add("c1.update:" + elem);
          }

          @Override
          public void finishLazyUpdate(Object elem) {
            events.add("c1.finish:" + elem);
          }
        };
    ElementCounter c2 =
        new ElementCounter() {
          @Override
          public void update(Object elem) {
            events.add("c2.update:" + elem);
          }

          @Override
          public void finishLazyUpdate(Object elem) {
            events.add("c2.finish:" + elem);
          }
        };
    ElementCounter c1Replacement =
        new ElementCounter() {
          @Override
          public void update(Object elem) {
            events.add("c1r.update:" + elem);
          }

          @Override
          public void finishLazyUpdate(Object elem) {
            events.add("c1r.finish:" + elem);
          }
        };

    OutputReceiver fanOut = new OutputReceiver();
    fanOut.addOutputCounter("first", c1);
    // Overwriting single counter replaces singleCounter in place.
    fanOut.addOutputCounter("first", c1Replacement);
    fanOut.addOutput(elem -> events.add("r1:" + elem));
    fanOut.process("a");
    assertEquals(Arrays.asList("c1r.update:a", "r1:a", "c1r.finish:a"), events);

    // Adding a second counter moves to multi-counter path preserving insertion order.
    events.clear();
    fanOut.addOutputCounter("second", c2);
    fanOut.process("b");
    assertEquals(
        Arrays.asList("c1r.update:b", "c2.update:b", "r1:b", "c1r.finish:b", "c2.finish:b"),
        events);

    // Overwriting a named counter when size >= 2 updates multipleCounters in insertion order.
    events.clear();
    fanOut.addOutputCounter("first", c1);
    fanOut.process("b2");
    assertEquals(
        Arrays.asList("c1.update:b2", "c2.update:b2", "r1:b2", "c1.finish:b2", "c2.finish:b2"),
        events);
    fanOut.addOutputCounter("first", c1Replacement);

    // Defensive copy: mutating the map returned by getOutputCounters() does not affect dispatch.
    HashMap<String, ElementCounter> copy = fanOut.getOutputCounters();
    copy.remove("first");
    copy.put("bogus", c1);
    events.clear();
    fanOut.process("c");
    assertEquals(
        Arrays.asList("c1r.update:c", "c2.update:c", "r1:c", "c1r.finish:c", "c2.finish:c"),
        events);
    assertEquals(2, fanOut.getOutputCounters().size());

    // Downstream exception prevents finishLazyUpdate.
    OutputReceiver throwing = new OutputReceiver();
    events.clear();
    throwing.addOutputCounter("first", c1);
    throwing.addOutput(
        elem -> {
          events.add("throw");
          throw new IllegalStateException("boom");
        });
    assertThrows(IllegalStateException.class, () -> throwing.process("z"));
    assertEquals(Arrays.asList("c1.update:z", "throw"), events);

    assertThrows(NullPointerException.class, () -> fanOut.addOutputCounter("null", null));
  }
}
