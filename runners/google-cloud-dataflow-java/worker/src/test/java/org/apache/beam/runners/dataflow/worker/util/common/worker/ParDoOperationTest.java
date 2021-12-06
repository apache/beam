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

import static org.apache.beam.runners.dataflow.worker.util.common.worker.TestOutputReceiver.TestOutputCounter.getMeanByteCounterName;
import static org.apache.beam.runners.dataflow.worker.util.common.worker.TestOutputReceiver.TestOutputCounter.getObjectCounterName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import org.apache.beam.runners.dataflow.worker.TestOperationContext;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.CounterFactory.LongCounterMean;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mockito;

/** Tests for ParDoOperation. */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class ParDoOperationTest {

  private CounterSet counterSet = new CounterSet();
  private OperationContext context =
      TestOperationContext.create(
          counterSet,
          NameContext.create("test", "ParDoOperation", "ParDoOperation", "ParDoOperation"));

  private static class TestParDoFn implements ParDoFn {
    final OutputReceiver outputReceiver;

    public TestParDoFn(OutputReceiver outputReceiver) {
      this.outputReceiver = outputReceiver;
    }

    @Override
    public void startBundle(final Receiver... receivers) throws Exception {
      if (receivers.length != 1) {
        throw new AssertionError("unexpected number of receivers for DoFn");
      }

      outputReceiver.process("x-start");
    }

    @Override
    public void processElement(Object elem) throws Exception {
      outputReceiver.process("y-" + elem);
    }

    @Override
    public void processTimers() throws Exception {
      outputReceiver.process("t-timers");
    }

    @Override
    public void finishBundle() throws Exception {
      outputReceiver.process("z-finish");
    }

    @Override
    public void abort() throws Exception {
      outputReceiver.process("a-aborted");
    }
  }

  @Test
  public void testRunParDoOperation() throws Exception {
    TestOutputReceiver receiver =
        new TestOutputReceiver(
            counterSet,
            NameContext.create("test", "test_receiver", "test_receiver", "test_receiver"));

    ParDoOperation parDoOperation =
        new ParDoOperation(new TestParDoFn(receiver), new OutputReceiver[] {receiver}, context);

    parDoOperation.start();

    parDoOperation.process("hi");
    parDoOperation.process("there");
    parDoOperation.process("");
    parDoOperation.process("bob");

    parDoOperation.finish();

    parDoOperation.abort();

    assertThat(
        receiver.outputElems,
        CoreMatchers.<Object>hasItems(
            "x-start", "y-hi", "y-there", "y-", "y-bob", "z-finish", "a-aborted"));

    CounterUpdateExtractor<?> updateExtractor = Mockito.mock(CounterUpdateExtractor.class);
    counterSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor)
        .longSum(eq(getObjectCounterName("test_receiver_out")), anyBoolean(), eq(8L));
    verify(updateExtractor)
        .longMean(
            eq(getMeanByteCounterName("test_receiver_out")),
            anyBoolean(),
            eq(LongCounterMean.ZERO.addValue(58L, 8)));
    verifyNoMoreInteractions(updateExtractor);
  }

  // TODO: Test side inputs.
  // TODO: Test side outputs.

  @Test
  public void testParDoOperationContext() throws Exception {
    ParDoFn fn = Mockito.mock(ParDoFn.class);
    OperationContext context = Mockito.mock(OperationContext.class);

    OutputReceiver[] receivers = {};
    ParDoOperation operation = new ParDoOperation(fn, receivers, context);

    Closeable startCloseable = Mockito.mock(Closeable.class);
    Closeable processCloseable = Mockito.mock(Closeable.class);
    Closeable processTimersCloseable = Mockito.mock(Closeable.class);
    Closeable finishCloseable = Mockito.mock(Closeable.class);
    when(context.enterStart()).thenReturn(startCloseable);
    when(context.enterProcess()).thenReturn(processCloseable);
    when(context.enterProcessTimers()).thenReturn(processTimersCloseable);
    when(context.enterFinish()).thenReturn(finishCloseable);

    operation.start();
    operation.process("hello");
    operation.finish();

    InOrder inOrder =
        Mockito.inOrder(
            fn, context, startCloseable, processCloseable, processTimersCloseable, finishCloseable);

    inOrder.verify(context).enterStart();
    inOrder.verify(fn).startBundle(receivers);
    inOrder.verify(startCloseable).close();
    inOrder.verify(context).enterProcess();
    inOrder.verify(fn).processElement("hello");
    inOrder.verify(processCloseable).close();
    inOrder.verify(context).enterProcessTimers();
    inOrder.verify(fn).processTimers();
    inOrder.verify(processTimersCloseable).close();
    inOrder.verify(context).enterFinish();
    inOrder.verify(fn).finishBundle();
    inOrder.verify(finishCloseable).close();
  }
}
