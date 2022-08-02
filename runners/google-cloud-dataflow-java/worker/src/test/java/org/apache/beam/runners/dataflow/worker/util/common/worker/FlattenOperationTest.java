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

/** Tests for FlattenOperation. */
@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class FlattenOperationTest {

  private static final NameContext nameContext =
      NameContext.create("test", "FlattenOperation", "FlattenOperation", "FlattenOperation");
  private final CounterSet counterSet = new CounterSet();

  @Test
  public void testRunFlattenOperation() throws Exception {
    TestOutputReceiver receiver =
        new TestOutputReceiver(
            counterSet, NameContext.create("test", "receiver", "receiver", "receiver"));

    OperationContext context = TestOperationContext.create(counterSet, nameContext);
    FlattenOperation flattenOperation = new FlattenOperation(receiver, context);

    flattenOperation.start();

    flattenOperation.process("hi");
    flattenOperation.process("there");
    flattenOperation.process("");
    flattenOperation.process("bob");

    flattenOperation.finish();

    assertThat(receiver.outputElems, CoreMatchers.<Object>hasItems("hi", "there", "", "bob"));

    CounterUpdateExtractor<?> updateExtractor = Mockito.mock(CounterUpdateExtractor.class);
    counterSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor).longSum(getObjectCounterName("test_receiver_out"), false, 4L);
    verify(updateExtractor)
        .longMean(
            getMeanByteCounterName("test_receiver_out"),
            false,
            LongCounterMean.ZERO.addValue(14L, 4));
    verifyNoMoreInteractions(updateExtractor);
  }

  @Test
  public void testFlattenOperationContext() throws Exception {
    OperationContext context = Mockito.mock(OperationContext.class);

    OutputReceiver receiver = Mockito.mock(OutputReceiver.class);
    FlattenOperation operation = new FlattenOperation(receiver, context);

    Closeable startCloseable = Mockito.mock(Closeable.class);
    Closeable processCloseable = Mockito.mock(Closeable.class);
    Closeable finishCloseable = Mockito.mock(Closeable.class);
    when(context.enterStart()).thenReturn(startCloseable);
    when(context.enterProcess()).thenReturn(processCloseable);
    when(context.enterFinish()).thenReturn(finishCloseable);

    operation.start();
    operation.process("hello");
    operation.finish();

    InOrder inOrder =
        Mockito.inOrder(receiver, context, startCloseable, processCloseable, finishCloseable);

    inOrder.verify(context).enterProcess();
    inOrder.verify(receiver).process("hello");
    inOrder.verify(processCloseable).close();
  }
}
