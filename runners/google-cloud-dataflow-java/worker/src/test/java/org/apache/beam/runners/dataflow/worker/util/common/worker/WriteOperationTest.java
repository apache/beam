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

import static org.apache.beam.runners.dataflow.worker.counters.CounterName.named;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.io.IOException;
import org.apache.beam.runners.dataflow.worker.NameContextsForTests;
import org.apache.beam.runners.dataflow.worker.TestOperationContext;
import org.apache.beam.runners.dataflow.worker.counters.Counter.CounterUpdateExtractor;
import org.apache.beam.runners.dataflow.worker.counters.CounterSet;
import org.apache.beam.runners.dataflow.worker.counters.NameContext;
import org.apache.beam.runners.dataflow.worker.util.common.worker.Operation.InitializationState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InOrder;
import org.mockito.Mockito;

/** Tests for WriteOperation. */
@RunWith(JUnit4.class)
@SuppressWarnings({
  "rawtypes" // TODO(https://github.com/apache/beam/issues/20447)
})
public class WriteOperationTest {

  private final CounterSet counterSet = new CounterSet();
  private final OperationContext context =
      TestOperationContext.create(
          counterSet,
          NameContext.create("test", "WriteOperation", "WriteOperation", "WriteOperation"));

  @Test
  @SuppressWarnings("unchecked")
  public void testRunWriteOperation() throws Exception {
    ExecutorTestUtils.TestSink sink = new ExecutorTestUtils.TestSink();
    WriteOperation writeOperation = WriteOperation.forTest(sink, context);

    writeOperation.start();

    writeOperation.process("hi");
    writeOperation.process("there");
    writeOperation.process("");
    writeOperation.process("bob");

    writeOperation.finish();

    assertThat(sink.outputElems, hasItems("hi", "there", "", "bob"));

    CounterUpdateExtractor<?> updateExtractor = mock(CounterUpdateExtractor.class);
    counterSet.extractUpdates(false, updateExtractor);
    verify(updateExtractor).longSum(eq(named("WriteOperation-ByteCount")), anyBoolean(), eq(10L));
    verifyNoMoreInteractions(updateExtractor);
  }

  @Test
  public void testStartAbort() throws Exception {
    Sink<Object> mockSink = mock(Sink.class);
    Sink.SinkWriter<Object> mockWriter = mock(Sink.SinkWriter.class);
    when(mockSink.writer()).thenReturn(mockWriter);
    WriteOperation writeOperation = WriteOperation.forTest(mockSink, context);

    writeOperation.start();
    writeOperation.abort();
    assertThat(writeOperation.initializationState, equalTo(InitializationState.ABORTED));

    verify(mockWriter).abort();
  }

  @Test
  public void testStartFinishAbort() throws Exception {
    Sink<Object> mockSink = mock(Sink.class);
    Sink.SinkWriter<Object> mockWriter = mock(Sink.SinkWriter.class);
    when(mockSink.writer()).thenReturn(mockWriter);
    WriteOperation writeOperation = WriteOperation.forTest(mockSink, context);

    writeOperation.start();
    writeOperation.finish();
    assertThat(writeOperation.initializationState, equalTo(InitializationState.FINISHED));
    writeOperation.abort();
    assertThat(writeOperation.initializationState, equalTo(InitializationState.ABORTED));

    verify(mockWriter).close(); // finish called close
    verify(mockWriter, never()).abort(); // so abort is not called on the writer
  }

  @Test
  public void testStartFinishFailureAbort() throws Exception {
    Sink<Object> mockSink = mock(Sink.class);
    Sink.SinkWriter<Object> mockWriter = mock(Sink.SinkWriter.class);
    when(mockSink.writer()).thenReturn(mockWriter);
    WriteOperation writeOperation = WriteOperation.forTest(mockSink, context);

    Mockito.doThrow(new IOException("Expected failure to close")).when(mockWriter).close();

    writeOperation.start();
    try {
      writeOperation.finish();
      fail("Expected exception from finish");
    } catch (Exception e) {
      // expected exception
    }
    assertThat(writeOperation.initializationState, equalTo(InitializationState.FINISHED));
    writeOperation.abort();
    assertThat(writeOperation.initializationState, equalTo(InitializationState.ABORTED));

    verify(mockWriter).close(); // finish called close
    verify(mockWriter, never()).abort(); // so abort is not called on the writer
  }

  @Test
  public void testAbortWithoutStart() throws Exception {
    Sink<Object> mockSink = mock(Sink.class);
    WriteOperation writeOperation = WriteOperation.forTest(mockSink, context);

    writeOperation.abort();
    assertThat(writeOperation.initializationState, equalTo(InitializationState.ABORTED));
  }

  @Test
  public void testWriteOperationContext() throws Exception {
    OperationContext context = mock(OperationContext.class);

    Sink sink = mock(Sink.class);
    Sink.SinkWriter sinkWriter = mock(Sink.SinkWriter.class);
    when(sink.writer()).thenReturn(sinkWriter);
    when(sinkWriter.add("hello")).thenReturn(10L);
    when(context.counterFactory()).thenReturn(counterSet);
    when(context.nameContext()).thenReturn(NameContextsForTests.nameContextForTest());

    WriteOperation operation = WriteOperation.forTest(sink, context);

    Closeable startCloseable = mock(Closeable.class);
    Closeable processCloseable = mock(Closeable.class);
    Closeable finishCloseable = mock(Closeable.class);
    when(context.enterStart()).thenReturn(startCloseable);
    when(context.enterProcess()).thenReturn(processCloseable);
    when(context.enterFinish()).thenReturn(finishCloseable);

    operation.start();
    operation.process("hello");
    operation.finish();

    InOrder inOrder =
        Mockito.inOrder(
            sink, sinkWriter, context, startCloseable, processCloseable, finishCloseable);

    inOrder.verify(context).enterStart();
    inOrder.verify(sink).writer();
    inOrder.verify(startCloseable).close();
    inOrder.verify(context).enterProcess();
    inOrder.verify(sinkWriter).add("hello");
    inOrder.verify(processCloseable).close();
    inOrder.verify(context).enterFinish();
    inOrder.verify(sinkWriter).close();
    inOrder.verify(finishCloseable).close();
  }
}
