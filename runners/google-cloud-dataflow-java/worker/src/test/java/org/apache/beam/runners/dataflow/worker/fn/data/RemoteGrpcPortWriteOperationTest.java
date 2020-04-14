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
package org.apache.beam.runners.dataflow.worker.fn.data;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.fn.IdGenerator;
import org.apache.beam.sdk.fn.data.CloseableFnDataReceiver;
import org.apache.beam.sdk.fn.data.LogicalEndpoint;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link RemoteGrpcPortWriteOperation}. */
@RunWith(JUnit4.class)
public class RemoteGrpcPortWriteOperationTest {
  private static final Coder<WindowedValue<String>> CODER =
      WindowedValue.getFullCoder(StringUtf8Coder.of(), GlobalWindow.Coder.INSTANCE);
  private static final String TRANSFORM_ID = "1";
  private static final String BUNDLE_ID = "999";
  private static final String BUNDLE_ID_2 = "222";

  @Mock IdGenerator bundleIdSupplier;
  @Mock private FnDataService beamFnDataService;
  @Mock private OperationContext operationContext;
  private RemoteGrpcPortWriteOperation<String> operation;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    operation =
        new RemoteGrpcPortWriteOperation<>(
            beamFnDataService, TRANSFORM_ID, bundleIdSupplier, CODER, operationContext);
  }

  @Test
  public void testSupportsRestart() {
    assertTrue(operation.supportsRestart());
  }

  @Test
  public void testSuccessfulProcessing() throws Exception {
    RecordingConsumer<WindowedValue<String>> recordingConsumer = new RecordingConsumer<>();
    when(beamFnDataService.send(any(), Matchers.<Coder<WindowedValue<String>>>any()))
        .thenReturn(recordingConsumer);
    when(bundleIdSupplier.getId()).thenReturn(BUNDLE_ID);
    operation.start();
    verify(beamFnDataService).send(LogicalEndpoint.data(BUNDLE_ID, TRANSFORM_ID), CODER);
    assertFalse(recordingConsumer.closed);

    operation.process(valueInGlobalWindow("ABC"));
    operation.process(valueInGlobalWindow("DEF"));
    operation.process(valueInGlobalWindow("GHI"));
    assertFalse(recordingConsumer.closed);

    operation.finish();
    assertTrue(recordingConsumer.closed);
    verify(bundleIdSupplier, times(1)).getId();

    assertThat(
        recordingConsumer,
        contains(
            valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF"), valueInGlobalWindow("GHI")));

    // Ensure that the old bundle id is cleared.
    when(bundleIdSupplier.getId()).thenReturn(BUNDLE_ID_2);
    when(beamFnDataService.send(any(), Matchers.<Coder<WindowedValue<String>>>any()))
        .thenReturn(recordingConsumer);
    operation.start();
    verify(beamFnDataService).send(LogicalEndpoint.data(BUNDLE_ID_2, TRANSFORM_ID), CODER);

    verifyNoMoreInteractions(beamFnDataService);
  }

  @Test
  public void testStartAndAbort() throws Exception {
    RecordingConsumer<WindowedValue<String>> recordingConsumer = new RecordingConsumer<>();
    when(beamFnDataService.send(any(), Matchers.<Coder<WindowedValue<String>>>any()))
        .thenReturn(recordingConsumer);
    when(bundleIdSupplier.getId()).thenReturn(BUNDLE_ID);
    operation.start();
    verify(beamFnDataService).send(LogicalEndpoint.data(BUNDLE_ID, TRANSFORM_ID), CODER);
    assertFalse(recordingConsumer.closed);

    operation.process(valueInGlobalWindow("ABC"));
    operation.process(valueInGlobalWindow("DEF"));
    operation.process(valueInGlobalWindow("GHI"));

    operation.abort();
    assertTrue(recordingConsumer.closed);
    verify(bundleIdSupplier, times(1)).getId();

    verifyNoMoreInteractions(beamFnDataService);
  }

  @Test
  public void testBufferRateLimiting() throws Exception {
    AtomicInteger processedElements = new AtomicInteger();
    AtomicInteger currentTimeMillis = new AtomicInteger(10000);

    final int START_BUFFER_SIZE = 3;
    final int STEADY_BUFFER_SIZE = 10;
    final int FIRST_ELEMENT_DURATION =
        (int) RemoteGrpcPortWriteOperation.MAX_BUFFER_MILLIS / START_BUFFER_SIZE + 1;
    final int STEADY_ELEMENT_DURATION =
        (int) RemoteGrpcPortWriteOperation.MAX_BUFFER_MILLIS / STEADY_BUFFER_SIZE + 1;

    operation =
        new RemoteGrpcPortWriteOperation<>(
            beamFnDataService,
            TRANSFORM_ID,
            bundleIdSupplier,
            CODER,
            operationContext,
            () -> (long) currentTimeMillis.get());

    RecordingConsumer<WindowedValue<String>> recordingConsumer = new RecordingConsumer<>();
    when(beamFnDataService.send(any(), Matchers.<Coder<WindowedValue<String>>>any()))
        .thenReturn(recordingConsumer);
    when(bundleIdSupplier.getId()).thenReturn(BUNDLE_ID);

    Consumer<Integer> processedElementConsumer = operation.processedElementsConsumer();

    operation.start();

    // Never wait before sending the first element.
    assertFalse(operation.shouldWait());
    operation.process(valueInGlobalWindow("first"));

    // After sending the first element, wait until it's processed before sending another.
    assertTrue(operation.shouldWait());

    // Once we've processed the element, we can send the second.
    currentTimeMillis.getAndAdd(FIRST_ELEMENT_DURATION);
    processedElementConsumer.accept(1);
    assertFalse(operation.shouldWait());
    operation.process(valueInGlobalWindow("second"));

    // Send elements until the buffer is full.
    for (int i = 2; i < START_BUFFER_SIZE + 1; i++) {
      assertFalse(operation.shouldWait());
      operation.process(valueInGlobalWindow("element" + i));
    }

    // The buffer is full.
    assertTrue(operation.shouldWait());

    // Now finish processing the second element.
    currentTimeMillis.getAndAdd(STEADY_ELEMENT_DURATION);
    processedElementConsumer.accept(2);

    // That was faster, so our buffer quota is larger.
    for (int i = START_BUFFER_SIZE + 1; i < STEADY_BUFFER_SIZE + 2; i++) {
      assertFalse(operation.shouldWait());
      operation.process(valueInGlobalWindow("element" + i));
    }

    // The buffer is full again.
    assertTrue(operation.shouldWait());

    // As elements are consumed, we can keep adding more.
    for (int i = START_BUFFER_SIZE + STEADY_BUFFER_SIZE + 2; i < 100; i++) {
      currentTimeMillis.getAndAdd(STEADY_ELEMENT_DURATION);
      processedElementConsumer.accept(i);
      assertFalse(operation.shouldWait());
      operation.process(valueInGlobalWindow("element" + i));
    }

    operation.finish();
  }

  private static class RecordingConsumer<T> extends ArrayList<T>
      implements CloseableFnDataReceiver<T> {
    private boolean closed;

    @Override
    public void close() throws Exception {
      closed = true;
    }

    @Override
    public void flush() throws Exception {}

    @Override
    public synchronized void accept(T t) throws Exception {
      if (closed) {
        throw new IllegalStateException("Consumer is closed but attempting to consume " + t);
      }
      add(t);
    }
  }
}
