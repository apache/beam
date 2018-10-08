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
import java.util.function.Supplier;
import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.runners.dataflow.worker.util.common.worker.OperationContext;
import org.apache.beam.runners.fnexecution.data.FnDataService;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
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
  private static final BeamFnApi.Target TARGET =
      BeamFnApi.Target.newBuilder().setPrimitiveTransformReference("1").setName("name").build();
  private static final String BUNDLE_ID = "999";
  private static final String BUNDLE_ID_2 = "222";

  @Mock Supplier<String> bundleIdSupplier;
  @Mock private FnDataService beamFnDataService;
  @Mock private OperationContext operationContext;
  private RemoteGrpcPortWriteOperation<String> operation;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    operation =
        new RemoteGrpcPortWriteOperation<>(
            beamFnDataService, TARGET, bundleIdSupplier, CODER, operationContext);
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
    when(bundleIdSupplier.get()).thenReturn(BUNDLE_ID);
    operation.start();
    verify(beamFnDataService).send(LogicalEndpoint.of(BUNDLE_ID, TARGET), CODER);
    assertFalse(recordingConsumer.closed);

    operation.process(valueInGlobalWindow("ABC"));
    operation.process(valueInGlobalWindow("DEF"));
    operation.process(valueInGlobalWindow("GHI"));
    assertFalse(recordingConsumer.closed);

    operation.finish();
    assertTrue(recordingConsumer.closed);
    verify(bundleIdSupplier, times(1)).get();

    assertThat(
        recordingConsumer,
        contains(
            valueInGlobalWindow("ABC"), valueInGlobalWindow("DEF"), valueInGlobalWindow("GHI")));

    // Ensure that the old bundle id is cleared.
    when(bundleIdSupplier.get()).thenReturn(BUNDLE_ID_2);
    when(beamFnDataService.send(any(), Matchers.<Coder<WindowedValue<String>>>any()))
        .thenReturn(recordingConsumer);
    operation.start();
    verify(beamFnDataService).send(LogicalEndpoint.of(BUNDLE_ID_2, TARGET), CODER);

    verifyNoMoreInteractions(beamFnDataService);
  }

  @Test
  public void testStartAndAbort() throws Exception {
    RecordingConsumer<WindowedValue<String>> recordingConsumer = new RecordingConsumer<>();
    when(beamFnDataService.send(any(), Matchers.<Coder<WindowedValue<String>>>any()))
        .thenReturn(recordingConsumer);
    when(bundleIdSupplier.get()).thenReturn(BUNDLE_ID);
    operation.start();
    verify(beamFnDataService).send(LogicalEndpoint.of(BUNDLE_ID, TARGET), CODER);
    assertFalse(recordingConsumer.closed);

    operation.process(valueInGlobalWindow("ABC"));
    operation.process(valueInGlobalWindow("DEF"));
    operation.process(valueInGlobalWindow("GHI"));

    operation.abort();
    assertTrue(recordingConsumer.closed);
    verify(bundleIdSupplier, times(1)).get();

    verifyNoMoreInteractions(beamFnDataService);
  }

  private static class RecordingConsumer<T> extends ArrayList<T>
      implements CloseableFnDataReceiver<T> {
    private boolean closed;

    @Override
    public void close() throws Exception {
      closed = true;
    }

    @Override
    public void accept(T t) throws Exception {
      if (closed) {
        throw new IllegalStateException("Consumer is closed but attempting to consume " + t);
      }
      add(t);
    }
  }
}
