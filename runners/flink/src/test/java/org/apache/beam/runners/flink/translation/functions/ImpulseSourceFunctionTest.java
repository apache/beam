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
package org.apache.beam.runners.flink.translation.functions;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Tests for {@link ImpulseSourceFunction}. */
public class ImpulseSourceFunctionTest {

  private static final Logger LOG = LoggerFactory.getLogger(ImpulseSourceFunctionTest.class);

  @Rule public TestName testName = new TestName();

  private final SourceFunction.SourceContext<WindowedValue<byte[]>> sourceContext;
  private final ImpulseElementMatcher elementMatcher = new ImpulseElementMatcher();

  public ImpulseSourceFunctionTest() {
    this.sourceContext = Mockito.mock(SourceFunction.SourceContext.class);
    when(sourceContext.getCheckpointLock()).thenReturn(new Object());
  }

  @Test
  public void testInstanceOfSourceFunction() {
    // should be a non-parallel source function
    assertThat(new ImpulseSourceFunction(0), instanceOf(SourceFunction.class));
  }

  @Test(timeout = 10_000)
  public void testImpulseInitial() throws Exception {
    ImpulseSourceFunction source = new ImpulseSourceFunction(0);
    // No state available from previous runs
    ListState<Object> mockListState = getMockListState(Collections.emptyList());
    source.initializeState(getInitializationContext(mockListState));

    // 1) Should finish
    source.run(sourceContext);
    // 2) Should use checkpoint lock
    verify(sourceContext).getCheckpointLock();
    // 3) Should emit impulse element and the final watermark
    verify(sourceContext).collect(argThat(elementMatcher));
    verify(sourceContext).emitWatermark(Watermark.MAX_WATERMARK);
    verifyNoMoreInteractions(sourceContext);
    // 4) Should modify checkpoint state
    verify(mockListState).get();
    verify(mockListState).add(true);
    verifyNoMoreInteractions(mockListState);
  }

  @Test(timeout = 10_000)
  public void testImpulseRestored() throws Exception {
    ImpulseSourceFunction source = new ImpulseSourceFunction(0);
    // Previous state available
    ListState<Object> mockListState = getMockListState(Collections.singletonList(true));
    source.initializeState(getInitializationContext(mockListState));

    // 1) Should finish
    source.run(sourceContext);
    // 2) Should keep checkpoint state
    verify(mockListState).get();
    verifyNoMoreInteractions(mockListState);
    // 3) Should always emit the final watermark
    verify(sourceContext).emitWatermark(Watermark.MAX_WATERMARK);
    // 4) Should _not_ emit impulse element
    verifyNoMoreInteractions(sourceContext);
  }

  @Test(timeout = 10_000)
  public void testKeepAlive() throws Exception {
    ImpulseSourceFunction source = new ImpulseSourceFunction(Long.MAX_VALUE);

    // No previous state available (=impulse should be emitted)
    ListState<Object> mockListState = getMockListState(Collections.emptyList());
    source.initializeState(getInitializationContext(mockListState));

    Thread sourceThread =
        new Thread(
            () -> {
              try {
                source.run(sourceContext);
                // should not finish
              } catch (Exception e) {
                LOG.error("Exception while executing ImpulseSourceFunction", e);
              }
            });
    try {
      sourceThread.start();
      source.cancel();
      // should finish
      sourceThread.join();
    } finally {
      sourceThread.interrupt();
      sourceThread.join();
    }
    verify(sourceContext).collect(argThat(elementMatcher));
    verify(sourceContext).emitWatermark(Watermark.MAX_WATERMARK);
    verify(mockListState).add(true);
    verify(mockListState).get();
    verifyNoMoreInteractions(mockListState);
  }

  @Test(timeout = 10_000)
  public void testKeepAliveDuringInterrupt() throws Exception {
    ImpulseSourceFunction source = new ImpulseSourceFunction(Long.MAX_VALUE);

    // No previous state available (=impulse should not be emitted)
    ListState<Object> mockListState = getMockListState(Collections.singletonList(true));
    source.initializeState(getInitializationContext(mockListState));

    Thread sourceThread =
        new Thread(
            () -> {
              try {
                source.run(sourceContext);
                // should not finish
              } catch (Exception e) {
                LOG.error("Exception while executing ImpulseSourceFunction", e);
              }
            });

    sourceThread.start();
    sourceThread.interrupt();
    Thread.sleep(200);
    assertThat(sourceThread.isAlive(), is(true));

    // should quit
    source.cancel();
    sourceThread.interrupt();
    sourceThread.join();

    // Should always emit the final watermark
    verify(sourceContext).emitWatermark(Watermark.MAX_WATERMARK);
    // no element should have been emitted because the impulse was emitted before restore
    verifyNoMoreInteractions(sourceContext);
  }

  private static <T> FunctionInitializationContext getInitializationContext(ListState<T> listState)
      throws Exception {
    FunctionInitializationContext mock = Mockito.mock(FunctionInitializationContext.class);
    OperatorStateStore mockOperatorState = getMockOperatorState(listState);
    when(mock.getOperatorStateStore()).thenReturn(mockOperatorState);
    return mock;
  }

  private static <T> OperatorStateStore getMockOperatorState(ListState<T> listState)
      throws Exception {
    OperatorStateStore mock = Mockito.mock(OperatorStateStore.class);
    when(mock.getListState(Matchers.any(ListStateDescriptor.class))).thenReturn(listState);
    return mock;
  }

  private static <T> ListState<T> getMockListState(List<T> initialState) throws Exception {
    ListState mock = Mockito.mock(ListState.class);
    when(mock.get()).thenReturn(initialState);
    return mock;
  }

  private static class ImpulseElementMatcher implements ArgumentMatcher<WindowedValue<byte[]>> {

    @Override
    public boolean matches(WindowedValue<byte[]> o) {
      return o instanceof WindowedValue
          && Arrays.equals((byte[]) ((WindowedValue) o).getValue(), new byte[] {});
    }
  }
}
