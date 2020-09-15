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
package org.apache.beam.sdk.transforms.reflect;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.reflect.DoFnSignature.TimerDeclaration;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link DoFnInvokers}. */
@RunWith(JUnit4.class)
public class OnTimerInvokersTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private BoundedWindow mockWindow;

  @Mock private DoFnInvoker.ArgumentProvider<String, String> mockArgumentProvider;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(mockArgumentProvider.window()).thenReturn(mockWindow);
  }

  private void invokeOnTimer(DoFn<String, String> fn, String timerId) {
    OnTimerInvokers.forTimer(fn, TimerDeclaration.PREFIX + timerId)
        .invokeOnTimer(mockArgumentProvider);
  }

  @Test
  public void testOnTimerHelloWord() throws Exception {
    final String timerId = "my-timer-id";

    class SimpleTimerDoFn extends DoFn<String, String> {

      public String status = "not yet";

      @TimerId(timerId)
      private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

      @ProcessElement
      public void process(ProcessContext c) {}

      @OnTimer(timerId)
      public void onMyTimer() {
        status = "OK now";
      }
    }

    SimpleTimerDoFn fn = new SimpleTimerDoFn();

    invokeOnTimer(fn, timerId);
    assertThat(fn.status, equalTo("OK now"));
  }

  @Test
  public void testOnTimerWithWindow() throws Exception {
    WindowedTimerDoFn fn = new WindowedTimerDoFn();

    invokeOnTimer(fn, WindowedTimerDoFn.TIMER_ID);
    assertThat(fn.window, theInstance(mockWindow));
  }

  private static class WindowedTimerDoFn extends DoFn<String, String> {
    public static final String TIMER_ID = "my-timer-id";

    public BoundedWindow window = null;

    @TimerId(TIMER_ID)
    private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void process(ProcessContext c) {}

    @OnTimer(TIMER_ID)
    public void onMyTimer(BoundedWindow window) {
      this.window = window;
    }
  }

  static class StableNameTestDoFn extends DoFn<Void, Void> {
    private static final String TIMER_ID = "timer-id.with specialChars{}";

    @TimerId(TIMER_ID)
    private final TimerSpec myTimer = TimerSpecs.timer(TimeDomain.PROCESSING_TIME);

    @ProcessElement
    public void process() {}

    @OnTimer(TIMER_ID)
    public void onMyTimer() {}
  }

  /** This is a change-detector test that the generated name is stable across runs. */
  @Test
  public void testStableName() {
    OnTimerInvoker<Void, Void> invoker =
        OnTimerInvokers.forTimer(
            new StableNameTestDoFn(), TimerDeclaration.PREFIX + StableNameTestDoFn.TIMER_ID);

    assertThat(
        invoker.getClass().getName(),
        equalTo(
            String.format(
                "%s$%s$%s$%s",
                StableNameTestDoFn.class.getName(),
                OnTimerInvoker.class.getSimpleName(),
                "tstimeridwithspecialChars" /* alphanum only; human readable but not unique */,
                "dHMtdGltZXItaWQud2l0aCBzcGVjaWFsQ2hhcnN7fQ" /* base64 encoding of UTF-8 timerId */)));
  }
}
