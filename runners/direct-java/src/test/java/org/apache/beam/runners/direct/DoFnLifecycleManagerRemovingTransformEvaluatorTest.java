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
package org.apache.beam.runners.direct;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.hamcrest.Matchers;
import org.joda.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link DoFnLifecycleManagerRemovingTransformEvaluator}. */
@RunWith(JUnit4.class)
public class DoFnLifecycleManagerRemovingTransformEvaluatorTest {
  private DoFnLifecycleManager lifecycleManager;

  @Before
  public void setup() {
    lifecycleManager = DoFnLifecycleManager.of(new TestFn());
  }

  @Test
  public void delegatesToUnderlying() throws Exception {
    ParDoEvaluator<Object> underlying = mock(ParDoEvaluator.class);
    DoFn<?, ?> original = lifecycleManager.get();
    TransformEvaluator<Object> evaluator =
        DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(underlying, lifecycleManager);
    WindowedValue<Object> first = WindowedValue.valueInGlobalWindow(new Object());
    WindowedValue<Object> second = WindowedValue.valueInGlobalWindow(new Object());

    evaluator.processElement(first);
    verify(underlying).processElement(first);

    evaluator.processElement(second);
    verify(underlying).processElement(second);

    evaluator.finishBundle();
    verify(underlying).finishBundle();
  }

  @Test
  public void removesOnExceptionInProcessElement() throws Exception {
    ParDoEvaluator<Object> underlying = mock(ParDoEvaluator.class);
    doThrow(IllegalArgumentException.class)
        .when(underlying)
        .processElement(any(WindowedValue.class));

    DoFn<?, ?> original = lifecycleManager.get();
    assertThat(original, not(nullValue()));
    TransformEvaluator<Object> evaluator =
        DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(underlying, lifecycleManager);

    try {
      evaluator.processElement(WindowedValue.valueInGlobalWindow(new Object()));
    } catch (Exception e) {
      assertThat(lifecycleManager.get(), not(Matchers.theInstance(original)));
      return;
    }
    fail("Expected underlying evaluator to throw on method call");
  }

  @Test
  public void removesOnExceptionInOnTimer() throws Exception {
    ParDoEvaluator<Object> underlying = mock(ParDoEvaluator.class);
    doThrow(IllegalArgumentException.class)
        .when(underlying)
        .onTimer(any(TimerData.class), any(Object.class), any(BoundedWindow.class));

    DoFn<?, ?> original = lifecycleManager.get();
    assertThat(original, not(nullValue()));
    DoFnLifecycleManagerRemovingTransformEvaluator<Object> evaluator =
        DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(underlying, lifecycleManager);

    try {
      evaluator.onTimer(
          TimerData.of(
              "foo",
              StateNamespaces.global(),
              new Instant(0),
              new Instant(0),
              TimeDomain.EVENT_TIME),
          "",
          GlobalWindow.INSTANCE);
    } catch (Exception e) {
      assertThat(lifecycleManager.get(), not(Matchers.theInstance(original)));
      return;
    }
    fail("Expected underlying evaluator to throw on method call");
  }

  @Test
  public void removesOnExceptionInFinishBundle() throws Exception {
    ParDoEvaluator<Object> underlying = mock(ParDoEvaluator.class);
    doThrow(IllegalArgumentException.class).when(underlying).finishBundle();

    DoFn<?, ?> original = lifecycleManager.get();
    // the LifecycleManager is set when the evaluator starts
    assertThat(original, not(nullValue()));
    TransformEvaluator<Object> evaluator =
        DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(underlying, lifecycleManager);

    try {
      evaluator.finishBundle();
    } catch (Exception e) {
      assertThat(lifecycleManager.get(), not(Matchers.theInstance(original)));
      return;
    }
    fail("Expected underlying evaluator to throw on method call");
  }

  private static class TestFn extends DoFn<Object, Object> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {}
  }
}
