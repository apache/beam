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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.beam.sdk.util.WindowedValue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests for {@link ThreadLocalInvalidatingTransformEvaluator}.
 */
@RunWith(JUnit4.class)
public class ThreadLocalInvalidatingTransformEvaluatorTest {
  private ThreadLocal<Object> threadLocal;

  @Before
  public void setup() {
    threadLocal = new ThreadLocal<>();
    threadLocal.set(new Object());
  }

  @Test
  public void delegatesToUnderlying() throws Exception {
    RecordingTransformEvaluator underlying = new RecordingTransformEvaluator();
    Object original = threadLocal.get();
    TransformEvaluator<Object> evaluator =
        ThreadLocalInvalidatingTransformEvaluator.wrapping(underlying, threadLocal);
    WindowedValue<Object> first = WindowedValue.valueInGlobalWindow(new Object());
    WindowedValue<Object> second = WindowedValue.valueInGlobalWindow(new Object());
    evaluator.processElement(first);
    assertThat(underlying.objects, containsInAnyOrder(first));
    evaluator.processElement(second);
    evaluator.finishBundle();

    assertThat(underlying.finishBundleCalled, is(true));
    assertThat(underlying.objects, containsInAnyOrder(second, first));
  }

  @Test
  public void removesOnExceptionInProcessElement() {
    ThrowingTransformEvaluator underlying = new ThrowingTransformEvaluator();
    Object original = threadLocal.get();
    assertThat(original, not(nullValue()));
    TransformEvaluator<Object> evaluator =
        ThreadLocalInvalidatingTransformEvaluator.wrapping(underlying, threadLocal);

    try {
      evaluator.processElement(WindowedValue.valueInGlobalWindow(new Object()));
    } catch (Exception e) {
      assertThat(threadLocal.get(), nullValue());
      return;
    }
    fail("Expected ThrowingTransformEvaluator to throw on method call");
  }

  @Test
  public void removesOnExceptionInFinishBundle() {
    ThrowingTransformEvaluator underlying = new ThrowingTransformEvaluator();
    Object original = threadLocal.get();
    // the ThreadLocal is set when the evaluator starts
    assertThat(original, not(nullValue()));
    TransformEvaluator<Object> evaluator =
        ThreadLocalInvalidatingTransformEvaluator.wrapping(underlying, threadLocal);

    try {
      evaluator.finishBundle();
    } catch (Exception e) {
      assertThat(threadLocal.get(), nullValue());
      return;
    }
    fail("Expected ThrowingTransformEvaluator to throw on method call");
  }

  private class RecordingTransformEvaluator implements TransformEvaluator<Object> {
    private boolean finishBundleCalled;
    private List<WindowedValue<Object>> objects;

    public RecordingTransformEvaluator() {
      this.finishBundleCalled = true;
      this.objects = new ArrayList<>();
    }

    @Override
    public void processElement(WindowedValue<Object> element) throws Exception {
      objects.add(element);
    }

    @Override
    public TransformResult finishBundle() throws Exception {
      finishBundleCalled = true;
      return null;
    }
  }

  private class ThrowingTransformEvaluator implements TransformEvaluator<Object> {
    @Override
    public void processElement(WindowedValue<Object> element) throws Exception {
      throw new Exception();
    }

    @Override
    public TransformResult finishBundle() throws Exception {
      throw new Exception();
    }
  }
}
