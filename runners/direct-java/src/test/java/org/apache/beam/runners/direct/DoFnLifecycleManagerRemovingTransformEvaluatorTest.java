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

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link DoFnLifecycleManagerRemovingTransformEvaluator}.
 */
@RunWith(JUnit4.class)
public class DoFnLifecycleManagerRemovingTransformEvaluatorTest {
  private DoFnLifecycleManager lifecycleManager;

  @Before
  public void setup() {
    lifecycleManager = DoFnLifecycleManager.of(new TestFn());
  }

  @Test
  public void delegatesToUnderlying() throws Exception {
    RecordingTransformEvaluator underlying = new RecordingTransformEvaluator();
    DoFn<?, ?> original = lifecycleManager.get();
    TransformEvaluator<Object> evaluator =
        DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(underlying, lifecycleManager);
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
  public void removesOnExceptionInProcessElement() throws Exception {
    ThrowingTransformEvaluator underlying = new ThrowingTransformEvaluator();
    DoFn<?, ?> original = lifecycleManager.get();
    assertThat(original, not(nullValue()));
    TransformEvaluator<Object> evaluator =
        DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(underlying, lifecycleManager);

    try {
      evaluator.processElement(WindowedValue.valueInGlobalWindow(new Object()));
    } catch (Exception e) {
      assertThat(lifecycleManager.get(), not(Matchers.<DoFn<?, ?>>theInstance(original)));
      return;
    }
    fail("Expected ThrowingTransformEvaluator to throw on method call");
  }

  @Test
  public void removesOnExceptionInFinishBundle() throws Exception {
    ThrowingTransformEvaluator underlying = new ThrowingTransformEvaluator();
    DoFn<?, ?> original = lifecycleManager.get();
    // the LifecycleManager is set when the evaluator starts
    assertThat(original, not(nullValue()));
    TransformEvaluator<Object> evaluator =
        DoFnLifecycleManagerRemovingTransformEvaluator.wrapping(underlying, lifecycleManager);

    try {
      evaluator.finishBundle();
    } catch (Exception e) {
      assertThat(lifecycleManager.get(),
          Matchers.not(Matchers.<DoFn<?, ?>>theInstance(original)));
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


  private static class TestFn extends DoFn<Object, Object> {
    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
    }
  }
}
