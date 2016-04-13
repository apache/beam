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
package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.CommittedBundle;
import com.google.cloud.dataflow.sdk.runners.inprocess.InProcessPipelineRunner.UncommittedBundle;
import com.google.cloud.dataflow.sdk.transforms.AppliedPTransform;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PaneInfo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window.Bound;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.WindowedValue;
import com.google.cloud.dataflow.sdk.values.PCollection;

import org.joda.time.Instant;

import java.util.Collection;

import javax.annotation.Nullable;

/**
 * The {@link InProcessPipelineRunner} {@link TransformEvaluatorFactory} for the
 * {@link Bound Window.Bound} primitive {@link PTransform}.
 */
class WindowEvaluatorFactory implements TransformEvaluatorFactory {

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application,
      @Nullable CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext)
      throws Exception {
    return createTransformEvaluator(
        (AppliedPTransform) application, inputBundle, evaluationContext);
  }

  private <InputT> TransformEvaluator<InputT> createTransformEvaluator(
      AppliedPTransform<PCollection<InputT>, PCollection<InputT>, Window.Bound<InputT>> transform,
      CommittedBundle<?> inputBundle,
      InProcessEvaluationContext evaluationContext) {
    WindowFn<? super InputT, ?> fn = transform.getTransform().getWindowFn();
    UncommittedBundle<InputT> outputBundle =
        evaluationContext.createBundle(inputBundle, transform.getOutput());
    if (fn == null) {
      return PassthroughTransformEvaluator.create(transform, outputBundle);
    }
    return new WindowIntoEvaluator<>(transform, fn, outputBundle);
  }

  private static class WindowIntoEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final AppliedPTransform<PCollection<InputT>, PCollection<InputT>, Window.Bound<InputT>>
        transform;
    private final WindowFn<InputT, ?> windowFn;
    private final UncommittedBundle<InputT> outputBundle;

    @SuppressWarnings("unchecked")
    public WindowIntoEvaluator(
        AppliedPTransform<PCollection<InputT>, PCollection<InputT>, Window.Bound<InputT>> transform,
        WindowFn<? super InputT, ?> windowFn,
        UncommittedBundle<InputT> outputBundle) {
      this.outputBundle = outputBundle;
      this.transform = transform;
      // Safe contravariant cast
      this.windowFn = (WindowFn<InputT, ?>) windowFn;
    }

    @Override
    public void processElement(WindowedValue<InputT> element) throws Exception {
      Collection<? extends BoundedWindow> windows = assignWindows(windowFn, element);
      outputBundle.add(
          WindowedValue.<InputT>of(
              element.getValue(), element.getTimestamp(), windows, PaneInfo.NO_FIRING));
    }

    private <W extends BoundedWindow> Collection<? extends BoundedWindow> assignWindows(
        WindowFn<InputT, W> windowFn, WindowedValue<InputT> element) throws Exception {
      WindowFn<InputT, W>.AssignContext assignContext =
          new InProcessAssignContext<>(windowFn, element);
      Collection<? extends BoundedWindow> windows = windowFn.assignWindows(assignContext);
      return windows;
    }

    @Override
    public InProcessTransformResult finishBundle() throws Exception {
      return StepTransformResult.withoutHold(transform).addOutput(outputBundle).build();
    }
  }

  private static class InProcessAssignContext<InputT, W extends BoundedWindow>
      extends WindowFn<InputT, W>.AssignContext {
    private final WindowedValue<InputT> value;

    public InProcessAssignContext(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
      fn.super();
      this.value = value;
    }

    @Override
    public InputT element() {
      return value.getValue();
    }

    @Override
    public Instant timestamp() {
      return value.getTimestamp();
    }

    @Override
    public Collection<? extends BoundedWindow> windows() {
      return value.getWindows();
    }

  }
}
