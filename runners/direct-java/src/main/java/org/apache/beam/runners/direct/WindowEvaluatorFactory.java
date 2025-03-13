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

import java.util.Collection;
import org.apache.beam.sdk.runners.AppliedPTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.WindowIntoTranslation;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the {@link Window.Assign}
 * primitive {@link PTransform}.
 */
@SuppressWarnings({"keyfor", "nullness"}) // TODO(https://github.com/apache/beam/issues/20497)
class WindowEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext evaluationContext;

  WindowEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      AppliedPTransform<?, ?, ?> application, @Nullable CommittedBundle<?> inputBundle)
      throws Exception {
    return createTransformEvaluator((AppliedPTransform) application);
  }

  private <InputT> TransformEvaluator<InputT> createTransformEvaluator(
      AppliedPTransform<PCollection<InputT>, PCollection<InputT>, Window.Assign<InputT>>
          transform) {

    WindowFn<? super InputT, ?> fn = (WindowFn) WindowIntoTranslation.getWindowFn(transform);

    UncommittedBundle<InputT> outputBundle =
        evaluationContext.createBundle(
            (PCollection<InputT>) Iterables.getOnlyElement(transform.getOutputs().values()));
    if (fn == null) {
      return PassthroughTransformEvaluator.create(transform, outputBundle);
    }
    return new WindowIntoEvaluator<>(transform, fn, outputBundle);
  }

  @Override
  public void cleanup() {}

  private static class WindowIntoEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final AppliedPTransform<PCollection<InputT>, PCollection<InputT>, Window.Assign<InputT>>
        transform;
    private final WindowFn<InputT, ?> windowFn;
    private final UncommittedBundle<InputT> outputBundle;

    @SuppressWarnings("unchecked")
    public WindowIntoEvaluator(
        AppliedPTransform<PCollection<InputT>, PCollection<InputT>, Window.Assign<InputT>>
            transform,
        WindowFn<? super InputT, ?> windowFn,
        UncommittedBundle<InputT> outputBundle) {
      this.outputBundle = outputBundle;
      this.transform = transform;
      // Safe contravariant cast
      this.windowFn = (WindowFn<InputT, ?>) windowFn;
    }

    @Override
    public void processElement(WindowedValue<InputT> compressedElement) throws Exception {
      for (WindowedValue<InputT> element : compressedElement.explodeWindows()) {
        Collection<? extends BoundedWindow> windows = assignWindows(windowFn, element);
        outputBundle.add(
            WindowedValue.of(
                element.getValue(), element.getTimestamp(), windows, element.getPane()));
      }
    }

    private <W extends BoundedWindow> Collection<? extends BoundedWindow> assignWindows(
        WindowFn<InputT, W> windowFn, WindowedValue<InputT> element) throws Exception {
      WindowFn<InputT, W>.AssignContext assignContext =
          new DirectAssignContext<>(windowFn, element);
      return windowFn.assignWindows(assignContext);
    }

    @Override
    public TransformResult<InputT> finishBundle() throws Exception {
      return StepTransformResult.<InputT>withoutHold(transform).addOutput(outputBundle).build();
    }
  }

  private static class DirectAssignContext<InputT, W extends BoundedWindow>
      extends WindowFn<InputT, W>.AssignContext {
    private final WindowedValue<InputT> value;

    public DirectAssignContext(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
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
    public BoundedWindow window() {
      return Iterables.getOnlyElement(value.getWindows());
    }
  }
}
