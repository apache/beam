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
package org.apache.beam.runners.direct.portable;

import com.google.common.collect.Iterables;
import java.util.Collection;
import javax.annotation.Nullable;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PCollectionNode;
import org.apache.beam.runners.core.construction.graph.PipelineNode.PTransformNode;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.joda.time.Instant;

/**
 * The {@link DirectRunner} {@link TransformEvaluatorFactory} for the
 * {@link Window.Assign} primitive {@link PTransform}.
 */
class WindowEvaluatorFactory implements TransformEvaluatorFactory {
  private final EvaluationContext evaluationContext;

  WindowEvaluatorFactory(EvaluationContext evaluationContext) {
    this.evaluationContext = evaluationContext;
  }

  @Override
  public <InputT> TransformEvaluator<InputT> forApplication(
      PTransformNode application, @Nullable CommittedBundle<?> inputBundle)  {
    return createTransformEvaluator(application);
  }

  private <InputT> TransformEvaluator<InputT> createTransformEvaluator(PTransformNode transform) {
    WindowFn<? super InputT, ?> fn = null;

    PCollectionNode outputPCollection = null;
    evaluationContext.createBundle(outputPCollection);
    throw new UnsupportedOperationException("Not yet migrated");
  }

  @Override
  public void cleanup() {}

  private static class WindowIntoEvaluator<InputT> implements TransformEvaluator<InputT> {
    private final PTransformNode transform;
    private final WindowFn<InputT, ?> windowFn;
    private final UncommittedBundle<InputT> outputBundle;

    @SuppressWarnings("unchecked")
    public WindowIntoEvaluator(
        PTransformNode transform,
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
          new DirectAssignContext<InputT, W>(windowFn, element);
      Collection<? extends BoundedWindow> windows = windowFn.assignWindows(assignContext);
      return windows;
    }

    @Override
    public TransformResult<InputT> finishBundle() throws Exception {
      return StepTransformResult.<InputT>withoutHold(transform)
          .addOutput(outputBundle)
          .build();
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
