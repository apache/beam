/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.runners.dataflow;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.WindowingStrategy;
import com.google.cloud.dataflow.sdk.values.PCollection;

/**
 * A primitive {@link PTransform} that implements the {@link Window#into(WindowFn)}
 * {@link PTransform}.
 *
 * For an application of {@link Window#into(WindowFn)} that changes the {@link WindowFn}, applies
 * a primitive {@link PTransform} in the Dataflow service.
 *
 * For an application of {@link Window#into(WindowFn)} that does not change the {@link WindowFn},
 * applies an identity {@link ParDo} and sets the windowing strategy of the output
 * {@link PCollection}.
 *
 * For internal use only.
 *
 * @param <T> the type of input element
 */
public class AssignWindows<T> extends PTransform<PCollection<T>, PCollection<T>> {
  private final Window.Bound<T> transform;

  /**
   * Builds an instance of this class from the overriden transform.
   */
  @SuppressWarnings("unused") // Used via reflection
  public AssignWindows(Window.Bound<T> transform) {
    this.transform = transform;
  }

  @Override
  public PCollection<T> apply(PCollection<T> input) {
    WindowingStrategy<?, ?> outputStrategy =
        transform.getOutputStrategyInternal(input.getWindowingStrategy());
    if (transform.getWindowFn() != null) {
      // If the windowFn changed, we create a primitive, and run the AssignWindows operation here.
      return PCollection.<T>createPrimitiveOutputInternal(
                            input.getPipeline(), outputStrategy, input.isBounded());
    } else {
      // If the windowFn didn't change, we just run a pass-through transform and then set the
      // new windowing strategy.
      return input.apply(ParDo.named("Identity").of(new DoFn<T, T>() {
        @Override
        public void processElement(DoFn<T, T>.ProcessContext c) throws Exception {
          c.output(c.element());
        }
      })).setWindowingStrategyInternal(outputStrategy);
    }
  }

  @Override
  public void validate(PCollection<T> input) {
    transform.validate(input);
  }

  @Override
  protected Coder<?> getDefaultOutputCoder(PCollection<T> input) {
    return input.getCoder();
  }

  @Override
  protected String getKindString() {
    return "Window.Into()";
  }
}

