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
package org.apache.beam.runners.dataflow.internal;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;

/**
 * A primitive {@link PTransform} that implements the {@link Window#into(WindowFn)}
 * {@link PTransform}.
 *
 * <p>For an application of {@link Window#into(WindowFn)} that changes the {@link WindowFn}, applies
 * a primitive {@link PTransform} in the Dataflow service.
 *
 * <p>For an application of {@link Window#into(WindowFn)} that does not change the {@link WindowFn},
 * applies an identity {@link ParDo} and sets the windowing strategy of the output
 * {@link PCollection}.
 *
 * <p>For internal use only.
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
  public PCollection<T> expand(PCollection<T> input) {
    WindowingStrategy<?, ?> outputStrategy =
        transform.getOutputStrategyInternal(input.getWindowingStrategy());
    if (transform.getWindowFn() != null) {
      // If the windowFn changed, we create a primitive, and run the AssignWindows operation here.
      return PCollection.<T>createPrimitiveOutputInternal(
                            input.getPipeline(), outputStrategy, input.isBounded());
    } else {
      // If the windowFn didn't change, we just run a pass-through transform and then set the
      // new windowing strategy.
      return input.apply("Identity", ParDo.of(new DoFn<T, T>() {
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {
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
