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
package org.apache.beam.runners.apex.translation;

import org.apache.beam.runners.apex.translation.operators.ApexProcessFnOperator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.values.PCollection;

/** {@link Window} is translated to {@link ApexProcessFnOperator#assignWindows}. */
class WindowAssignTranslator<T> implements TransformTranslator<Window.Assign<T>> {
  private static final long serialVersionUID = 1L;

  @Override
  public void translate(Window.Assign<T> transform, TranslationContext context) {
    PCollection<T> output = context.getOutput();
    PCollection<T> input = context.getInput();

    if (transform.getWindowFn() == null) {
      // no work to do
      context.addAlias(output, input);
    } else {
      @SuppressWarnings("unchecked")
      WindowFn<T, BoundedWindow> windowFn = (WindowFn<T, BoundedWindow>) transform.getWindowFn();
      ApexProcessFnOperator<T> operator =
          ApexProcessFnOperator.assignWindows(windowFn, context.getPipelineOptions());
      context.addOperator(operator, operator.outputPort);
      context.addStream(context.getInput(), operator.inputPort);
    }
  }
}
