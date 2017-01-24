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

import java.util.Collections;
import org.apache.beam.runners.apex.ApexPipelineOptions;
import org.apache.beam.runners.apex.translation.operators.ApexParDoOperator;
import org.apache.beam.runners.core.AssignWindowsDoFn;
import org.apache.beam.runners.core.DoFnAdapters;
import org.apache.beam.runners.core.OldDoFn;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/**
 * {@link Window.Bound} is translated to {link ApexParDoOperator} that wraps an {@link
 * AssignWindowsDoFn}.
 */
class WindowBoundTranslator<T> implements TransformTranslator<Window.Bound<T>> {
  private static final long serialVersionUID = 1L;

  @Override
  public void translate(Window.Bound<T> transform, TranslationContext context) {
    PCollection<T> output = (PCollection<T>) context.getOutput();
    PCollection<T> input = (PCollection<T>) context.getInput();
    @SuppressWarnings("unchecked")
    WindowingStrategy<T, BoundedWindow> windowingStrategy =
        (WindowingStrategy<T, BoundedWindow>) output.getWindowingStrategy();

    OldDoFn<T, T> fn =
        (transform.getWindowFn() == null)
            ? DoFnAdapters.toOldDoFn(new IdentityFn<T>())
            : new AssignWindowsDoFn<>(transform.getWindowFn());

    ApexParDoOperator<T, T> operator =
        new ApexParDoOperator<T, T>(
            context.getPipelineOptions().as(ApexPipelineOptions.class),
            fn,
            new TupleTag<T>(),
            TupleTagList.empty().getAll(),
            windowingStrategy,
            Collections.<PCollectionView<?>>emptyList(),
            WindowedValue.getFullCoder(
                input.getCoder(), windowingStrategy.getWindowFn().windowCoder()),
            context.<Void>stateInternalsFactory());
    context.addOperator(operator, operator.output);
    context.addStream(context.getInput(), operator.input);
  }

  private static class IdentityFn<T> extends DoFn<T, T> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(c.element());
    }
  }
}
