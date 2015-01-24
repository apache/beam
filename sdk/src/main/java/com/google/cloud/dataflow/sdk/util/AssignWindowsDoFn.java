/*
 * Copyright (C) 2014 Google Inc.
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

package com.google.cloud.dataflow.sdk.util;

import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;

import org.joda.time.Instant;

import java.util.Collection;

/**
 * {@link DoFn} that tags elements of a PCollection with windows, according
 * to the provided {@link WindowFn}.
 * @param <T> Type of elements being windowed
 * @param <W> Window type
 */
@SuppressWarnings("serial")
public class AssignWindowsDoFn<T, W extends BoundedWindow> extends DoFn<T, T> {
  private WindowFn<? super T, W> fn;

  public AssignWindowsDoFn(WindowFn<? super T, W> fn) {
    this.fn = fn;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void processElement(ProcessContext c) throws Exception {
    final DoFnProcessContext<T, T> context = (DoFnProcessContext<T, T>) c;
    Collection<W> windows =
        ((WindowFn<T, W>) fn).assignWindows(
            ((WindowFn<T, W>) fn).new AssignContext() {
                @Override
                public T element() {
                  return context.element();
                }

                @Override
                public Instant timestamp() {
                  return context.timestamp();
                }

                @Override
                public Collection<? extends BoundedWindow> windows() {
                  return context.windows();
                }
              });

    context.outputWindowedValue(context.element(), context.timestamp(), windows);
  }
}
