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

package org.apache.beam.runners.gearpump.translators;

import com.google.common.collect.Iterables;

import java.util.Collection;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.gearpump.streaming.dsl.api.functions.MapFunction;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.joda.time.Instant;

/**
 * {@link Window.Bound} is translated to Gearpump flatMap function.
 */
@SuppressWarnings("unchecked")
public class WindowBoundTranslator<T> implements  TransformTranslator<Window.Bound<T>> {

  @Override
  public void translate(Window.Bound<T> transform, TranslationContext context) {
    PCollection<T> input = context.getInput(transform);
    JavaStream<WindowedValue<T>> inputStream = context.getInputStream(input);
    WindowingStrategy<?, ?> outputStrategy =
        transform.getOutputStrategyInternal(input.getWindowingStrategy());
    WindowFn<T, BoundedWindow> windowFn = (WindowFn<T, BoundedWindow>) outputStrategy.getWindowFn();
    JavaStream<WindowedValue<T>> outputStream =
        inputStream
            .map(new AssignWindows(windowFn), "assign_windows");

    context.setOutputStream(context.getOutput(transform), outputStream);
  }

  private static class AssignWindows<T> extends
      MapFunction<WindowedValue<T>, WindowedValue<T>> {

    private final WindowFn<T, BoundedWindow> windowFn;

    AssignWindows(WindowFn<T, BoundedWindow> windowFn) {
      this.windowFn = windowFn;
    }

    @Override
    public WindowedValue<T> apply(final WindowedValue<T> value) {
      try {
        Collection<BoundedWindow> windows = windowFn.assignWindows(windowFn.new AssignContext() {
          @Override
          public T element() {
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
        });
        return WindowedValue.of(value.getValue(), value.getTimestamp(), windows, value.getPane());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
