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
package org.apache.beam.runners.spark.structuredstreaming.translation.helpers;

import com.google.common.collect.Iterables;
import java.util.Collection;
import org.apache.beam.runners.spark.structuredstreaming.translation.TranslationContext;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.spark.api.java.function.MapFunction;
import org.joda.time.Instant;

/** Helper functions for working with windows. */
public final class WindowingHelpers {

  /**
   * A Spark {@link MapFunction} for converting a value to a {@link WindowedValue}. The resulting
   * {@link WindowedValue} will be in a global windows, and will have the default timestamp == MIN
   * and pane.
   *
   * @param <T> The type of the object.
   * @return A {@link MapFunction} that accepts an object and returns its {@link WindowedValue}.
   */
  public static <T> MapFunction<T, WindowedValue<T>> windowMapFunction() {
    return new MapFunction<T, WindowedValue<T>>() {

      @Override public WindowedValue<T> call(T t) {
        return WindowedValue.valueInGlobalWindow(t);
      }
    };
  }

  /**
   * A Spark {@link MapFunction} for extracting the value from a {@link WindowedValue}.
   *
   * @param <T> The type of the object.
   * @return A {@link MapFunction} that accepts a {@link WindowedValue} and returns its value.
   */
  public static <T> MapFunction<WindowedValue<T>, T> unwindowMapFunction() {
    return new MapFunction<WindowedValue<T>, T>() {

      @Override public T call(WindowedValue<T> t) {
        return t.getValue();
      }
    };
  }

  /**
   * Checks if the window transformation should be applied or skipped.
   *
   * <p>Avoid running assign windows if both source and destination are global window or if the user
   * has not specified the WindowFn (meaning they are just messing with triggering or allowed
   * lateness).
   *
   */
  @SuppressWarnings("unchecked") public static <T, W extends BoundedWindow> boolean skipAssignWindows(
      Window.Assign<T> transform, TranslationContext context) {
    WindowFn<? super T, W> windowFnToApply = (WindowFn<? super T, W>) transform.getWindowFn();
    PCollection<T> input = (PCollection<T>) context.getInput();
    WindowFn<?, ?> windowFnOfInput = input.getWindowingStrategy().getWindowFn();
    return windowFnToApply == null || (windowFnOfInput instanceof GlobalWindows
        && windowFnToApply instanceof GlobalWindows);
  }

  public static <T, W extends BoundedWindow> MapFunction<WindowedValue<T>, WindowedValue<T>> assignWindowsMapFunction(
      WindowFn<T, W> windowFn) {
    return new MapFunction<WindowedValue<T>, WindowedValue<T>>() {

      @Override public WindowedValue<T> call(WindowedValue<T> windowedValue) throws Exception {
        final BoundedWindow boundedWindow = Iterables.getOnlyElement(windowedValue.getWindows());
        final T element = windowedValue.getValue();
        final Instant timestamp = windowedValue.getTimestamp();
        Collection<W> windows = windowFn.assignWindows(windowFn.new AssignContext() {

          @Override public T element() {
            return element;
          }

          @Override public Instant timestamp() {
            return timestamp;
          }

          @Override public BoundedWindow window() {
            return boundedWindow;
          }
        });
        return WindowedValue.of(element, timestamp, windows, windowedValue.getPane());
      }
    };
  }
}

