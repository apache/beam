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
package org.apache.beam.runners.spark.structuredstreaming.translation.batch;

import java.util.Collection;
import org.apache.beam.runners.spark.structuredstreaming.translation.TransformTranslator;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Instant;

class WindowAssignTranslatorBatch<T>
    extends TransformTranslator<PCollection<T>, PCollection<T>, Window.Assign<T>> {

  WindowAssignTranslatorBatch() {
    super(0.05f);
  }

  @Override
  public void translate(Window.Assign<T> transform, Context cxt) {
    WindowFn<T, ?> windowFn = transform.getWindowFn();
    PCollection<T> input = cxt.getInput();
    Dataset<WindowedValue<T>> inputDataset = cxt.getDataset(input);

    if (windowFn == null || skipAssignWindows(windowFn, input)) {
      cxt.putDataset(cxt.getOutput(), inputDataset);
    } else {
      Dataset<WindowedValue<T>> outputDataset =
          inputDataset.map(
              assignWindows(windowFn),
              cxt.windowedEncoder(input.getCoder(), windowFn.windowCoder()));

      cxt.putDataset(cxt.getOutput(), outputDataset);
    }
  }

  /**
   * Checks if the window transformation should be applied or skipped.
   *
   * <p>Avoid running assign windows if both source and destination are global window or if the user
   * has not specified the WindowFn (meaning they are just messing with triggering or allowed
   * lateness).
   */
  private boolean skipAssignWindows(WindowFn<T, ?> newFn, PCollection<T> input) {
    WindowFn<?, ?> currentFn = input.getWindowingStrategy().getWindowFn();
    return currentFn instanceof GlobalWindows && newFn instanceof GlobalWindows;
  }

  private static <T, W extends @NonNull BoundedWindow>
      MapFunction<WindowedValue<T>, WindowedValue<T>> assignWindows(WindowFn<T, W> windowFn) {
    return value -> {
      final BoundedWindow window = getOnlyWindow(value);
      final T element = value.getValue();
      final Instant timestamp = value.getTimestamp();
      Collection<W> windows =
          windowFn.assignWindows(
              windowFn.new AssignContext() {

                @Override
                public T element() {
                  return element;
                }

                @Override
                public @NonNull Instant timestamp() {
                  return timestamp;
                }

                @Override
                public @NonNull BoundedWindow window() {
                  return window;
                }
              });
      return WindowedValue.of(element, timestamp, windows, value.getPane());
    };
  }

  private static <T> BoundedWindow getOnlyWindow(WindowedValue<T> wv) {
    return Iterables.getOnlyElement((Iterable<BoundedWindow>) wv.getWindows());
  }
}
