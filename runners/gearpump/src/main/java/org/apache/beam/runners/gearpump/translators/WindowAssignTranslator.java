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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.WindowingStrategy;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.dsl.javaapi.functions.FlatMapFunction;
import org.joda.time.Instant;

/**
 * {@link Window.Assign} is translated to Gearpump flatMap function.
 */
@SuppressWarnings("unchecked")
public class WindowAssignTranslator<T> implements TransformTranslator<Window.Assign<T>> {

  private static final long serialVersionUID = -964887482120489061L;

  @Override
  public void translate(Window.Assign<T> transform, TranslationContext context) {
    PCollection<T> input = (PCollection<T>) context.getInput();
    PCollection<T> output = (PCollection<T>) context.getOutput();
    JavaStream<WindowedValue<T>> inputStream = context.getInputStream(input);
    WindowingStrategy<?, ?> outputStrategy = output.getWindowingStrategy();
    WindowFn<T, BoundedWindow> windowFn = (WindowFn<T, BoundedWindow>) outputStrategy.getWindowFn();
    JavaStream<WindowedValue<T>> outputStream =
        inputStream
            .flatMap(new AssignWindows(windowFn), "assign_windows");

    context.setOutputStream(output, outputStream);
  }

  /**
   * A Function used internally by Gearpump to wrap the actual Beam's WindowFn.
   */
  protected static class AssignWindows<T> extends
      FlatMapFunction<WindowedValue<T>, WindowedValue<T>> {

    private static final long serialVersionUID = 7284565861938681360L;
    private final WindowFn<T, BoundedWindow> windowFn;

    AssignWindows(WindowFn<T, BoundedWindow> windowFn) {
      this.windowFn = windowFn;
    }

    @Override
    public Iterator<WindowedValue<T>> flatMap(final WindowedValue<T> value) {
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
        List<WindowedValue<T>> values = new ArrayList<>(windows.size());
        for (BoundedWindow win: windows) {
          values.add(
              WindowedValue.of(value.getValue(), value.getTimestamp(), win, value.getPane()));
        }
        return values.iterator();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
