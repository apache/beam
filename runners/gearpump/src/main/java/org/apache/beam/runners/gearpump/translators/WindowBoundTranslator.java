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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.PCollection;
import org.apache.gearpump.Message;
import org.apache.gearpump.cluster.UserConfig;
import org.apache.gearpump.streaming.dsl.javaapi.JavaStream;
import org.apache.gearpump.streaming.javaapi.Task;
import org.apache.gearpump.streaming.javaapi.dsl.functions.FlatMapFunction;
import org.apache.gearpump.streaming.task.TaskContext;
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
    WindowFn<T, BoundedWindow> windowFn =
        (WindowFn<T, BoundedWindow>) outputStrategy.getWindowFn();
    JavaStream<WindowedValue<T>> outputStream =
        inputStream
            .flatMap(new AssignWindows(windowFn), "assign_windows")
            .process(AssignTimestampTask.class, 1, UserConfig.empty(), "assign_timestamp");

    context.setOutputStream(context.getOutput(transform), outputStream);
  }

  private static class AssignWindows<T> implements
      FlatMapFunction<WindowedValue<T>, WindowedValue<T>> {

    private final WindowFn<T, BoundedWindow> fn;

    AssignWindows(WindowFn<T, BoundedWindow> fn) {
      this.fn = fn;
    }

    @Override
    public Iterator<WindowedValue<T>> apply(final WindowedValue<T> value) {
      List<WindowedValue<T>>  ret = new LinkedList<>();
      try {
        Collection<BoundedWindow> windows = fn.assignWindows(fn.new AssignContext() {
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
        for (BoundedWindow window: windows) {
          ret.add(WindowedValue.of(
              value.getValue(), value.getTimestamp(), window, value.getPane()));
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return ret.iterator();
    }
  }

  /**
   * Assign WindowedValue timestamp to Gearpump message.
   * @param <T> element type of WindowedValue
   */
  public static class AssignTimestampTask<T> extends Task {

    public AssignTimestampTask(TaskContext taskContext, UserConfig userConfig) {
      super(taskContext, userConfig);
    }

    @Override
    public void onNext(Message message) {
      final WindowedValue<T> value = (WindowedValue<T>) message.msg();
      context.output(Message.apply(value, value.getTimestamp().getMillis()));
    }
  }
}
