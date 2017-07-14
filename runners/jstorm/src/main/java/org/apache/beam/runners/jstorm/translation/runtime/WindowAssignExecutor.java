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
package org.apache.beam.runners.jstorm.translation.runtime;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Iterables;
import java.util.Collection;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.WindowFn;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JStorm {@link Executor} for {@link org.apache.beam.sdk.transforms.windowing.Window.Assign}.
 * @param <T>
 * @param <W>
 */
public class WindowAssignExecutor<T, W extends BoundedWindow> implements Executor {
  private static final Logger LOG = LoggerFactory.getLogger(WindowAssignExecutor.class);

  private final String description;
  private WindowFn<T, W> windowFn;
  private ExecutorsBolt executorsBolt;
  private TupleTag outputTag;

  class JStormAssignContext<InputT, W extends BoundedWindow>
      extends WindowFn<InputT, W>.AssignContext {
    private final WindowedValue<InputT> value;

    JStormAssignContext(WindowFn<InputT, W> fn, WindowedValue<InputT> value) {
      fn.super();
      checkArgument(
          Iterables.size(value.getWindows()) == 1,
          String.format(
              "%s passed to window assignment must be in a single window, but it was in %s: %s",
              WindowedValue.class.getSimpleName(),
              Iterables.size(value.getWindows()),
              value.getWindows()));
      this.value = value;
    }

    @Override
    public InputT element() {
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
  }

  public WindowAssignExecutor(String description, WindowFn<T, W> windowFn, TupleTag outputTag) {
    this.description = description;
    this.windowFn = windowFn;
    this.outputTag = outputTag;
  }

  @Override
  public void init(ExecutorContext context) {
    this.executorsBolt = context.getExecutorsBolt();
  }

  @Override
  public void process(TupleTag tag, WindowedValue elem) {
    Collection<W> windows = null;
    try {
      windows = windowFn.assignWindows(new JStormAssignContext<>(windowFn, elem));
      for (W window : windows) {
        executorsBolt.processExecutorElem(
            outputTag,
            WindowedValue.of(elem.getValue(), elem.getTimestamp(), window, elem.getPane()));
      }
    } catch (Exception e) {
      LOG.warn("Failed to assign windows for elem=" + elem, e);
    }
  }

  @Override
  public void cleanup() {
  }


  @Override
  public String toString() {
    return description;
  }
}
