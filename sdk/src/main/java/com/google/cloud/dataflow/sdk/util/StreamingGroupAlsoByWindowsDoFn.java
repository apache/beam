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

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.PartitioningWindowFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.common.base.Preconditions;

import java.io.IOException;

/**
 * DoFn that merges windows and groups elements in those windows.
 *
 * @param <K> key type
 * @param <VI> input value element type
 * @param <VO> output value element type
 * @param <W> window type
 */
@SuppressWarnings("serial")
public class StreamingGroupAlsoByWindowsDoFn<K, VI, VO, W extends BoundedWindow>
    extends DoFn<TimerOrElement<KV<K, VI>>, KV<K, VO>> implements DoFn.RequiresKeyedState {

  protected WindowFn<?, W> windowFn;
  protected Coder<VI> inputCoder;

  protected StreamingGroupAlsoByWindowsDoFn(
      WindowFn<?, W> windowFn,
      Coder<VI> inputCoder) {
    this.windowFn = windowFn;
    this.inputCoder = inputCoder;
  }

  public static <K, VI, VO, W extends BoundedWindow>
      StreamingGroupAlsoByWindowsDoFn<K, VI, VO, W> create(
          WindowFn<?, W> windowFn,
          Coder<VI> inputCoder) {
    return new StreamingGroupAlsoByWindowsDoFn<>(windowFn, inputCoder);
  }

  private AbstractWindowSet<K, VI, VO, W> createWindowSet(
      K key,
      DoFnProcessContext<?, KV<K, VO>> context,
      AbstractWindowSet.ActiveWindowManager<W> activeWindowManager) throws Exception {
    if (windowFn instanceof PartitioningWindowFn) {
      return new PartitionBufferingWindowSet(
          key, windowFn, inputCoder, context, activeWindowManager);
    } else {
      return new BufferingWindowSet(key, windowFn, inputCoder, context, activeWindowManager);
    }
  }

  @Override
  public void processElement(ProcessContext processContext) throws Exception {
    DoFnProcessContext<TimerOrElement<KV<K, VI>>, KV<K, VO>> context =
        (DoFnProcessContext<TimerOrElement<KV<K, VI>>, KV<K, VO>>) processContext;
    if (!context.element().isTimer()) {
      KV<K, VI> element = context.element().element();
      K key = element.getKey();
      VI value = element.getValue();
      AbstractWindowSet<K, VI, VO, W> windowSet = createWindowSet(
          key, context, new StreamingActiveWindowManager<>(windowFn, context));

      for (BoundedWindow window : context.windows()) {
        windowSet.put((W) window, value);
      }

      windowSet.flush();
    } else {
      TimerOrElement<?> timer = context.element();
      AbstractWindowSet<K, VI, VO, W> windowSet = createWindowSet(
          (K) timer.key(), context, new StreamingActiveWindowManager<>(windowFn, context));

      // Attempt to merge windows before emitting; that may remove the current window under
      // consideration.
      ((WindowFn<Object, W>) windowFn)
        .mergeWindows(new AbstractWindowSet.WindowMergeContext<Object, W>(windowSet, windowFn));

      W window = WindowUtils.windowFromString(timer.tag(), windowFn.windowCoder());

      if ((windowFn instanceof PartitioningWindowFn) || windowSet.contains(window)) {
        Preconditions.checkState(!timer.timestamp().isBefore(window.maxTimestamp()));
        windowSet.markCompleted(window);
        windowSet.flush();
      }
    }
  }

  private static class StreamingActiveWindowManager<W extends BoundedWindow>
      implements AbstractWindowSet.ActiveWindowManager<W> {
    WindowFn<?, W> windowFn;
    DoFnProcessContext<?, ?> context;

    StreamingActiveWindowManager(
        WindowFn<?, W> windowFn,
        DoFnProcessContext<?, ?> context) {
      this.windowFn = windowFn;
      this.context = context;
    }

    @Override
    public void addWindow(W window) throws IOException {
      context.context.stepContext.getExecutionContext().setTimer(
          WindowUtils.windowToString(window, windowFn.windowCoder()), window.maxTimestamp());
    }

    @Override
    public void removeWindow(W window) throws IOException {
      if (windowFn instanceof PartitioningWindowFn) {
        // For PartitioningWindowFn, each window triggers exactly one timer.
        // And, timers are automatically deleted once they are fired.
        return;
      }
      context.context.stepContext.getExecutionContext().deleteTimer(
          WindowUtils.windowToString(window, windowFn.windowCoder()));
    }
  }
}
