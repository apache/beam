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

import static com.google.cloud.dataflow.sdk.util.WindowUtils.bufferTag;

import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.cloud.dataflow.sdk.values.KV;

import java.util.Collection;

/**
 * A WindowSet where each value is placed in exactly one window,
 * and windows are never merged, deleted, or flushed early, and the
 * WindowSet itself is never exposed to user code, allowing
 * a much simpler (and cheaper) implementation.
 *
 * <p>This WindowSet only works with {@link StreamingGroupAlsoByWindowsDoFn}.
 */
class PartitionBufferingWindowSet<K, V, W extends BoundedWindow>
    extends AbstractWindowSet<K, V, Iterable<V>, W> {
  PartitionBufferingWindowSet(
      K key,
      WindowFn<?, W> windowFn,
      Coder<V> inputCoder,
      DoFnProcessContext<?, KV<K, Iterable<V>>> context,
      ActiveWindowManager<W> activeWindowManager) {
    super(key, windowFn, inputCoder, context, activeWindowManager);
  }

  @Override
  public void put(W window, V value) throws Exception {
    context.context.stepContext.writeToTagList(
        bufferTag(window, windowFn.windowCoder(), inputCoder), value, context.timestamp());
    // Adds the window even if it is already present, relying on the streaming backend to
    // de-deduplicate.
    activeWindowManager.addWindow(window);
  }

  @Override
  public void remove(W window) throws Exception {
    context.context.stepContext.deleteTagList(
        bufferTag(window, windowFn.windowCoder(), inputCoder));
    activeWindowManager.removeWindow(window);
  }

  @Override
  public void merge(Collection<W> otherWindows, W newWindow) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<W> windows() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean contains(W window) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Iterable<V> finalValue(W window) throws Exception {
    CodedTupleTag<V> tag = bufferTag(window, windowFn.windowCoder(), inputCoder);
    Iterable<V> result = context.context.stepContext.readTagList(tag);
    if (result == null) {
      throw new IllegalStateException("finalValue called for non-existent window");
    }
    return result;
  }
}
