/*
 * Copyright (C) 2015 Google Inc.
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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.WindowFn;
import com.google.cloud.dataflow.sdk.util.Trigger.WindowStatus;
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
      DoFn<?, KV<K, Iterable<V>>>.ProcessContext context) {
    super(key, windowFn, inputCoder, context);
  }

  @Override
  public WindowStatus put(W window, V value) throws Exception {
    context.windowingInternals().writeToTagList(
        bufferTag(window, windowFn.windowCoder(), inputCoder), value, context.timestamp());
    // Adds the window even if it is already present, relying on the streaming backend to
    // de-duplicate.
    return WindowStatus.UNKNOWN;
  }

  @Override
  public void remove(W window) throws Exception {
    context.windowingInternals().deleteTagList(
        bufferTag(window, windowFn.windowCoder(), inputCoder));
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
    Iterable<V> result = context.windowingInternals().readTagList(tag);
    if (result == null) {
      throw new IllegalStateException("finalValue called for non-existent window");
    }
    return result;
  }
}
