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
import com.google.cloud.dataflow.sdk.transforms.DoFn.KeyedState;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.Trigger.WindowStatus;
import com.google.cloud.dataflow.sdk.values.CodedTupleTag;
import com.google.common.collect.ImmutableList;

import java.util.Collection;

/**
 * A WindowSet where windows are never merged or deleted. This allows us to improve upon the default
 * {@link BufferingWindowSet} by not maintaining a merge tree (or the list of active windows at all)
 * and by blindly using tag lists to store elements.
 */
class NonMergingBufferingWindowSet<K, V, W extends BoundedWindow>
    extends AbstractWindowSet<K, V, Iterable<V>, W> {

  public static <K, V, W extends BoundedWindow>
  AbstractWindowSet.Factory<K, V, Iterable<V>, W> factory(final Coder<V> inputCoder) {
    return new AbstractWindowSet.Factory<K, V, Iterable<V>, W>() {

      private static final long serialVersionUID = 0L;

      @Override
      public AbstractWindowSet<K, V, Iterable<V>, W> create(K key,
          Coder<W> windowFn, KeyedState keyedState,
          WindowingInternals<?, ?> windowingInternals) throws Exception {
        return new NonMergingBufferingWindowSet<>(
            key, windowFn, inputCoder, keyedState, windowingInternals);
      }
    };
  }

  private NonMergingBufferingWindowSet(
      K key,
      Coder<W> windowCoder,
      Coder<V> inputCoder,
      KeyedState keyedState,
      WindowingInternals<?, ?> windowingInternals) {
    super(key, windowCoder, inputCoder, keyedState, windowingInternals);
  }

  @Override
  public WindowStatus put(W window, V value) throws Exception {
    windowingInternals.writeToTagList(bufferTag(window, windowCoder, inputCoder), value);

    // Adds the window even if it is already present, relying on the streaming backend to
    // de-duplicate. As such, we don't know if this was a genuinely new window.
    return WindowStatus.UNKNOWN;
  }

  @Override
  public void remove(W window) throws Exception {
    windowingInternals.deleteTagList(bufferTag(window, windowCoder, inputCoder));
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
    throw new UnsupportedOperationException(
        "NonMergingBufferingWindowSet does not supporting reading the active window set.");
  }

  @Override
  protected Iterable<V> finalValue(W window) throws Exception {
    CodedTupleTag<V> tag = bufferTag(window, windowCoder, inputCoder);
    Iterable<V> result = windowingInternals.readTagList(tag);
    if (result == null) {
      return null;
    }

    // Create a copy here, since otherwise we may return the same list object from readTagList, and
    // that may be mutated later, which would lead to mutation of output values.
    return ImmutableList.copyOf(result);
  }
}
