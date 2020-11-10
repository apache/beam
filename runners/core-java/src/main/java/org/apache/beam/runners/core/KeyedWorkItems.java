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
package org.apache.beam.runners.core;

import java.util.Collections;
import java.util.Objects;
import org.apache.beam.runners.core.TimerInternals.TimerData;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Iterables;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Static utility methods that provide {@link KeyedWorkItem} implementations. */
public class KeyedWorkItems {
  /**
   * Returns an implementation of {@link KeyedWorkItem} that wraps around an elements iterable.
   *
   * @param <K> the key type
   * @param <ElemT> the element type
   */
  public static <K, ElemT> KeyedWorkItem<K, ElemT> elementsWorkItem(
      K key, Iterable<WindowedValue<ElemT>> elementsIterable) {
    return new ComposedKeyedWorkItem<>(key, Collections.emptyList(), elementsIterable);
  }

  /**
   * Returns an implementation of {@link KeyedWorkItem} that wraps around an timers iterable.
   *
   * @param <K> the key type
   * @param <ElemT> the element type
   */
  public static <K, ElemT> KeyedWorkItem<K, ElemT> timersWorkItem(
      K key, Iterable<TimerData> timersIterable) {
    return new ComposedKeyedWorkItem<>(
        key, timersIterable, Collections.<WindowedValue<ElemT>>emptyList());
  }

  /**
   * Returns an implementation of {@link KeyedWorkItem} that wraps around an timers iterable and an
   * elements iterable.
   *
   * @param <K> the key type
   * @param <ElemT> the element type
   */
  public static <K, ElemT> KeyedWorkItem<K, ElemT> workItem(
      K key, Iterable<TimerData> timersIterable, Iterable<WindowedValue<ElemT>> elementsIterable) {
    return new ComposedKeyedWorkItem<>(key, timersIterable, elementsIterable);
  }

  /**
   * A {@link KeyedWorkItem} composed of an underlying key, {@link TimerData} iterable, and element
   * iterable.
   */
  public static class ComposedKeyedWorkItem<K, ElemT> implements KeyedWorkItem<K, ElemT> {
    private final K key;
    private final Iterable<TimerData> timers;
    private final Iterable<WindowedValue<ElemT>> elements;

    private ComposedKeyedWorkItem(
        K key, Iterable<TimerData> timers, Iterable<WindowedValue<ElemT>> elements) {
      this.key = key;
      this.timers = timers;
      this.elements = elements;
    }

    @Override
    public K key() {
      return key;
    }

    @Override
    public Iterable<TimerData> timersIterable() {
      return timers;
    }

    @Override
    public Iterable<WindowedValue<ElemT>> elementsIterable() {
      return elements;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (other == null || !(other instanceof ComposedKeyedWorkItem)) {
        return false;
      }
      KeyedWorkItem<?, ?> that = (KeyedWorkItem<?, ?>) other;
      return Objects.equals(this.key, that.key())
          && Iterables.elementsEqual(this.timersIterable(), that.timersIterable())
          && Iterables.elementsEqual(this.elementsIterable(), that.elementsIterable());
    }

    @Override
    public int hashCode() {
      return Objects.hash(key, timers, elements);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(ComposedKeyedWorkItem.class)
          .add("key", key)
          .add("elements", elements)
          .add("timers", timers)
          .toString();
    }
  }
}
