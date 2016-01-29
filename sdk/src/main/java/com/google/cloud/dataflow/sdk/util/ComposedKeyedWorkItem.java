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

import com.google.cloud.dataflow.sdk.util.TimerInternals.TimerData;

/**
 * A {@link KeyedWorkItem} composed of an underlying key, {@link TimerData} iterable, and element
 * iterable.
 */
public class ComposedKeyedWorkItem<K, ElemT> implements KeyedWorkItem<K, ElemT> {

  private final K key;
  private final Iterable<TimerData> timers;
  private final Iterable<WindowedValue<ElemT>> elements;

  public static <K, ElemT> ComposedKeyedWorkItem<K, ElemT> create(
      K key, Iterable<TimerData> timers, Iterable<WindowedValue<ElemT>> elements) {
    return new ComposedKeyedWorkItem<K, ElemT>(key, timers, elements);
  }

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
}
