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
package org.apache.beam.runners.jstorm.translation;

import java.util.Collections;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;

/**
 * Singleton keyed word item.
 * @param <K>
 * @param <ElemT>
 */
class SingletonKeyedWorkItem<K, ElemT> implements KeyedWorkItem<K, ElemT> {

  final K key;
  final WindowedValue<ElemT> value;

  private SingletonKeyedWorkItem(K key, WindowedValue<ElemT> value) {
    this.key = key;
    this.value = value;
  }

  public static <K, ElemT> SingletonKeyedWorkItem<K, ElemT> of(K key, WindowedValue<ElemT> value) {
    return new SingletonKeyedWorkItem<K, ElemT>(key, value);
  }

  @Override
  public K key() {
    return key;
  }

  public WindowedValue<ElemT> value() {
    return value;
  }

  @Override
  public Iterable<TimerInternals.TimerData> timersIterable() {
    return Collections.EMPTY_LIST;
  }

  @Override
  public Iterable<WindowedValue<ElemT>> elementsIterable() {
    return Collections.singletonList(value);
  }
}
