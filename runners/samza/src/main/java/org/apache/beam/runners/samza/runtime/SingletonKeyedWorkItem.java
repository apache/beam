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
package org.apache.beam.runners.samza.runtime;

import java.util.Collections;
import org.apache.beam.runners.core.KeyedWorkItem;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.sdk.util.WindowedValue;

/** Implementation of {@link KeyedWorkItem} which contains only a single value. */
class SingletonKeyedWorkItem<K, V> implements KeyedWorkItem<K, V> {
  private final K key;
  private final WindowedValue<V> value;

  public SingletonKeyedWorkItem(K key, WindowedValue<V> value) {
    this.key = key;
    this.value = value;
  }

  @Override
  public K key() {
    return key;
  }

  @Override
  public Iterable<TimerInternals.TimerData> timersIterable() {
    return Collections.emptyList();
  }

  @Override
  public Iterable<WindowedValue<V>> elementsIterable() {
    return Collections.singletonList(value);
  }
}
