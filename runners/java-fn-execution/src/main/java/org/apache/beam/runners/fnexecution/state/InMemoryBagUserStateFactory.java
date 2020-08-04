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
package org.apache.beam.runners.fnexecution.state;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.beam.runners.core.InMemoryStateInternals;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateNamespaces;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.runners.core.StateTags;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

/**
 * Holds user state in memory. Only one key is active at a time due to the GroupReduceFunction being
 * called once per key. Needs to be reset via {@code resetForNewKey()} before processing a new key.
 */
public class InMemoryBagUserStateFactory<K, V, W extends BoundedWindow>
    implements StateRequestHandlers.BagUserStateHandlerFactory<K, V, W> {

  final List<InMemorySingleKeyBagState> handlers;

  public InMemoryBagUserStateFactory() {
    handlers = new ArrayList<>();
  }

  @Override
  public StateRequestHandlers.BagUserStateHandler<K, V, W> forUserState(
      String pTransformId,
      String userStateId,
      Coder<K> keyCoder,
      Coder<V> valueCoder,
      Coder<W> windowCoder) {

    InMemorySingleKeyBagState<K, V, W> bagUserStateHandler =
        new InMemorySingleKeyBagState<>(userStateId, valueCoder, windowCoder);
    handlers.add(bagUserStateHandler);

    return bagUserStateHandler;
  }

  /** Prepares previous emitted state handlers for processing a new key. */
  public void resetForNewKey() {
    for (InMemorySingleKeyBagState stateBags : handlers) {
      stateBags.reset();
    }
  }

  static class InMemorySingleKeyBagState<K, V, W extends BoundedWindow>
      implements StateRequestHandlers.BagUserStateHandler<K, V, W> {

    private final StateTag<BagState<V>> stateTag;
    private final Coder<W> windowCoder;

    /* Lazily initialized state internals upon first access */
    private volatile StateInternals stateInternals;

    InMemorySingleKeyBagState(String userStateId, Coder<V> valueCoder, Coder<W> windowCoder) {
      this.windowCoder = windowCoder;
      this.stateTag = StateTags.bag(userStateId, valueCoder);
    }

    @Override
    public Iterable<V> get(K key, W window) {
      initStateInternals(key);
      StateNamespace namespace = StateNamespaces.window(windowCoder, window);
      BagState<V> bagState = stateInternals.state(namespace, stateTag);
      return bagState.read();
    }

    @Override
    public void append(K key, W window, Iterator<V> values) {
      initStateInternals(key);
      StateNamespace namespace = StateNamespaces.window(windowCoder, window);
      BagState<V> bagState = stateInternals.state(namespace, stateTag);
      while (values.hasNext()) {
        bagState.add(values.next());
      }
    }

    @Override
    public void clear(K key, W window) {
      initStateInternals(key);
      StateNamespace namespace = StateNamespaces.window(windowCoder, window);
      BagState<V> bagState = stateInternals.state(namespace, stateTag);
      bagState.clear();
    }

    private void initStateInternals(K key) {
      if (stateInternals == null) {
        stateInternals = InMemoryStateInternals.forKey(key);
      }
    }

    void reset() {
      stateInternals = null;
    }
  }
}
