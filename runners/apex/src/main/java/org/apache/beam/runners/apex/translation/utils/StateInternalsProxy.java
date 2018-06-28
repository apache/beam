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
package org.apache.beam.runners.apex.translation.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import java.io.Serializable;
import org.apache.beam.runners.core.StateInternals;
import org.apache.beam.runners.core.StateInternalsFactory;
import org.apache.beam.runners.core.StateNamespace;
import org.apache.beam.runners.core.StateTag;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.state.State;
import org.apache.beam.sdk.state.StateContext;

/** State internals for reusable processing context. */
@DefaultSerializer(JavaSerializer.class)
public class StateInternalsProxy<K> implements StateInternals, Serializable {

  private final ApexStateInternals.ApexStateInternalsFactory<K> factory;
  private transient K currentKey;

  public StateInternalsProxy(ApexStateInternals.ApexStateInternalsFactory<K> factory) {
    this.factory = factory;
  }

  public StateInternalsFactory<K> getFactory() {
    return this.factory;
  }

  public Coder<K> getKeyCoder() {
    return factory.getKeyCoder();
  }

  public void setKey(K key) {
    currentKey = key;
  }

  @Override
  public K getKey() {
    return currentKey;
  }

  @Override
  public <T extends State> T state(StateNamespace namespace, StateTag<T> address) {
    return factory.stateInternalsForKey(currentKey).state(namespace, address);
  }

  @Override
  public <T extends State> T state(
      StateNamespace namespace, StateTag<T> address, StateContext<?> c) {
    return factory.stateInternalsForKey(currentKey).state(namespace, address, c);
  }
}
