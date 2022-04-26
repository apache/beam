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
package org.apache.beam.sdk.io.aws2;

import static java.util.Collections.synchronizedMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Static supplier supporting unserializable objects such as mocks for unit tests. */
public class StaticSupplier<V, T extends StaticSupplier<V, T>>
    implements Supplier<V>, Serializable {
  private static final Map<Integer, Object> objects = synchronizedMap(new HashMap<>());

  private int id;
  private transient boolean cleanup;

  protected T withObject(V object) {
    id = System.identityHashCode(object);
    cleanup = true;
    objects.put(id, object);
    return (T) this;
  }

  @Override
  public V get() {
    return (V) objects.get(id);
  }

  @Override
  protected void finalize() {
    if (cleanup) {
      objects.remove(id);
    }
  }
}
