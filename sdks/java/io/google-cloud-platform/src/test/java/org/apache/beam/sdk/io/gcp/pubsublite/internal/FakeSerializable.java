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
package org.apache.beam.sdk.io.gcp.pubsublite.internal;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.beam.sdk.util.SerializableSupplier;

/**
 * A FakeSerializable hides a non-serializable object in a static map and returns a handle into the
 * static map. It is useful in the presence of in-process serialization, but not out of process
 * serialization.
 */
final class FakeSerializable {
  private static final AtomicInteger idCounter = new AtomicInteger(0);
  private static final ConcurrentHashMap<Integer, Object> map = new ConcurrentHashMap<>();

  private FakeSerializable() {}

  static class Handle<T> implements Serializable {
    private Handle(int id) {
      this.id = id;
    }

    private final int id;

    @SuppressWarnings("unchecked")
    T get() {
      return (T) map.get(id);
    }
  }

  static <T> Handle<T> put(T value) {
    int id = idCounter.incrementAndGet();
    map.put(id, value);
    return new Handle<T>(id);
  }

  static <T> SerializableSupplier<T> getSupplier(T value) {
    Handle<T> handle = put(value);
    return handle::get;
  }
}
