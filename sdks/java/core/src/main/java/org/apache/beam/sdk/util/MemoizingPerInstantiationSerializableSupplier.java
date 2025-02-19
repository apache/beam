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
package org.apache.beam.sdk.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * A supplier that memoizes within an instantiation across serialization/deserialization.
 *
 * <p>Specifically the wrapped supplier will be called once and the result memoized per group
 * consisting of an instance and all instances deserialized from its serialized state.
 *
 * <p>A particular use for this is within a DoFn class to maintain shared state across all instances
 * of the DoFn that correspond to same step in the graph but separate from other steps in the graph
 * using the same DoFn. This differs from a static variable which would be shared across all
 * instances of the DoFn and a non-static variable which is per instance.
 */
public class MemoizingPerInstantiationSerializableSupplier<T> implements SerializableSupplier<T> {
  private static final AtomicInteger idGenerator = new AtomicInteger();
  private final int id;

  private static final ConcurrentHashMap<Integer, Object> staticCache = new ConcurrentHashMap<>();
  private final SerializableSupplier<@NonNull T> supplier;
  private transient volatile @MonotonicNonNull T value;

  public MemoizingPerInstantiationSerializableSupplier(SerializableSupplier<@NonNull T> supplier) {
    id = idGenerator.incrementAndGet();
    this.supplier = supplier;
  }

  @Override
  @SuppressWarnings("unchecked")
  public T get() {
    @Nullable T result = value;
    if (result != null) {
      return result;
    }
    @Nullable T mapValue = (T) staticCache.computeIfAbsent(id, ignored -> supplier.get());
    return value = Preconditions.checkStateNotNull(mapValue);
  }
}
