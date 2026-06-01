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
package org.apache.beam.runners.kafka.streams.translation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test-only side-effect sink that survives Beam serialization without losing collected elements.
 *
 * <p>An ExecutableStage that contains a user {@link org.apache.beam.sdk.transforms.DoFn} runs the
 * DoFn in the SDK harness even when its output PCollection has no downstream consumer — the work is
 * still performed for its side effects. The natural unit test for that is to have the DoFn record
 * into a side-effect container and assert the container's contents from the test thread.
 *
 * <p>A plain static {@code AtomicReference} / {@code List} works only as long as the runner does
 * not serialize the {@code DoFn} (and therefore the container instance it holds). The EMBEDDED
 * environment may already, and could in the future, serialize the user code, in which case a cloned
 * container would silently drop its writes.
 *
 * <p>This class works around that by keying the actual storage on a {@link UUID} held by an
 * otherwise-empty instance. The instance itself is cheaply {@link Serializable}; clones still carry
 * the same {@code UUID} and therefore see the same backing list in the static {@link #REGISTRY}.
 *
 * @param <T> element type
 */
final class SharedTestCollector<T> implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Per-UUID storage, populated lazily on the first {@code record} for each instance. */
  private static final Map<UUID, List<Object>> REGISTRY = new ConcurrentHashMap<>();

  private final UUID id = UUID.randomUUID();

  /** Returns a fresh, empty collector instance with its own UUID. */
  static <T> SharedTestCollector<T> create() {
    return new SharedTestCollector<>();
  }

  /** Records a single element. Safe to call from any thread. */
  void record(T element) {
    REGISTRY.computeIfAbsent(id, k -> Collections.synchronizedList(new ArrayList<>())).add(element);
  }

  /** Returns an immutable snapshot of all recorded elements, in order. */
  @SuppressWarnings("unchecked")
  List<T> recorded() {
    List<Object> raw = REGISTRY.get(id);
    if (raw == null) {
      return Collections.emptyList();
    }
    synchronized (raw) {
      return Collections.unmodifiableList(new ArrayList<>((List<T>) (List<?>) raw));
    }
  }

  /** Clears the backing storage for this collector. Useful for {@code @Before} resets. */
  void reset() {
    REGISTRY.remove(id);
  }
}
