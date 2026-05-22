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
package org.apache.beam.sdk.io.solace.read;

import java.lang.ref.WeakReference;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A global registry to keep track of active {@link UnboundedSolaceReader} instances on the worker
 * JVM using weak references.
 *
 * <p>This allows serialized {@link SolaceCheckpointMark} instances to resolve their originating
 * reader and perform sequential acknowledgments.
 */
class ActiveReadersRegistry {
  private static final ConcurrentHashMap<UUID, WeakReference<UnboundedSolaceReader<?>>> registry =
      new ConcurrentHashMap<>();

  public static void register(UUID uuid, UnboundedSolaceReader<?> reader) {
    registry.put(uuid, new WeakReference<>(reader));
  }

  public static void unregister(UUID uuid) {
    registry.remove(uuid);
  }

  public static @Nullable UnboundedSolaceReader<?> get(UUID uuid) {
    WeakReference<UnboundedSolaceReader<?>> ref = registry.get(uuid);
    return ref != null ? ref.get() : null;
  }
}
