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

package org.apache.beam.runners.direct;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

/**
 * A {@link KeyedResourcePool} which is limited to at most one outstanding instance at a time for
 * each key.
 */
class LockedKeyedResourcePool<K, V> implements KeyedResourcePool<K, V> {
  /**
   * A map from each key to an {@link Optional} of the associated value. At most one value is stored
   * per key, and it is obtained by at most one thread at a time.
   *
   * <p>For each key in this map:
   *
   * <ul>
   * <li>If there is no associated value, then no value has been stored yet.
   * <li>If the value is {@code Optional.absent()} then the value is currently in use.
   * <li>If the value is {@code Optional.present()} then the contained value is available for use.
   * </ul>
   */
  public static <K, V> LockedKeyedResourcePool<K, V> create() {
    return new LockedKeyedResourcePool<>();
  }

  private final ConcurrentMap<K, Optional<V>> cache;

  private LockedKeyedResourcePool() {
    cache = new ConcurrentHashMap<>();
  }

  @Override
  public Optional<V> tryAcquire(K key, Callable<V> loader) throws ExecutionException {
    Optional<V> value = cache.replace(key, Optional.<V>absent());
    if (value == null) {
      // No value already existed, so populate the cache with the value returned by the loader
      cache.putIfAbsent(key, Optional.of(load(loader)));
      // Some other thread may obtain the result after the putIfAbsent, so retry acquisition
      value = cache.replace(key, Optional.<V>absent());
    }
    return value;
  }

  private V load(Callable<V> loader) throws ExecutionException {
    try {
      return loader.call();
    } catch (Error t) {
      throw new ExecutionError(t);
    } catch (RuntimeException e) {
      throw new UncheckedExecutionException(e);
    } catch (Exception e) {
      throw new ExecutionException(e);
    }
  }

  @Override
  public void release(K key, V value) {
    Optional<V> replaced = cache.replace(key, Optional.of(value));
    checkNotNull(replaced, "Tried to release before a value was acquired");
    checkState(
        !replaced.isPresent(),
        "Released a value to a %s where there is already a value present for key %s (%s). "
            + "At most one value may be present at a time.",
        LockedKeyedResourcePool.class.getSimpleName(),
        key,
        replaced);
  }
}
