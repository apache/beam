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

import com.google.common.base.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A pool of resources associated with specific keys. Implementations enforce specific use patterns,
 * such as limiting the the number of outstanding elements available per key.
 */
interface KeyedResourcePool<K, V> {
  /**
   * Tries to acquire a value for the provided key, loading it via the provided loader if necessary.
   *
   * <p>If the returned {@link Optional} contains a value, the caller obtains ownership of that
   * value. The value should be released back to this {@link KeyedResourcePool} after the
   * caller no longer has use of it using {@link #release(Object, Object)}.
   *
   * <p>The provided {@link Callable} <b>must not</b> return null; it may either return a non-null
   * value or throw an exception.
   */
  Optional<V> tryAcquire(K key, Callable<V> loader) throws ExecutionException;

  /**
   * Release the provided value, relinquishing ownership of it. Future calls to
   * {@link #tryAcquire(Object, Callable)} may return the released value.
   */
  void release(K key, V value);
}
