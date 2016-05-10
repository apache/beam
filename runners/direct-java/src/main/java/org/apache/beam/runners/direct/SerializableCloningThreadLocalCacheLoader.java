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

import org.apache.beam.sdk.util.SerializableUtils;

import com.google.common.cache.CacheLoader;

import java.io.Serializable;

/**
 * A {@link CacheLoader} that loads {@link ThreadLocal ThreadLocals} with initial values equal to
 * the clone of the key.
 */
class SerializableCloningThreadLocalCacheLoader<T extends Serializable>
    extends CacheLoader<T, ThreadLocal<T>> {
  public static <T extends Serializable> CacheLoader<T, ThreadLocal<T>> create() {
    return new SerializableCloningThreadLocalCacheLoader<T>();
  }

  @Override
  public ThreadLocal<T> load(T key) throws Exception {
    return new CloningThreadLocal<>(key);
  }

  private static class CloningThreadLocal<T extends Serializable> extends ThreadLocal<T> {
    private final T original;

    public CloningThreadLocal(T value) {
      this.original = value;
    }

    @Override
    public T initialValue() {
      return SerializableUtils.clone(original);
    }
  }
}
