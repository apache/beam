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

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.OldDoFn;
import org.apache.beam.sdk.util.SerializableUtils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Manages {@link DoFn} setup, teardown, and serialization.
 *
 * <p>{@link DoFnLifecycleManager} is similar to a {@link ThreadLocal} storing a {@link DoFn}, but
 * calls the {@link DoFn} {@link Setup} the first time the {@link DoFn} is obtained and {@link
 * Teardown} whenever the {@link DoFn} is removed, and provides a method for clearing all cached
 * {@link DoFn DoFns}.
 */
class DoFnLifecycleManager {
  public static DoFnLifecycleManager of(OldDoFn<?, ?> original) {
    return new DoFnLifecycleManager(original);
  }

  private final LoadingCache<Thread, OldDoFn<?, ?>> outstanding;

  private DoFnLifecycleManager(OldDoFn<?, ?> original) {
    this.outstanding = CacheBuilder.newBuilder().build(new DeserializingCacheLoader(original));
  }

  public OldDoFn<?, ?> get() throws Exception {
    Thread currentThread = Thread.currentThread();
    return outstanding.get(currentThread);
  }

  public void remove() throws Exception {
    Thread currentThread = Thread.currentThread();
    outstanding.invalidate(currentThread);
  }

  /**
   * Remove all {@link DoFn DoFns} from this {@link DoFnLifecycleManager}.
   */
  public void removeAll() throws Exception {
    outstanding.invalidateAll();
  }

  private class DeserializingCacheLoader extends CacheLoader<Thread, OldDoFn<?, ?>> {
    private final byte[] original;

    public DeserializingCacheLoader(OldDoFn<?, ?> original) {
      this.original = SerializableUtils.serializeToByteArray(original);
    }

    @Override
    public OldDoFn<?, ?> load(Thread key) throws Exception {
      return (OldDoFn<?, ?>) SerializableUtils.deserializeFromByteArray(original,
          "DoFn Copy in thread " + key.getName());
    }
  }
}
