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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.Setup;
import org.apache.beam.sdk.transforms.DoFn.Teardown;
import org.apache.beam.sdk.transforms.reflect.DoFnInvokers;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheBuilder;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.CacheLoader;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LoadingCache;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalListener;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.RemovalNotification;

/**
 * Manages {@link DoFn} setup, teardown, and serialization.
 *
 * <p>{@link DoFnLifecycleManager} is similar to a {@link ThreadLocal} storing a {@link DoFn}, but
 * calls the {@link DoFn} {@link Setup @Setup} method the first time the {@link DoFn} is obtained
 * and {@link Teardown @Teardown} whenever the {@link DoFn} is removed, and provides a method for
 * clearing all cached {@link DoFn DoFns}.
 */
class DoFnLifecycleManager {
  public static DoFnLifecycleManager of(DoFn<?, ?> original, PipelineOptions options) {
    return new DoFnLifecycleManager(original, options);
  }

  private final LoadingCache<Thread, DoFn<?, ?>> outstanding;
  private final ConcurrentMap<Thread, Exception> thrownOnTeardown;

  private DoFnLifecycleManager(DoFn<?, ?> original, PipelineOptions options) {
    this.outstanding =
        CacheBuilder.newBuilder()
            .removalListener(new TeardownRemovedFnListener())
            .build(new DeserializingCacheLoader(original, options));
    thrownOnTeardown = new ConcurrentHashMap<>();
  }

  public <InputT, OutputT> DoFn<InputT, OutputT> get() throws Exception {
    Thread currentThread = Thread.currentThread();
    return (DoFn<InputT, OutputT>) outstanding.get(currentThread);
  }

  public void remove() throws Exception {
    Thread currentThread = Thread.currentThread();
    outstanding.invalidate(currentThread);
    // Block until the invalidate is fully completed
    outstanding.cleanUp();
    // Remove to try too avoid reporting the same teardown exception twice. May still double-report,
    // but the second will be suppressed.
    Exception thrown = thrownOnTeardown.remove(currentThread);
    if (thrown != null) {
      throw thrown;
    }
  }

  /**
   * Remove all {@link DoFn DoFns} from this {@link DoFnLifecycleManager}. Returns all exceptions
   * that were thrown while calling the remove methods.
   *
   * <p>If the returned Collection is nonempty, an exception was thrown from at least one {@link
   * DoFn.Teardown @Teardown} method, and the {@link PipelineRunner} should throw an exception.
   */
  public Collection<Exception> removeAll() throws Exception {
    outstanding.invalidateAll();
    // Make sure all of the teardowns are run
    outstanding.cleanUp();
    return thrownOnTeardown.values();
  }

  private static class DeserializingCacheLoader extends CacheLoader<Thread, DoFn<?, ?>> {
    private final byte[] original;
    private final PipelineOptions options;

    public DeserializingCacheLoader(DoFn<?, ?> original, PipelineOptions options) {
      this.original = SerializableUtils.serializeToByteArray(original);
      this.options = options;
    }

    @Override
    public DoFn<?, ?> load(Thread key) throws Exception {
      DoFn<?, ?> fn =
          (DoFn<?, ?>)
              SerializableUtils.deserializeFromByteArray(
                  original, "DoFn Copy in thread " + key.getName());
      DoFnInvokers.tryInvokeSetupFor(fn, options);
      return fn;
    }
  }

  private class TeardownRemovedFnListener implements RemovalListener<Thread, DoFn<?, ?>> {
    @Override
    public void onRemoval(RemovalNotification<Thread, DoFn<?, ?>> notification) {
      try {
        DoFnInvokers.invokerFor(checkNotNull(notification.getValue())).invokeTeardown();
      } catch (Exception e) {
        thrownOnTeardown.put(checkNotNull(notification.getKey()), e);
      }
    }
  }
}
