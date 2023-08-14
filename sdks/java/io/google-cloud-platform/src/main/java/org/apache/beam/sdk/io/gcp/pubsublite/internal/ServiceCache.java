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

import com.google.api.core.ApiService;
import com.google.api.core.ApiService.Listener;
import com.google.api.core.ApiService.State;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.pubsublite.internal.wire.ApiServiceUtils;
import com.google.cloud.pubsublite.internal.wire.SystemExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.function.Supplier;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A map of working ApiServices by identifying key. The key must be hashable. */
class ServiceCache<K, V extends ApiService> implements AutoCloseable {
  private final Logger logger = LoggerFactory.getLogger(ServiceCache.class);

  @GuardedBy("this")
  private final HashMap<K, V> liveMap = new HashMap<>();

  private synchronized void evict(K key, V service) {
    liveMap.remove(key, service);
  }

  synchronized V get(K key, Supplier<V> factory) throws ApiException {
    V service = liveMap.get(key);
    if (service != null) {
      return service;
    }
    V newService = factory.get();
    liveMap.put(key, newService);
    newService.addListener(
        new Listener() {
          @Override
          public void failed(State s, Throwable t) {
            logger.warn(newService.getClass().getSimpleName() + " failed.", t);
            evict(key, newService);
          }

          @Override
          public void terminated(State from) {
            evict(key, newService);
          }
        },
        SystemExecutors.getFuturesExecutor());
    newService.startAsync().awaitRunning();
    return newService;
  }

  @VisibleForTesting
  synchronized void set(K key, V service) {
    liveMap.put(key, service);
  }

  @Override
  public synchronized void close() {
    ApiServiceUtils.blockingShutdown(liveMap.values());
    liveMap.clear();
  }
}
