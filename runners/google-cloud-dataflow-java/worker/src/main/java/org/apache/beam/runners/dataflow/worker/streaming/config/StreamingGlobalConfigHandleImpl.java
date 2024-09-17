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
package org.apache.beam.runners.dataflow.worker.streaming.config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Internal
@ThreadSafe
public class StreamingGlobalConfigHandleImpl implements StreamingGlobalConfigHandle {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingGlobalConfigHandleImpl.class);

  private final AtomicReference<StreamingGlobalConfig> streamingEngineConfig =
      new AtomicReference<>();

  @GuardedBy("this")
  private final List<Consumer<StreamingGlobalConfig>> config_callbacks = new ArrayList<>();

  // Using a single threaded executor to call callbacks in the scheduled order.
  private final ExecutorService singleThreadExecutor =
      Executors.newSingleThreadExecutor(
          r -> new Thread(r, "StreamingGlobalConfigHandleImpl Executor"));

  @Override
  public StreamingGlobalConfig getConfig() {
    Preconditions.checkState(
        streamingEngineConfig.get() != null,
        "Global config should be set before any processing is done");
    return streamingEngineConfig.get();
  }

  @Override
  public void registerConfigObserver(@Nonnull Consumer<StreamingGlobalConfig> callback) {
    synchronized (this) {
      config_callbacks.add(callback);
      // If the config is already set, schedule a callback
      if (streamingEngineConfig.get() != null) {
        scheduleConfigCallback(callback);
      }
    }
  }

  void setConfig(@Nonnull StreamingGlobalConfig config) {
    Iterator<Consumer<StreamingGlobalConfig>> iterator;
    synchronized (this) {
      if (config.equals(streamingEngineConfig.get())) {
        return;
      }
      streamingEngineConfig.set(config);
      for (Consumer<StreamingGlobalConfig> callback : config_callbacks) {
        scheduleConfigCallback(callback);
      }
    }
  }

  private void scheduleConfigCallback(Consumer<StreamingGlobalConfig> callback) {
    Future<?> unusedFuture =
        singleThreadExecutor.submit(
            () -> {
              try {
                callback.accept(streamingEngineConfig.get());
              } catch (Exception e) {
                LOG.error("Exception from StreamingGlobalConfig callback", e);
              }
            });
  }
}
