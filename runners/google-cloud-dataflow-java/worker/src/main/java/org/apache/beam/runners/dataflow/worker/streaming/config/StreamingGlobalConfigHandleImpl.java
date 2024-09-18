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

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.common.base.Preconditions;

@Internal
@ThreadSafe
public class StreamingGlobalConfigHandleImpl implements StreamingGlobalConfigHandle {

  private final AtomicReference<StreamingGlobalConfig> streamingEngineConfig =
      new AtomicReference<>();

  private final CopyOnWriteArrayList<ConfigCallback> configCallbacks = new CopyOnWriteArrayList<>();

  @Override
  public StreamingGlobalConfig getConfig() {
    Preconditions.checkState(
        streamingEngineConfig.get() != null,
        "Global config should be set before any processing is done");
    return streamingEngineConfig.get();
  }

  @Override
  public void registerConfigObserver(@Nonnull Consumer<StreamingGlobalConfig> callback) {
    ConfigCallback configCallback = new ConfigCallback(callback);
    configCallbacks.add(configCallback);
    if (streamingEngineConfig.get() != null) {
      configCallback.run();
    }
  }

  void setConfig(@Nonnull StreamingGlobalConfig config) {
    if (config.equals(streamingEngineConfig.get())) {
      return;
    }
    streamingEngineConfig.set(config);
    for (ConfigCallback configCallback : configCallbacks) {
      configCallback.run();
    }
  }

  private class ConfigCallback {

    private final AtomicInteger queuedOrRunning = new AtomicInteger(0);
    private final Consumer<StreamingGlobalConfig> configConsumer;

    private ConfigCallback(Consumer<StreamingGlobalConfig> configConsumer) {
      this.configConsumer = configConsumer;
    }

    /**
     * Runs the passed in callback with the latest config. Overlapping `run()` calls will be
     * collapsed into one. If the callback is already running a new call will be scheduled to run
     * after the current execution completes, on the same thread which ran the previous run.
     */
    private void run() {
      // If the callback is already running,
      // Increment queued and return. The thread running
      // the callback will run it again with the latest config.
      if (queuedOrRunning.incrementAndGet() > 1) {
        return;
      }
      // Else run the callback
      while (true) {
        configConsumer.accept(StreamingGlobalConfigHandleImpl.this.streamingEngineConfig.get());
        if (queuedOrRunning.updateAndGet(
                queuedOrRunning -> {
                  if (queuedOrRunning == 1) {
                    // If there are no queued requests stop processing.
                    return 0;
                  }
                  // Else, clear queue, set 1 running and run the callback
                  return 1;
                })
            == 0) {
          break;
        }
      }
    }
  }
}
