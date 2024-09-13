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

import com.google.common.base.Preconditions;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;

@Internal
@ThreadSafe
public class StreamingGlobalConfigHandleImpl implements StreamingGlobalConfigHandle {

  private final AtomicReference<StreamingGlobalConfig> streamingEngineConfig =
      new AtomicReference<>();

  private final CopyOnWriteArrayList<Consumer<StreamingGlobalConfig>> config_callbacks =
      new CopyOnWriteArrayList<>();

  @Override
  public StreamingGlobalConfig getConfig() {
    Preconditions.checkState(
        streamingEngineConfig.get() != null,
        "Global config should be set before any processing is done");
    return streamingEngineConfig.get();
  }

  @Override
  public void registerConfigObserver(@Nonnull Consumer<StreamingGlobalConfig> callback) {
    StreamingGlobalConfig config;
    synchronized (this) {
      config_callbacks.add(callback);
      config = streamingEngineConfig.get();
    }
    if (config != null) {
      // read config from streamingEngineConfig again
      // to prevent calling callback with stale config.
      // The cached `config` will be stale if setConfig
      // ran after the synchronized block.
      callback.accept(streamingEngineConfig.get());
    }
  }

  void setConfig(@Nonnull StreamingGlobalConfig config) {
    Iterator<Consumer<StreamingGlobalConfig>> iterator;
    synchronized (this) {
      if (config.equals(streamingEngineConfig.get())) {
        return;
      }
      streamingEngineConfig.set(config);
      // iterator of CopyOnWriteArrayList provides
      // snapshot semantics
      iterator = config_callbacks.iterator();
    }
    while (iterator.hasNext()) {
      iterator.next().accept(config);
    }
  }
}
