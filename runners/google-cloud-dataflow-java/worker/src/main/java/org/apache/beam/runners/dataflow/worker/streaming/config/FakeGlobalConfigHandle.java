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

import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.beam.sdk.annotations.Internal;

@Internal
@ThreadSafe
/*
 *  Fake StreamingGlobalConfigHandle used for Tests. Allows setting fake configs.
 */
public class FakeGlobalConfigHandle implements StreamingGlobalConfigHandle {

  private final StreamingGlobalConfigHandleImpl globalConfigHandle;

  public FakeGlobalConfigHandle(StreamingGlobalConfig config) {
    this.globalConfigHandle = new StreamingGlobalConfigHandleImpl();
    this.globalConfigHandle.setConfig(config);
  }

  @Override
  public StreamingGlobalConfig getConfig() {
    return globalConfigHandle.getConfig();
  }

  public void setConfig(StreamingGlobalConfig config) {
    globalConfigHandle.setConfig(config);
  }

  @Override
  public void registerConfigObserver(@Nonnull Consumer<StreamingGlobalConfig> callback) {
    globalConfigHandle.registerConfigObserver(callback);
  }
}
