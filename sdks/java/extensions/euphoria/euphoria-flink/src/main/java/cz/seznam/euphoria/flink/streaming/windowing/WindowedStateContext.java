/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.io.SpillTools;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.util.Settings;
import cz.seznam.euphoria.flink.storage.FlinkSpillTools;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedStateBackend;

/**
 * A {@code StateContext} for windowed operations.
 */
public class WindowedStateContext<WID extends Window> implements StateContext {

  private final WindowedStorageProvider<WID> provider;

  private final SpillTools spillTools;

  public WindowedStateContext(
      ExecutionConfig conf,
      Settings settings,
      KeyedStateBackend keyedStateBackend,
      TypeSerializer<WID> windowSerializer,
      int descriptorsCacheMaxSize) {

    this.provider = new WindowedStorageProvider<>(
        keyedStateBackend, windowSerializer, descriptorsCacheMaxSize);
    this.spillTools = new FlinkSpillTools(conf, settings);
  }

  @Override
  public StorageProvider getStorageProvider() {
    return provider;
  }

  @Override
  public SpillTools getSpillTools() {
    return spillTools;
  }

  public void setWindow(WID window) {
    this.provider.setWindow(window);
  }

}
