/*
 * Copyright 2016-2018 Seznam.cz, a.s.
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
package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.flink.storage.FlinkSpillTools;
import cz.seznam.euphoria.core.client.io.SpillTools;
import cz.seznam.euphoria.core.client.operator.state.StateContext;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.util.Settings;
import org.apache.flink.api.java.ExecutionEnvironment;

/**
 * A {@code StateContext} for batch environment.
 */
public class BatchStateContext implements StateContext {

  private final BatchStateStorageProvider provider;
  private final SpillTools spillTools;

  public BatchStateContext(
      Settings settings, ExecutionEnvironment env, int maxInMemElements) {
    this.spillTools = new FlinkSpillTools(env.getConfig(), settings);
    this.provider = new BatchStateStorageProvider(maxInMemElements, env);
  }

  @Override
  public StorageProvider getStorageProvider() {
    return provider;
  }

  @Override
  public SpillTools getSpillTools() {
    return spillTools;
  }

}
