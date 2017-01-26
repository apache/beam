/**
 * Copyright 2016 Seznam a.s.
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
import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.core.client.triggers.TriggerContext;
import cz.seznam.euphoria.flink.storage.Descriptors;
import cz.seznam.euphoria.flink.storage.FlinkListStorage;
import cz.seznam.euphoria.flink.storage.FlinkReducingValueStorage;
import cz.seznam.euphoria.flink.storage.FlinkValueStorage;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

/**
 * Adapts Flink {@link Trigger.TriggerContext} to be used as trigger context in
 * Euphoria API
 */
public class TriggerContextWrapper implements TriggerContext {

  private final Trigger.TriggerContext flinkContext;

  public TriggerContextWrapper(Trigger.TriggerContext flinkContext) {
    this.flinkContext = flinkContext;
  }

  Trigger.TriggerContext getFlinkContext() {
    return flinkContext;
  }

  @Override
  public boolean registerTimer(long stamp, Window window) {
    if (stamp <= getCurrentTimestamp()) {
      return false;
    }

    flinkContext.registerEventTimeTimer(stamp);
    return true;
  }

  @Override
  public void deleteTimer(long stamp, Window window) {
    flinkContext.deleteEventTimeTimer(stamp);
  }

  @Override
  public long getCurrentTimestamp() {
    return flinkContext.getCurrentWatermark();
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    if (descriptor instanceof ValueStorageDescriptor.MergingValueStorageDescriptor) {
      @SuppressWarnings("unchecked")
      ReducingStateDescriptor<T> from = Descriptors.from(
          (ValueStorageDescriptor.MergingValueStorageDescriptor<T>) descriptor);
      ReducingState<T> state = getFlinkContext().getPartitionedState(from);
      return new FlinkReducingValueStorage<>(state, descriptor.getDefaultValue());
    }
    return new FlinkValueStorage<>(
        flinkContext.getPartitionedState(Descriptors.from(descriptor)));
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    return new FlinkListStorage<>(
        flinkContext.getPartitionedState(Descriptors.from(descriptor)));
  }
}
