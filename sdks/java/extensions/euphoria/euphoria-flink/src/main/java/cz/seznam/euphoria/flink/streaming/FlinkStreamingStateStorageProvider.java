
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;

import java.io.Serializable;

import cz.seznam.euphoria.flink.storage.FlinkListStorage;
import cz.seznam.euphoria.flink.storage.FlinkValueStorage;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * Storage provider using flink's state API.
 */
class FlinkStreamingStateStorageProvider implements StorageProvider, Serializable {

  private RuntimeContext context;

  void initialize(RuntimeContext context) {
    this.context = context;
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return new FlinkValueStorage<>(context.getState(
            new ValueStateDescriptor<>(
                    descriptor.getName(),
                    descriptor.getValueClass(),
                    descriptor.getDefaultValue())));
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    return new FlinkListStorage<>(context.getListState(
            new ListStateDescriptor<>(
                    descriptor.getName(),
                    descriptor.getElementClass())));
  }
}
