package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import cz.seznam.euphoria.flink.storage.Descriptors;
import cz.seznam.euphoria.flink.storage.FlinkListStorage;
import cz.seznam.euphoria.flink.storage.FlinkValueStorage;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;

/**
 * Storage provider using flink's state API.
 */
class FlinkStreamingStateStorageProvider implements StorageProvider, Serializable {

  private transient RuntimeContext context;

  void initialize(RuntimeContext context) {
    this.context = context;
  }

  @Override
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return new FlinkValueStorage<>(context.getState(Descriptors.from(descriptor)));
  }

  @Override
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {
    return new FlinkListStorage<>(context.getListState(Descriptors.from(descriptor)));
  }
}
