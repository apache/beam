
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import cz.seznam.euphoria.core.client.operator.state.ListStorageDescriptor;
import cz.seznam.euphoria.core.client.operator.state.StorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import cz.seznam.euphoria.core.client.operator.state.ValueStorageDescriptor;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * Storage provider using flink's state API.
 */
class FlinkStreamingStateStorageProvider implements StorageProvider, Serializable {

  @SuppressWarnings("unchecked")
  private static class FlinkValueStorage implements ValueStorage<Object> {

    final ValueState state;

    FlinkValueStorage(ValueStorageDescriptor descriptor, RuntimeContext context) {
      state = context.getState(new ValueStateDescriptor<>(
          descriptor.getName(),
          descriptor.getValueClass(),
          descriptor.getDefaultValue()));
    }

    @Override
    public void set(Object value) {
      try {
        state.update(value);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public Object get() {
      try {
        return state.value();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void clear() {
      state.clear();
    }

  }

  @SuppressWarnings("unchecked")
  private static class FlinkListStorage implements ListStorage<Object> {

    final ListState state;

    FlinkListStorage(ListStorageDescriptor descriptor, RuntimeContext context) {
      state = context.getListState(new ListStateDescriptor(
          descriptor.getName(),
          descriptor.getElementClass()));
    }

    @Override
    public void add(Object element) {
      try {
        state.add(element);
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public Iterable<Object> get() {
      try {
        return (Iterable<Object>) state.get();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void clear() {
      state.clear();
    }
  }


  RuntimeContext context;

  void initialize(RuntimeContext context) {
    this.context = context;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ValueStorage<T> getValueStorage(ValueStorageDescriptor<T> descriptor) {
    return (ValueStorage) new FlinkValueStorage(descriptor, context);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListStorage<T> getListStorage(ListStorageDescriptor<T> descriptor) {    
    return (ListStorage) new FlinkListStorage(descriptor, context);
  }


}
