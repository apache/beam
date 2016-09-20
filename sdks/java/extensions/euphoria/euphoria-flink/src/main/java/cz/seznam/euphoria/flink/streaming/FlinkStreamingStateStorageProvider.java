
package cz.seznam.euphoria.flink.streaming;

import cz.seznam.euphoria.core.client.operator.state.ListStateStorage;
import cz.seznam.euphoria.core.client.operator.state.StateStorageProvider;
import cz.seznam.euphoria.core.client.operator.state.ValueStateStorage;
import java.io.Serializable;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

/**
 * Storage provider using flink's state API.
 */
class FlinkStreamingStateStorageProvider implements StateStorageProvider, Serializable {

  @SuppressWarnings("unchecked")
  private static class ValueStorage implements ValueStateStorage<Object> {

    final ValueState state;

    ValueStorage(String name, Class clz, RuntimeContext context) {
      state = context.getState(new ValueStateDescriptor(
          "euphoria-state::" + name,
          clz,
          null));
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
  private static class ListStorage implements ListStateStorage<Object> {

    final ListState state;

    ListStorage(String name, Class clz, RuntimeContext context) {
      state = context.getListState(new ListStateDescriptor(
          "euphoria-state::" + name,
          clz));
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
  public <T> ValueStateStorage<T> getValueStorage(String name, Class<T> what) {

    return (ValueStateStorage) new ValueStorage(name, what, context);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> ListStateStorage<T> getListStorage(String name, Class<T> what) {
    
    return (ListStateStorage) new ListStorage(name, what, context);
  }


}
