package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import org.apache.flink.api.common.state.ReducingState;

public class FlinkReducingValueStorage<T> implements ValueStorage<T> {

  private final ReducingState<T> state;
  private final T defaultValue;

  public FlinkReducingValueStorage(ReducingState<T> state, T defaultValue) {
    this.state = state;
    this.defaultValue = defaultValue;
  }

  @Override
  public void set(T value) {
    try {
      state.clear();
      state.add(value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public T get() {
    try {
      T s = state.get();
      return (s == null) ? defaultValue : s;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void clear() {
    state.clear();
  }
}
