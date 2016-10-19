package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.client.operator.state.ValueStorage;
import org.apache.flink.api.common.state.ValueState;

/**
 * Implementation of {@link ValueStorage} using Flink state API
 */
public class FlinkValueStorage<T> implements ValueStorage<T> {

  private final ValueState<T> state;

  public FlinkValueStorage(ValueState<T> state) {
    this.state = state;
  }

  @Override
  public void set(T value) {
    try {
      state.update(value);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public T get() {
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
