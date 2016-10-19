package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.client.operator.state.ListStorage;
import org.apache.flink.api.common.state.ListState;

/**
 * Implementation of {@link ListStorage} using Flink state API
 */
public class FlinkListStorage<T> implements ListStorage<T> {

  private final ListState<T> state;

  public FlinkListStorage(ListState<T> state) {
    this.state = state;
  }

  @Override
  public void add(T element) {
    try {
      state.add(element);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public Iterable<T> get() {
    try {
      return state.get();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void clear() {
    state.clear();
  }
}
