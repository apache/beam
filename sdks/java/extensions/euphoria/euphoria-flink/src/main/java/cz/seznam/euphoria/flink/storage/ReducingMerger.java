package cz.seznam.euphoria.flink.storage;

import cz.seznam.euphoria.core.client.functional.BinaryFunction;
import org.apache.flink.api.common.functions.ReduceFunction;

import java.util.Objects;

public final class ReducingMerger<T> implements ReduceFunction<T> {
  private final BinaryFunction<T, T, T> merger;

  public ReducingMerger(BinaryFunction<T, T, T> merger) {
    this.merger = Objects.requireNonNull(merger);
  }

  @Override
  public T reduce(T value1, T value2) throws Exception {
    return merger.apply(value1, value2);
  }
}