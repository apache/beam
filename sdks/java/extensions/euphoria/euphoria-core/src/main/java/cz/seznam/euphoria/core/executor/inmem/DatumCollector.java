package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.io.Collector;

import java.util.Objects;

class DatumCollector<T> implements Collector<T> {
  private final Collector wrap;

  private Object assignGroup;
  private Object assignLabel;

  DatumCollector(Collector wrap) {
    this.wrap = Objects.requireNonNull(wrap);
  }

  void assignWindowing(Object group, Object label) {
    this.assignGroup = group;
    this.assignLabel = label;
  }

  Object getAssignGroup() {
    return assignGroup;
  }

  Object getAssignLabel() {
    return assignLabel;
  }

  @Override
  public void collect(T elem) {
    wrap.collect(new Datum<>(assignGroup, assignLabel, elem));
  }
}
