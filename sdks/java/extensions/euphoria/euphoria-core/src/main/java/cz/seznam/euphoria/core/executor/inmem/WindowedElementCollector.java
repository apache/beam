package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.io.Collector;

import java.util.Objects;

class WindowedElementCollector<T> implements Collector<T> {
  private final Collector<Datum> wrap;

  protected WindowID<Object, Object> windowID;

  WindowedElementCollector(Collector<Datum> wrap) {
    this.wrap = Objects.requireNonNull(wrap);
  }

  void assignWindowing(WindowID<Object, Object> windowID) {
    this.windowID = windowID;
  }

  @Override
  public void collect(T elem) {
    wrap.collect(Datum.of(windowID, elem));
  }
}
