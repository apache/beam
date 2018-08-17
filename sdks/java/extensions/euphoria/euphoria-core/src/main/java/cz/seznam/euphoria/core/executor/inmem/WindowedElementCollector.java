package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.WindowID;
import cz.seznam.euphoria.core.client.dataset.WindowedElement;
import cz.seznam.euphoria.core.client.io.Collector;

import java.util.Objects;

class WindowedElementCollector<T> implements Collector<T> {
  private final Collector<WindowedElement<?, ?, T>> wrap;

  protected WindowID<Object, Object> windowID;

  WindowedElementCollector(Collector<WindowedElement<?, ?, T>> wrap) {
    this.wrap = Objects.requireNonNull(wrap);
  }

  void assignWindowing(WindowID<Object, Object> windowID) {
    this.windowID = windowID;
  }

  @Override
  public void collect(T elem) {
    wrap.collect(new WindowedElement<>(windowID, elem));
  }
}
