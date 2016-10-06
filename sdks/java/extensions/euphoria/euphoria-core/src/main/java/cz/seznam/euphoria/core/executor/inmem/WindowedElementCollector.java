package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.io.Context;

import java.util.Objects;

class WindowedElementCollector<T> implements Context<T> {
  private final Collector<Datum> wrap;

  protected WindowID<Object> windowID;

  WindowedElementCollector(Collector<Datum> wrap) {
    this.wrap = Objects.requireNonNull(wrap);
  }

  @Override
  public void collect(T elem) {
    wrap.collect(Datum.of(windowID, elem));
  }

  void setWindow(WindowID<Object> windowID) {
    this.windowID = windowID;
  }

  @Override
  public Object getWindow() {
    return windowID.getLabel();
  }
}
