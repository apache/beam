package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.io.Context;

import java.util.Objects;

class WindowedElementCollector<T> implements Context<T> {
  private final Context<Datum> wrap;

  protected WindowID<Object> windowID;

  WindowedElementCollector(Context<Datum> wrap) {
    this.wrap = Objects.requireNonNull(wrap);
  }

  void assignWindowing(WindowID<Object> windowID) {
    this.windowID = windowID;
  }

  @Override
  public void collect(T elem) {
    wrap.collect(Datum.of(windowID, elem));
  }
}
