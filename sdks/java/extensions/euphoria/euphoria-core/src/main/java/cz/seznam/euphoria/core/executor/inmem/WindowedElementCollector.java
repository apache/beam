package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.io.Context;

import java.util.Objects;
import java.util.function.Supplier;

class WindowedElementCollector<T> implements Context<T> {
  private final Collector<Datum> wrap;
  private final Supplier<Long> stampSupplier;

  protected Window window;

  WindowedElementCollector(Collector<Datum> wrap, Supplier<Long> stampSupplier) {
    this.wrap = Objects.requireNonNull(wrap);
    this.stampSupplier = stampSupplier;
  }

  @Override
  public void collect(T elem) {
    wrap.collect(Datum.of(window, elem, stampSupplier.get()));
  }

  void setWindow(Window window) {
    this.window = window;
  }

  @Override
  public Object getWindow() {
    return window;
  }

}
