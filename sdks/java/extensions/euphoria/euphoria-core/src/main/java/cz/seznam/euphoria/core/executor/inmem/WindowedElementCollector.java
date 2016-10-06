package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.core.client.io.Context;

import java.util.Objects;
import java.util.function.Supplier;

class WindowedElementCollector<T> implements Context<T> {
  private final Context<Datum> wrap;
  private final Supplier<Long> stampSupplier;

  protected WindowID<Object> windowID;

  WindowedElementCollector(Context<Datum> wrap, Supplier<Long> stampSupplier) {
    this.wrap = Objects.requireNonNull(wrap);
    this.stampSupplier = stampSupplier;
  }

  @Override
  public void collect(T elem) {
    wrap.collect(Datum.of(windowID, elem, stampSupplier.get()));
  }

  void setWindow(WindowID<Object> windowID) {
    this.windowID = windowID;
  }

  @Override
  public Object getWindow() {
    return windowID.getLabel();
  }

}
