package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.windowing.TimedWindow;
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
    long endWindowStamp = (window instanceof TimedWindow)
            ? ((TimedWindow) window).maxTimestamp()
            : Long.MAX_VALUE;

    // ~ timestamp assigned to element can be either end of window
    // or current watermark supplied by triggering
    // ~ this is a workaround for NoopTriggerScheduler
    // used for batch processing that fires all windows
    // at the end of bounded input
    long stamp = Math.min(endWindowStamp, stampSupplier.get());

    wrap.collect(Datum.of(window, elem, stamp));
  }

  void setWindow(Window window) {
    this.window = window;
  }

  @Override
  public Object getWindow() {
    return window;
  }

}
