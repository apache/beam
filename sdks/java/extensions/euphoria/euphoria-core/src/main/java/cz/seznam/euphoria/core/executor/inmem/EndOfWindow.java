package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.WindowContext;

import java.util.Objects;

// Instances of this class are wandering around the dataflow in the inmem executor
// and signal the end of a fired window (pane.)
class EndOfWindow<W extends WindowContext<?, ?>> {
  private final W windowContext;

  EndOfWindow(W windowContext) {
    this.windowContext = Objects.requireNonNull(windowContext);
  }

  W getWindowContext() {
    return windowContext;
  }

  @Override
  public String toString() {
    return "EndOfWindow{" +
        "windowContext=" + windowContext +
        '}';
  }
}
