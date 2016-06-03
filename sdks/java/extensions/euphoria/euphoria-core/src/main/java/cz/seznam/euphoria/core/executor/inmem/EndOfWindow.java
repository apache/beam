package cz.seznam.euphoria.core.executor.inmem;

import cz.seznam.euphoria.core.client.dataset.Window;

import java.util.Objects;

// Instances of this class are wandering around the dataflow in the inmem executor
// and signal the end of a fired window (pane.)
class EndOfWindow<W extends Window> {
  private final W window;

  EndOfWindow(W window) {
    this.window = Objects.requireNonNull(window);
  }

  W getWindow() {
    return window;
  }

  @Override
  public String toString() {
    return "EndOfWindow{" +
        "window=" + window +
        '}';
  }
}
