package cz.seznam.euphoria.flink.batch;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;

public class StampedWindowElement<W extends Window, T> extends WindowedElement<W, T> {
  final long timestamp;

  public StampedWindowElement(W window, T element, long timestamp) {
    super(window, element);
    this.timestamp = timestamp;
  }

  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return "StampedWindowElement{" +
        "window=" + getWindow() +
        ", element=" + get() +
        ", timestamp=" + timestamp +
        '}';
  }
}
