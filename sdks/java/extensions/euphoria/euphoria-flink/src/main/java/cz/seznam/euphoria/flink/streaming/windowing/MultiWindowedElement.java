package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowID;
import cz.seznam.euphoria.flink.streaming.ElementProvider;

import java.util.Set;

public final class MultiWindowedElement<GROUP, LABEL, T> implements ElementProvider<T> {
  private final Set<WindowID<GROUP, LABEL>> windows;
  private final T element;

  public MultiWindowedElement(Set<WindowID<GROUP, LABEL>> windows, T element) {
    this.windows = windows;
    this.element = element;
  }

  @Override
  public T get() {
    return element;
  }

  public Set<WindowID<GROUP, LABEL>> windows() {
    return windows;
  }

  @Override
  public String toString() {
    return "MultiWindowedElement("
        + windows + ", " + element + ")";
  }


}
