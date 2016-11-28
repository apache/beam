package cz.seznam.euphoria.flink.streaming.windowing;

import cz.seznam.euphoria.core.client.dataset.windowing.Window;
import cz.seznam.euphoria.flink.streaming.ElementProvider;

import java.util.Set;

public final class MultiWindowedElement<WID extends Window, T> implements ElementProvider<T> {
  private final Set<WID> windows;
  private final T element;

  public MultiWindowedElement(Set<WID> windows, T element) {
    this.windows = windows;
    this.element = element;
  }

  @Override
  public T getElement() {
    return element;
  }

  public Set<WID> windows() {
    return windows;
  }

  @Override
  public String toString() {
    return "MultiWindowedElement("
        + windows + ", " + element + ")";
  }


}
