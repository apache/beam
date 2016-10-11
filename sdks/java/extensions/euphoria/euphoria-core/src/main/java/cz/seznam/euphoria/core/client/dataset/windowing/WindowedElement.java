package cz.seznam.euphoria.core.client.dataset.windowing;

/**
 * A single data element flowing in dataset. Every such element
 * is associated with a windowing identifier, i.e. a tuple of window group and label.
 */
public class WindowedElement<W extends Window, T> {

  final W window;
  final T element;

  public WindowedElement(W window, T element) {
    this.window = window;
    this.element = element;
  }
  
  public W getWindow() {
    return window;
  }

  public T get() {
    return element;
  }

  @Override
  public String toString() {
    return "WindowedElement{" +
        "window=" + window +
        ", element=" + element +
        '}';
  }
}
