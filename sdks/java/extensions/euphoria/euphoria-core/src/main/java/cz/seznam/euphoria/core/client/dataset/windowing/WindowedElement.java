package cz.seznam.euphoria.core.client.dataset.windowing;

/**
 * A single data element flowing in dataset. Every such element
 * is associated with a windowing identifier, i.e. a tuple of window group and label.
 */
public class WindowedElement<W extends Window, T> {

  final T element;
  W window;
  long timestamp;

  public WindowedElement(W window, long timestamp, T element) {
    this.window = window;
    this.timestamp = timestamp;
    this.element = element;
  }

  public W getWindow() {
    return window;
  }

  public void setWindow(W window) {
    this.window = window;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public T get() {
    return element;
  }

  @Override
  public String toString() {
    return "WindowedElement{" +
        "window=" + window +
        ", timestamp=" + timestamp +
        ", element=" + element +
        '}';
  }
}
