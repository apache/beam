package cz.seznam.euphoria.core.client.dataset.windowing;

/**
 * A single data element flowing in dataset. Every such element
 * is associated with a windowing identifier, i.e. a tuple of window group and label.
 */
public class WindowedElement<GROUP, LABEL, T> {

  final WindowID<GROUP, LABEL> windowID;
  final T element;

  public WindowedElement(WindowID<GROUP, LABEL> windowID, T element) {
    this.windowID = windowID;
    this.element = element;
  }
  
  public WindowID<GROUP, LABEL> getWindowID() {
    return windowID;
  }

  public T get() {
    return element;
  }

  @Override
  public String toString() {
    return "WindowedElement{" +
        "windowID=" + windowID +
        ", element=" + element +
        '}';
  }
}
