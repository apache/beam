
package cz.seznam.euphoria.core.client.dataset.windowing;

import java.util.Objects;

/**
 * Identifier of a data window. A data window is a set of elements that
 * are assigned the same WindowID. WindowID consists of a window label.
 */
public final class WindowID<LABEL> {

  final LABEL label;

  public WindowID(LABEL label) {
    this.label = label;
  }

  public LABEL getLabel() {
    return label;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof WindowID) {
      WindowID<?> that = (WindowID<?>) o;
      return Objects.equals(this.label, that.label);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return label == null ? 0 : label.hashCode();
  }

  @Override
  public String toString() {
    return "WindowID(" + label + ")";
  }


}
