
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
    if (this == o) return true;
    if (!(o instanceof WindowID)) return false;
    WindowID<?> other = (WindowID<?>) o;
    return Objects.equals(other.label, label);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label);
  }

  @Override
  public String toString() {
    return "WindowID(" + label + ")";
  }


}
