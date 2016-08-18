
package cz.seznam.euphoria.core.client.dataset;

import java.util.Objects;

/**
 * Identifier of a data window. A data window is a set of elements that
 * are assigned the same WindowID. WindowID consists of a group identifier and
 * a label. Group defines a 'space' of window labels - only labels with the same
 * group are compared.
 */
public class WindowID<GROUP, LABEL> {

  /** Create unaligned window ID with given group and label. */
  public static <GROUP, LABEL> WindowID<GROUP, LABEL> unaligned(
      GROUP group, LABEL label) {
    return new WindowID<>(group, label);
  }

  /** Create aligned window id. All windows are of the same group (type void). */
  public static <LABEL> WindowID<Void, LABEL> aligned(LABEL label) {
    return new WindowID<>(null, label);
  }


  final GROUP group;
  final LABEL label;

  private WindowID(GROUP group, LABEL label) {
    this.group = group;
    this.label = label;
  }

  public GROUP getGroup() {
    return group;
  }

  public LABEL getLabel() {
    return label;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof WindowID)) return false;
    WindowID<?, ?> other = (WindowID<?, ?>) o;
    return Objects.equals(other.group, group)
        && Objects.equals(other.label, label);
  }

  @Override
  public int hashCode() {
    return Objects.hash(group, label);
  }

  @Override
  public String toString() {
    return "WindowID(" + group + ", " + label + ")";
  }


}
