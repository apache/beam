package cz.seznam.euphoria.core.executor;

/**
 * A single data element flowing through the inmem executor. Every such element
 * is associated with a windowing identifier, i.e. a tuple of window group and label.
 */
class Datum<WGROUP, WLABEL, T> {
  final WGROUP group;
  final WLABEL label;
  final T element;

  Datum(WGROUP group, WLABEL label, T element) {
    this.group = group;
    this.label = label;
    this.element = element;
  }

  @Override
  public String toString() {
    return "Datum{" +
        "group=" + group +
        ", label=" + label +
        ", element=" + element +
        '}';
  }
}
