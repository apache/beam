package cz.seznam.euphoria.spark;

final class TimestampedElement {

  private long timestamp;
  private final Object el;

  public TimestampedElement(long timestamp, Object el) {
    this.timestamp = timestamp;
    this.el = el;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public Object getElement() {
    return el;
  }
}
