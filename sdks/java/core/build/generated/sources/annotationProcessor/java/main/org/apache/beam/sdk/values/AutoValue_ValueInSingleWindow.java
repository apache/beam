package org.apache.beam.sdk.values;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_ValueInSingleWindow<T> extends ValueInSingleWindow<T> {

  private final T nullableValue;

  private final Instant timestamp;

  private final BoundedWindow window;

  private final PaneInfo pane;

  AutoValue_ValueInSingleWindow(
      T nullableValue,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo pane) {
    this.nullableValue = nullableValue;
    if (timestamp == null) {
      throw new NullPointerException("Null timestamp");
    }
    this.timestamp = timestamp;
    if (window == null) {
      throw new NullPointerException("Null window");
    }
    this.window = window;
    if (pane == null) {
      throw new NullPointerException("Null pane");
    }
    this.pane = pane;
  }

  @Override
  protected T getNullableValue() {
    return nullableValue;
  }

  @Override
  public Instant getTimestamp() {
    return timestamp;
  }

  @Override
  public BoundedWindow getWindow() {
    return window;
  }

  @Override
  public PaneInfo getPane() {
    return pane;
  }

  @Override
  public String toString() {
    return "ValueInSingleWindow{"
        + "nullableValue=" + nullableValue + ", "
        + "timestamp=" + timestamp + ", "
        + "window=" + window + ", "
        + "pane=" + pane
        + "}";
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof ValueInSingleWindow) {
      ValueInSingleWindow<?> that = (ValueInSingleWindow<?>) o;
      return (this.nullableValue == null ? that.getNullableValue() == null : this.nullableValue.equals(that.getNullableValue()))
          && this.timestamp.equals(that.getTimestamp())
          && this.window.equals(that.getWindow())
          && this.pane.equals(that.getPane());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= (nullableValue == null) ? 0 : nullableValue.hashCode();
    h$ *= 1000003;
    h$ ^= timestamp.hashCode();
    h$ *= 1000003;
    h$ ^= window.hashCode();
    h$ *= 1000003;
    h$ ^= pane.hashCode();
    return h$;
  }

}
