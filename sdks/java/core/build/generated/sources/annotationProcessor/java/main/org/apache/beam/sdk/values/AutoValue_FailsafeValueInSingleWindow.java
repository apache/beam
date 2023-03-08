package org.apache.beam.sdk.values;

import javax.annotation.Generated;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.joda.time.Instant;

@Generated("com.google.auto.value.processor.AutoValueProcessor")
final class AutoValue_FailsafeValueInSingleWindow<T, ErrorT> extends FailsafeValueInSingleWindow<T, ErrorT> {

  private final T value;

  private final Instant timestamp;

  private final BoundedWindow window;

  private final PaneInfo pane;

  private final ErrorT failsafeValue;

  AutoValue_FailsafeValueInSingleWindow(
      T value,
      Instant timestamp,
      BoundedWindow window,
      PaneInfo pane,
      ErrorT failsafeValue) {
    if (value == null) {
      throw new NullPointerException("Null value");
    }
    this.value = value;
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
    if (failsafeValue == null) {
      throw new NullPointerException("Null failsafeValue");
    }
    this.failsafeValue = failsafeValue;
  }

  @Override
  public T getValue() {
    return value;
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
  public ErrorT getFailsafeValue() {
    return failsafeValue;
  }

  @Override
  public String toString() {
    return "FailsafeValueInSingleWindow{"
        + "value=" + value + ", "
        + "timestamp=" + timestamp + ", "
        + "window=" + window + ", "
        + "pane=" + pane + ", "
        + "failsafeValue=" + failsafeValue
        + "}";
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (o instanceof FailsafeValueInSingleWindow) {
      FailsafeValueInSingleWindow<?, ?> that = (FailsafeValueInSingleWindow<?, ?>) o;
      return this.value.equals(that.getValue())
          && this.timestamp.equals(that.getTimestamp())
          && this.window.equals(that.getWindow())
          && this.pane.equals(that.getPane())
          && this.failsafeValue.equals(that.getFailsafeValue());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h$ = 1;
    h$ *= 1000003;
    h$ ^= value.hashCode();
    h$ *= 1000003;
    h$ ^= timestamp.hashCode();
    h$ *= 1000003;
    h$ ^= window.hashCode();
    h$ *= 1000003;
    h$ ^= pane.hashCode();
    h$ *= 1000003;
    h$ ^= failsafeValue.hashCode();
    return h$;
  }

}
